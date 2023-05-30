package input

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/filter"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/output"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/position"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/rule"
	"github.com/siddontang/go-log/log"
	"strings"
	"time"
)

type Mysql struct {
	*config.Mysql
}

type MyEventHandler struct {
	canal.DummyEventHandler
	syncCh        chan interface{}
	syncChGTIDSet mysql.GTIDSet // sync chan中last gtid
	ackGTIDSet    mysql.GTIDSet // sync data ack的 gtid
	starrocks     *output.Starrocks
	rulesMap      map[string]*rule.MysqlToSrRule
	ctx           context.Context
	cancel        context.CancelFunc
	position      *position.Position
	c             *canal.Canal
	matcher       filter.BinlogFilterMatcher
	syncParam     *config.SyncParam
	StartPosition string
}

func (m *Mysql) initCanalCfg() *canal.Config {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", m.Host, m.Port)
	cfg.User = m.UserName
	cfg.Password = m.Password
	cfg.Dump.ExecutionPath = "" // ignore mysqldump, use binlog only
	return cfg
}

func (h *MyEventHandler) String() string {
	return "MyEventHandler"
}

func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {
	msgs := h.eventPreProcessing(e)
	for _, m := range msgs {
		if !h.matcher.IterateFilter(m) {
			h.syncCh <- m
		}
	}
	return nil
}

func (h *MyEventHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	// log.Infof(pos.String(), set.String(), force)
	h.syncCh <- set
	return nil
}

func (h *MyEventHandler) chanOutPut() {
	for {
		select {
		case v := <-h.syncCh:
			switch data := v.(type) {
			case *canal.RowsEvent:
				log.Infof(data.String())
			case mysql.GTIDSet:
				h.syncChGTIDSet = data
			}
		case <-h.ctx.Done():
			// 被取消或者超时就结束协程
			log.Infof("chan output goroutine finished")
			return
		}
	}
}

func (h *MyEventHandler) chanLoop() {
	ticker := time.NewTicker(time.Second * time.Duration(h.syncParam.FlushDelaySecond))
	//defer func() {
	//	if e := recover(); e != nil {
	//		debug.PrintStack()
	//		log.Errorf("recover: ", e)
	//		h.cancel()
	//	}
	//}()
	defer ticker.Stop()

	eventsLen := 0
	schemaTableEvents := make(map[string][]*msg.Msg)
	for {
		needFlush := false
		select {
		case v := <-h.syncCh:
			switch data := v.(type) {
			case *msg.Msg:
				schemaTable := data.Table.Schema + ":" + data.Table.Name
				rowsData, ok := schemaTableEvents[schemaTable]
				if !ok {
					schemaTableEvents[schemaTable] = make([]*msg.Msg, 0, h.syncParam.ChannelSize)
				}
				schemaTableEvents[schemaTable] = append(rowsData, data)
				eventsLen += 1

				if eventsLen >= h.syncParam.ChannelSize {
					needFlush = true
				}
			case mysql.GTIDSet:
				h.syncChGTIDSet = data
			}
		case <-ticker.C:
			needFlush = true
		case <-h.ctx.Done():
			// 被取消或者超时就结束协程
			log.Infof("chanLoop output goroutine finished")
			return
		}

		if needFlush {
			for schemaTable := range schemaTableEvents {
				schema := strings.Split(schemaTable, ":")[0]
				table := strings.Split(schemaTable, ":")[1]
				tableObj, err := h.c.GetTable(schema, table)
				if err != nil {
					log.Errorf("", err)
					h.cancel()
					return
				}
				err = h.starrocks.Execute(schemaTableEvents[schemaTable], h.rulesMap[schemaTable], tableObj)
				if err != nil {
					log.Errorf("do starrocks bulk err %v, close sync", err)
					h.cancel()
					return
				}
				// log.Debugf("bulk消费batch数据：", schemaTable, len(schemaTableEvents[schemaTable]))

				delete(schemaTableEvents, schemaTable)
			}
			if err := h.position.Save(h.syncChGTIDSet); err != nil {
				h.cancel()
				return
			}
			h.ackGTIDSet = h.syncChGTIDSet
			eventsLen = 0
			ticker.Reset(time.Second * time.Duration(h.syncParam.FlushDelaySecond))
		}

	}
}

func (h *MyEventHandler) getMysqlGtidSet() mysql.GTIDSet {
	var gs mysql.GTIDSet
	var err error
	if h.position.BinlogGTID != "" {
		gs, err = mysql.ParseGTIDSet("mysql", h.position.BinlogGTID)
		if err != nil {
			log.Fatal(err)
		}
	} else if h.StartPosition != "" {
		gs, err = mysql.ParseGTIDSet("mysql", h.StartPosition)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		log.Infof("%s param 'binlog-gtid' not exist", h.position.GetFilePath())
		log.Infof("The configuration file [input] param 'start-gtid' not exist")
		log.Infof("get the current gtid set value")
		gs, err = h.c.GetMasterGTIDSet()
		if err != nil {
			log.Fatal(err)
		}
		if gs.String() == "" {
			log.Fatal("the gtid value is empty, please confirm whether to enable gtid!")
		}
		if err := h.position.Save(gs); err != nil {
			log.Fatal(err)
		}
	}
	return gs
}

func (h *MyEventHandler) Ctx() context.Context {
	return h.ctx
}

func (h *MyEventHandler) AckGTIDSet() mysql.GTIDSet {
	return h.ackGTIDSet
}

func (h *MyEventHandler) C() *canal.Canal {
	return h.c
}

func (h *MyEventHandler) Cancel() context.CancelFunc {
	return h.cancel
}

func (h *MyEventHandler) eventPreProcessing(e *canal.RowsEvent) []*msg.Msg {
	var msgs []*msg.Msg
	if e.Action == canal.InsertAction {
		for _, row := range e.Rows {
			data := make(map[string]interface{})
			for j := 0; j < len(e.Table.Columns); j++ {
				data[e.Table.Columns[j].Name] = row[j]
			}
			log.Debugf("canal event: %s %s.%s %v\n", e.Action, e.Table.Schema, e.Table.Name, row)
			msgs = append(msgs, &msg.Msg{
				Table:  e.Table,
				Action: e.Action,
				Data:   data,
			})
			// log.Debugf("msg data:%v", data)

		}
		return msgs
	}
	if e.Action == canal.UpdateAction {
		for i, row := range e.Rows {
			if i%2 == 0 {
				continue
			}
			data := make(map[string]interface{})
			old := make(map[string]interface{})
			for j := 0; j < len(e.Table.Columns); j++ {
				data[e.Table.Columns[j].Name] = row[j]
				old[e.Table.Columns[j].Name] = e.Rows[i-1][j]
			}
			log.Debugf("canal event: %s %s.%s %v\n", e.Action, e.Table.Schema, e.Table.Name, row)
			msgs = append(msgs, &msg.Msg{
				Table:  e.Table,
				Action: e.Action,
				Data:   data,
				Old:    old,
			})
			// log.Debugf("msg data:%v", data)
		}
		return msgs
	}
	if e.Action == canal.DeleteAction {
		for _, row := range e.Rows {
			data := make(map[string]interface{})
			for j := 0; j < len(e.Table.Columns); j++ {
				data[e.Table.Columns[j].Name] = row[j]
			}
			log.Debugf("canal event: %s %s.%s %v\n", e.Action, e.Table.Schema, e.Table.Name, row)
			msgs = append(msgs, &msg.Msg{
				Table:  e.Table,
				Action: e.Action,
				Data:   data,
			})
			// log.Debugf("msg data:%v", data)

		}
		return msgs
	}
	return nil
}

func NewMysql(conf *config.MysqlSrConfig) *MyEventHandler {
	m := &Mysql{conf.Mysql}
	cfg := m.initCanalCfg()
	cfg.IncludeTableRegex = rule.NewMysqlToSrRule(conf.Rules)
	cfg.Logger = conf.Logger

	// 初始化canal
	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Register a handler to handle RowsEvent
	h := &MyEventHandler{}
	h.syncParam = conf.SyncParam
	h.starrocks = &output.Starrocks{Starrocks: conf.Starrocks}
	h.rulesMap = map[string]*rule.MysqlToSrRule{}
	for _, r := range conf.Rules {
		h.rulesMap[r.SourceSchema+":"+r.SourceTable] = r
	}
	h.c = c
	h.syncCh = make(chan interface{}, h.syncParam.ChannelSize)
	h.ctx, h.cancel = context.WithCancel(context.Background())
	c.SetEventHandler(h)

	if conf.Input != nil {
		h.StartPosition = conf.Input.StartPosition
	}

	// 获取gtidSet
	// 加载mysql position
	pos, err := position.LoadPosition(conf)
	if err != nil {
		log.Fatal(err)
	}
	h.position = pos
	gs := h.getMysqlGtidSet()
	h.syncChGTIDSet, h.ackGTIDSet = gs, gs

	for _, f := range conf.Filter {
		if f.Type == "delete-dml-column" {
			deleteDmlColumnFilter, err := filter.NewDeleteDmlColumnFilter(f.Config)
			if err != nil {
				log.Fatal(err)
			}
			h.matcher = append(h.matcher, deleteDmlColumnFilter)
		}
		if f.Type == "convert-dml-column" {
			convertDmlColumnFilter, err := filter.NewConvertDmlColumnFilter(f.Config)
			if err != nil {
				log.Fatal(err)
			}
			h.matcher = append(h.matcher, convertDmlColumnFilter)
		}
	}

	// 启动chanLoop
	go h.chanLoop()

	return h

}
