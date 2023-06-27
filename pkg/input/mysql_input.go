package input

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/juju/errors"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/channel"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/metrics"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/position"
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
	"sync"
	"time"
)

type MysqlInputPlugin struct {
	canal.DummyEventHandler
	*config.MysqlConfig
	canalConfig *canal.Config
	syncChan    *channel.SyncChannel
	canal       *canal.Canal
	position    position.Position
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

type inputContext struct {
	BinlogName string `toml:"binlog-name"`
	BinlogPos  uint32 `toml:"binlog-pos"`
	BinlogGTID string `toml:"binlog-gtid"`
}

func (mi *MysqlInputPlugin) NewInput(inputConfig interface{}, ruleRegex []string) {
	mi.MysqlConfig = &config.MysqlConfig{}
	err := mapstructure.Decode(inputConfig, mi.MysqlConfig)
	if err != nil {
		log.Fatal("input config parsing failed. err: ", err.Error())
	}
	mi.ctx, mi.cancel = context.WithCancel(context.Background())
	// 初始化canal配置
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", mi.MysqlConfig.Host, mi.MysqlConfig.Port)
	cfg.User = mi.MysqlConfig.UserName
	cfg.Password = mi.MysqlConfig.Password
	cfg.Dump.ExecutionPath = "" // ignore mysqldump, use binlog only
	cfg.IncludeTableRegex = ruleRegex
	// cfg.Logger = &log.Logger{}
	mi.canalConfig = cfg
}

func (mi *MysqlInputPlugin) StartInput(pos position.Position, syncChan *channel.SyncChannel) position.Position {
	// 初始化canal
	c, err := canal.NewCanal(mi.canalConfig)
	if err != nil {
		log.Fatal(err)
	}
	mi.canal = c

	// Register a handler to handle RowsEvent
	c.SetEventHandler(mi)

	var mysqlPos = &position.MysqlPosition{}
	if err := mapstructure.Decode(pos, mysqlPos); err != nil {
		log.Fatalf("mysql position parsing failed. err: %s", err.Error())
	}

	var gs mysql.GTIDSet
	if mysqlPos.BinlogGTID != "" {
		if gs, err = mysql.ParseGTIDSet("mysql", mysqlPos.BinlogGTID); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Infof("%s param 'binlog-gtid' not exist", mysqlPos.FilePath)
		log.Infof("The configuration file [input] param 'start-gtid' not exist")
		log.Infof("get the current 'binlog-gtid' value")
		if gs, err = c.GetMasterGTIDSet(); err != nil {
			log.Fatal(err)
		}
		if gs.String() == "" {
			log.Fatal("the gtid value is empty, please confirm whether to enable gtid!")
		}
		mysqlPos.BinlogGTID = gs.String()
		if err := mysqlPos.SavePosition(); err != nil {
			log.Fatal(err)
		}
	}
	// assign value
	mi.syncChan = syncChan
	mi.position = mysqlPos

	// Start canal
	go func() {
		err := c.StartFromGTID(gs)
		if err != nil {
			log.Fatal(err)
		}
	}()

	// Start metrics
	mi.StartMetrics()

	return mysqlPos
}

func (mi *MysqlInputPlugin) StartMetrics() {
	mi.promTimingMetrics()
}

func (mi *MysqlInputPlugin) Close() {
	mi.canal.Close()
	log.Infof("close mysql input canal.")
	mi.cancel()
	mi.wg.Wait()
	log.Infof("close mysql input metrics.")
}

func (mi *MysqlInputPlugin) OnRow(e *canal.RowsEvent) error {
	msgs := mi.eventPreProcessing(e)
	for _, m := range msgs {
		mi.syncChan.SyncChan <- m
	}
	return nil
}

func (mi *MysqlInputPlugin) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	ctlMsg := &msg.Msg{
		Type:                msg.MsgCtl,
		InputContext:        &inputContext{BinlogName: pos.Name, BinlogPos: pos.Pos, BinlogGTID: set.String()},
		AfterCommitCallback: mi.AfterMsgCommit,
	}
	mi.syncChan.SyncChan <- ctlMsg
	return nil
}

func (mi *MysqlInputPlugin) eventPreProcessing(e *canal.RowsEvent) []*msg.Msg {
	var msgs []*msg.Msg
	if e.Action == canal.InsertAction {
		for _, row := range e.Rows {
			data := make(map[string]interface{})
			for j := 0; j < len(e.Table.Columns); j++ {
				data[e.Table.Columns[j].Name] = row[j]
			}
			log.Debugf("msg event: %s %s.%s %v\n", e.Action, e.Table.Schema, e.Table.Name, row)
			msgs = append(msgs, &msg.Msg{
				Table:     e.Table.Name,
				Database:  e.Table.Schema,
				Type:      msg.MsgDML,
				DmlMsg:    &msg.DMLMsg{Action: msg.InsertAction, Data: data},
				Timestamp: time.Unix(int64(e.Header.Timestamp), 0),
			})

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
			log.Debugf("msg event: %s %s.%s %v\n", e.Action, e.Table.Schema, e.Table.Name, row)
			msgs = append(msgs, &msg.Msg{
				Table:     e.Table.Name,
				Database:  e.Table.Schema,
				Type:      msg.MsgDML,
				DmlMsg:    &msg.DMLMsg{Action: msg.UpdateAction, Data: data},
				Timestamp: time.Unix(int64(e.Header.Timestamp), 0),
			})
		}
		return msgs
	}
	if e.Action == canal.DeleteAction {
		for _, row := range e.Rows {
			data := make(map[string]interface{})
			for j := 0; j < len(e.Table.Columns); j++ {
				data[e.Table.Columns[j].Name] = row[j]
			}
			log.Debugf("msg event: %s %s.%s %v\n", e.Action, e.Table.Schema, e.Table.Name, row)
			msgs = append(msgs, &msg.Msg{
				Table:     e.Table.Name,
				Database:  e.Table.Schema,
				Type:      msg.MsgDML,
				DmlMsg:    &msg.DMLMsg{Action: msg.DeleteAction, Data: data},
				Timestamp: time.Unix(int64(e.Header.Timestamp), 0),
			})

		}
		return msgs
	}
	log.Fatalf("msg actionType: %s not found")
	return nil
}

func (mi *MysqlInputPlugin) AfterMsgCommit(msg *msg.Msg) error {
	ctx := msg.InputContext.(*inputContext)
	if ctx.BinlogGTID != "" {
		if err := mi.position.ModifyPosition(ctx.BinlogGTID); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (mi *MysqlInputPlugin) promTimingMetrics() {
	mi.wg.Add(1)
	go func() {
		defer mi.wg.Done()
		ticker := time.NewTicker(time.Second * 3)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// prom sync delay set
				metrics.DelayReadTime.Set(float64(mi.canal.GetDelay()))
			case <-mi.ctx.Done():
				return
			}
		}
	}()
}
