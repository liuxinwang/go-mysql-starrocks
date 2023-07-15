package input

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/juju/errors"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/channel"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/metrics"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/position"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/rule"
	schema2 "github.com/liuxinwang/go-mysql-starrocks/pkg/schema"
	"github.com/mitchellh/mapstructure"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/siddontang/go-log/log"
	"regexp"
	"sync"
	"time"
)

type MysqlInputPlugin struct {
	canal.DummyEventHandler
	*config.MysqlConfig
	canalConfig         *canal.Config
	syncChan            *channel.SyncChannel
	canal               *canal.Canal
	position            position.Position
	inSchema            schema2.Schema
	parser              *parser.Parser
	syncPosition        *position.MysqlBasePosition
	ctlMsgFlushPosition *position.MysqlBasePosition // only ddl before handle
	wg                  sync.WaitGroup
	ctx                 context.Context
	cancel              context.CancelFunc
}

type inputContext struct {
	BinlogName string `toml:"binlog-name"`
	BinlogPos  uint32 `toml:"binlog-pos"`
	BinlogGTID string `toml:"binlog-gtid"`
	force      bool
}

func (mi *MysqlInputPlugin) NewInput(inputConfig interface{}, ruleRegex []string, inSchema schema2.Schema) {
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
	mi.inSchema = inSchema
	mi.parser = parser.New()
	mi.ctlMsgFlushPosition = &position.MysqlBasePosition{BinlogName: "", BinlogPos: 0, BinlogGTID: ""}
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

	var mysqlPos = &position.MysqlPositionV2{}
	if err := mapstructure.Decode(pos, mysqlPos); err != nil {
		log.Fatalf("mysql position parsing failed. err: %s", err.Error())
	}

	var gs mysql.GTIDSet
	if mysqlPos.BinlogGTID != "" {
		if gs, err = mysql.ParseGTIDSet("mysql", mysqlPos.BinlogGTID); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Infof("load 'binlog-gtid' from db not exist")
		log.Infof("config file [input] param 'start-gtid' not exist")
		log.Infof("start get the current 'binlog-gtid' value")
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
	mi.syncPosition = &position.MysqlBasePosition{BinlogName: mysqlPos.BinlogName, BinlogPos: mysqlPos.BinlogPos, BinlogGTID: mysqlPos.BinlogGTID}

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

func (mi *MysqlInputPlugin) SetIncludeTableRegex(config map[string]interface{}) (*regexp.Regexp, error) {
	sourceSchema := fmt.Sprintf("%v", config["source-schema"])
	sourceTable := fmt.Sprintf("%v", config["source-table"])
	cacheKey := fmt.Sprintf("%v.%v", sourceSchema, sourceTable)
	reg, err := regexp.Compile(rule.SchemaTableToStrRegex(sourceSchema, sourceTable))
	if err != nil {
		return reg, err
	}

	_, err = mi.canal.AddIncludeTableRegex(cacheKey, reg)
	if err != nil {
		return reg, err
	}
	return reg, nil
}

func (mi *MysqlInputPlugin) RemoveIncludeTableRegex(config map[string]interface{}) (*regexp.Regexp, error) {
	sourceSchema := fmt.Sprintf("%v", config["source-schema"])
	sourceTable := fmt.Sprintf("%v", config["source-table"])
	cacheKey := fmt.Sprintf("%v.%v", sourceSchema, sourceTable)
	reg, err := regexp.Compile(rule.SchemaTableToStrRegex(sourceSchema, sourceTable))
	if err != nil {
		return reg, err
	}

	_, err = mi.canal.DelIncludeTableRegex(cacheKey, reg)
	if err != nil {
		return reg, err
	}
	return reg, nil
}

func (mi *MysqlInputPlugin) OnRow(e *canal.RowsEvent) error {
	msgs := mi.eventPreProcessing(e)
	for _, m := range msgs {
		mi.syncChan.SyncChan <- m
	}
	return nil
}

func (mi *MysqlInputPlugin) OnTableChanged(schema string, table string) error {
	// onDDL before
	// send flush data msg
	// mi.syncChan.SyncChan <- ctlMsg
	ctlMsg := &msg.Msg{
		Type:       msg.MsgCtl,
		PluginName: msg.MysqlPlugin,
		InputContext: &inputContext{ // last sync position
			BinlogName: mi.syncPosition.BinlogName,
			BinlogPos:  mi.syncPosition.BinlogPos,
			BinlogGTID: mi.syncPosition.BinlogGTID,
			force:      true},
		AfterCommitCallback: mi.AfterMsgCommit,
	}
	mi.syncChan.SyncChan <- ctlMsg

	// waiting flush data msgs...
	// if syncPosition gitd == ctlMsgFlushPosition gitd indicates that flush is complete
	for true {
		if mi.ctlMsgFlushPosition.BinlogGTID != "" {
			if mi.syncPosition.BinlogGTID == mi.ctlMsgFlushPosition.BinlogGTID {
				break
			}
		}
		time.Sleep(time.Second * 1)
	}
	return nil
}

func (mi *MysqlInputPlugin) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	ctlMsg := &msg.Msg{
		Type:                msg.MsgCtl,
		PluginName:          msg.MysqlPlugin,
		InputContext:        &inputContext{BinlogName: pos.Name, BinlogPos: pos.Pos, BinlogGTID: set.String(), force: false},
		AfterCommitCallback: mi.AfterMsgCommit,
	}
	mi.syncChan.SyncChan <- ctlMsg
	mi.syncPosition.BinlogName = pos.Name
	mi.syncPosition.BinlogPos = pos.Pos
	mi.syncPosition.BinlogGTID = set.String()
	return nil
}

func (mi *MysqlInputPlugin) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	db := string(queryEvent.Schema)
	ddl := string(queryEvent.Query)
	stmts, _, err := mi.parser.Parse(ddl, "", "")
	if err != nil {
		log.Fatalf("parse query(%s) err %v", queryEvent.Query, err)
	}
	log.Infof("ddl event: %v", ddl)
	for _, stmt := range stmts {
		ns := mi.parseStmt(stmt)
		for _, n := range ns {
			if n.db == "" {
				n.db = db
			}
			if n.db == position.DbName {
				continue
			}

			// fix go-mysql-server not support column charset
			reg, _ := regexp.Compile("charset \\w*")
			ddl = reg.ReplaceAllString(ddl, "")

			err = mi.inSchema.UpdateTable(n.db, n.table, ddl)
			if err != nil {
				log.Warnf("handle query(%s) err %v", queryEvent.Query, err)
			}
		}
	}
	return nil
}

func (mi *MysqlInputPlugin) eventPreProcessing(e *canal.RowsEvent) []*msg.Msg {
	var msgs []*msg.Msg
	if e.Action == canal.InsertAction {
		for _, row := range e.Rows {
			data := make(map[string]interface{})

			if len(row) != len(e.Table.Columns) {
				columns := make([]string, 0, 16)
				for _, column := range e.Table.Columns {
					columns = append(columns, column.Name)
				}
				log.Warnf("insert %s.%s columns and data mismatch in length: %d vs %d, table %v",
					e.Table.Schema, e.Table.Name, len(e.Table.Columns), len(row), columns)
				log.Infof("load table:%s.%s meta columns from local", e.Table.Schema, e.Table.Name)
				ta, err := mi.inSchema.GetTable(e.Table.Schema, e.Table.Name)
				if err != nil {
					log.Fatalf("get tables failed, err: %v", err.Error())
				}
				if len(row) != len(ta.Columns) {
					log.Warnf("insert %s.%s columns and data mismatch in local length: %d vs %d, table %v",
						e.Table.Schema, e.Table.Name, len(ta.Columns), len(row), ta.GetTableColumnsName())
				}
				for j := 0; j < len(row); j++ {
					data[ta.Columns[j].Name] = deserializeForLocal(row[j], ta.Columns[j])
				}
			} else {
				for j := 0; j < len(row); j++ {
					data[e.Table.Columns[j].Name] = deserialize(row[j], e.Table.Columns[j])
				}
			}

			log.Debugf("msg event: %s %s.%s %v\n", e.Action, e.Table.Schema, e.Table.Name, data)
			msgs = append(msgs, &msg.Msg{
				Table:      e.Table.Name,
				Database:   e.Table.Schema,
				Type:       msg.MsgDML,
				DmlMsg:     &msg.DMLMsg{Action: msg.InsertAction, Data: data},
				Timestamp:  time.Unix(int64(e.Header.Timestamp), 0),
				PluginName: msg.MysqlPlugin,
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

			if len(row) != len(e.Table.Columns) {
				columns := make([]string, 0, 16)
				for _, column := range e.Table.Columns {
					columns = append(columns, column.Name)
				}
				log.Warnf("update %s.%s columns and data mismatch in length: %d vs %d, table %v",
					e.Table.Schema, e.Table.Name, len(e.Table.Columns), len(row), columns)
				log.Infof("load table:%s.%s meta columns from local", e.Table.Schema, e.Table.Name)
				ta, err := mi.inSchema.GetTable(e.Table.Schema, e.Table.Name)
				if err != nil {
					log.Fatalf("get tables failed, err: %v", err.Error())
				}
				if len(row) != len(ta.Columns) {
					log.Warnf("update %s.%s columns and data mismatch in local length: %d vs %d, table %v",
						e.Table.Schema, e.Table.Name, len(ta.Columns), len(row), ta.GetTableColumnsName())
				}
				for j := 0; j < len(row); j++ {
					data[ta.Columns[j].Name] = deserializeForLocal(row[j], ta.Columns[j])
					old[ta.Columns[j].Name] = deserializeForLocal(e.Rows[i-1][j], ta.Columns[j])
				}
			} else {
				for j := 0; j < len(row); j++ {
					data[e.Table.Columns[j].Name] = deserialize(row[j], e.Table.Columns[j])
					old[e.Table.Columns[j].Name] = deserialize(e.Rows[i-1][j], e.Table.Columns[j])
				}
			}

			log.Debugf("msg event: %s %s.%s %v\n", e.Action, e.Table.Schema, e.Table.Name, data)
			msgs = append(msgs, &msg.Msg{
				Table:      e.Table.Name,
				Database:   e.Table.Schema,
				Type:       msg.MsgDML,
				DmlMsg:     &msg.DMLMsg{Action: msg.UpdateAction, Data: data},
				Timestamp:  time.Unix(int64(e.Header.Timestamp), 0),
				PluginName: msg.MysqlPlugin,
			})
		}
		return msgs
	}
	if e.Action == canal.DeleteAction {
		for _, row := range e.Rows {
			data := make(map[string]interface{})

			if len(row) != len(e.Table.Columns) {
				log.Warnf("delete %s.%s columns and data mismatch in length: %d vs %d",
					e.Table.Schema, e.Table.Name, len(e.Table.Columns), len(row))
				log.Infof("load table:%s.%s meta columns from local", e.Table.Schema, e.Table.Name)
				ta, err := mi.inSchema.GetTable(e.Table.Schema, e.Table.Name)
				if err != nil {
					log.Fatalf("get tables failed, err: %v", err.Error())
				}
				if len(row) != len(ta.Columns) {
					log.Warnf("delete %s.%s columns and data mismatch in local length: %d vs %d, table %v",
						e.Table.Schema, e.Table.Name, len(ta.Columns), len(row), ta.GetTableColumnsName())
				}
				for j := 0; j < len(row); j++ {
					data[ta.Columns[j].Name] = deserializeForLocal(row[j], ta.Columns[j])
				}
			} else {
				for j := 0; j < len(row); j++ {
					data[e.Table.Columns[j].Name] = deserialize(row[j], e.Table.Columns[j])
				}
			}

			log.Debugf("msg event: %s %s.%s %v\n", e.Action, e.Table.Schema, e.Table.Name, data)
			msgs = append(msgs, &msg.Msg{
				Table:      e.Table.Name,
				Database:   e.Table.Schema,
				Type:       msg.MsgDML,
				DmlMsg:     &msg.DMLMsg{Action: msg.DeleteAction, Data: data},
				Timestamp:  time.Unix(int64(e.Header.Timestamp), 0),
				PluginName: msg.MysqlPlugin,
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
		if ctx.force {
			// flush position
			if err := mi.position.SavePosition(); err != nil {
				log.Fatalf("msg event position save failed: %v", errors.ErrorStack(err))
			}
			mi.ctlMsgFlushPosition.BinlogName = ctx.BinlogName
			mi.ctlMsgFlushPosition.BinlogPos = ctx.BinlogPos
			mi.ctlMsgFlushPosition.BinlogGTID = ctx.BinlogGTID
		}
	} else {
		log.Warnf("after msg commit binlog gtid is empty, no modify position! msg: %v", msg.InputContext)
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

func deserialize(raw interface{}, column schema.TableColumn) interface{} {
	if raw == nil {
		return nil
	}

	ret := raw
	if column.RawType == "text" || column.RawType == "json" {
		_, ok := raw.([]uint8)
		if ok {
			ret = string(raw.([]uint8))
		}
	}
	return ret
}

func deserializeForLocal(raw interface{}, column schema2.TableColumn) interface{} {
	if raw == nil {
		return nil
	}

	ret := raw
	if column.RawType == "text" || column.RawType == "json" {
		_, ok := raw.([]uint8)
		if ok {
			ret = string(raw.([]uint8))
		}
	}
	return ret
}

type node struct {
	db    string
	table string
}

func (mi *MysqlInputPlugin) parseStmt(stmt ast.StmtNode) (ns []*node) {
	switch t := stmt.(type) {
	case *ast.RenameTableStmt:
		for _, tableInfo := range t.TableToTables {
			n := &node{
				db:    tableInfo.OldTable.Schema.String(),
				table: tableInfo.OldTable.Name.String(),
			}
			ns = append(ns, n)
		}
	case *ast.AlterTableStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Name.String(),
		}
		ns = []*node{n}
	case *ast.DropTableStmt:
		for _, table := range t.Tables {
			n := &node{
				db:    table.Schema.String(),
				table: table.Name.String(),
			}
			ns = append(ns, n)
		}
	case *ast.CreateTableStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Name.String(),
		}
		ns = []*node{n}
	case *ast.TruncateTableStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Name.String(),
		}
		ns = []*node{n}
	}
	return ns
}
