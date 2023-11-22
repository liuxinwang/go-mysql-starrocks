package output

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/juju/errors"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/channel"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/core"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/metrics"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/registry"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/rule"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/schema"
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
	"strings"
	"sync"
	"time"
)

type Mysql struct {
	*config.MysqlConfig
	tableLock     sync.RWMutex
	tables        map[string]*schema.Table
	ruleLock      sync.RWMutex
	rulesMap      map[string]*rule.MysqlRule
	lastCtlMsg    *msg.Msg
	syncTimestamp time.Time // sync chan中last event timestamp
	ackTimestamp  time.Time // sync data ack的 event timestamp
	close         bool
	connLock      sync.Mutex
	conn          *client.Conn
	inSchema      core.Schema
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	pauseC        chan bool
	resumeC       chan bool
	paused        bool
}

const MysqlName = "mysql"

func init() {
	registry.RegisterPlugin(registry.OutputPlugin, MysqlName, &Mysql{})
}

func (m *Mysql) Configure(pipelineName string, configOutput map[string]interface{}) error {
	m.MysqlConfig = &config.MysqlConfig{}
	var target = configOutput["target"]
	err := mapstructure.Decode(target, m.MysqlConfig)
	if err != nil {
		log.Fatal("output.target config parsing failed. err: %v", err.Error())
	}
	return nil
}

func (m *Mysql) NewOutput(outputConfig interface{}, rulesMap map[string]interface{}, inSchema core.Schema) {
	// init map obj
	m.tables = make(map[string]*schema.Table)
	m.rulesMap = make(map[string]*rule.MysqlRule)

	m.ctx, m.cancel = context.WithCancel(context.Background())

	m.close = false
	m.StartMetrics()
	var err error
	// init conn
	m.conn, err = client.Connect(fmt.Sprintf("%s:%d", m.Host, m.Port), m.UserName, m.Password, "")
	_ = m.conn.SetCharset("utf8mb4")
	if err != nil {
		log.Fatal("output config conn failed. err: ", err.Error())
	}
	// init rulesMap
	if err = mapstructure.Decode(rulesMap, &m.rulesMap); err != nil {
		log.Fatal(err)
	}
	m.inSchema = inSchema
	m.pauseC = make(chan bool, 1)
	m.resumeC = make(chan bool, 1)
	m.paused = false
}

func (m *Mysql) StartOutput(outputChan *channel.OutputChannel) {
	m.wg.Add(1)

	ticker := time.NewTicker(time.Second * time.Duration(outputChan.FLushCHanMaxWaitSecond))
	defer ticker.Stop()
	defer m.wg.Done()

	for {
		select {
		case v := <-outputChan.SyncChan:
			switch data := v.(type) {
			case *msg.Msg:
				if data.Type == msg.MsgCtl {
					m.lastCtlMsg = data
					continue
				}

				m.syncTimestamp = data.Timestamp

				schemaTable := rule.RuleKeyFormat(data.Database, data.Table)

				ruleMap, ok := m.rulesMap[schemaTable]
				if !ok {
					log.Fatalf("get ruleMap failed: %v", schemaTable)
				}
				tableObj, err := m.inSchema.GetTable(ruleMap.SourceSchema, ruleMap.SourceTable)
				if err != nil {
					log.Fatal(err)
				}
				err = m.RewriteExecute(data, tableObj, ruleMap.TargetSchema, ruleMap.TargetTable)
				if err != nil {
					log.Fatalf("do mysql sync err %v, close sync", err)
					m.cancel()
					return
				}
			}
		case <-m.ctx.Done():
			m.close = true
		case <-ticker.C:
		case <-m.pauseC:
			m.paused = true
			<-m.resumeC
			select {
			default:
				m.paused = false
				continue
			}
		}
		// only start lastCtlMsg is nil
		if m.lastCtlMsg == nil {
			if m.close {
				log.Infof("not found lastCtlMsg and output data, not last one flush.")
				return
			} else {
				continue
			}
		}

		if m.lastCtlMsg.AfterCommitCallback != nil {
			err := m.lastCtlMsg.AfterCommitCallback(m.lastCtlMsg)
			if err != nil {
				log.Fatalf("ack msg failed: %v", errors.ErrorStack(err))
			}
			// log.Debugf("after commit callback lastCtl: %v", m.lastCtlMsg.InputContext)
		} else {
			log.Fatalf("not found AfterCommitCallback func")
		}

		m.ackTimestamp = m.syncTimestamp
		if m.close {
			log.Infof("last one flush output data done.")
			return
		}
	}
}

func (m *Mysql) Execute(msgs []*msg.Msg, table *schema.Table, targetSchema string, targetTable string) error {
	return nil
}

func (m *Mysql) RewriteExecute(event *msg.Msg, table *schema.Table, targetSchema string, targetTable string) error {
	outPutSQL, args, err := m.generateSQL(event, table)
	log.Debugf("mysql custom row data: %v; args: %v", outPutSQL, args)
	if err != nil {
		return err
	}
	_, err = m.ExecuteSQL(outPutSQL, args...)
	if err != nil {
		return err
	}
	return nil
}

func (m *Mysql) Close() {
	m.cancel()
	m.connLock.Lock()
	err := m.conn.Close()
	if err != nil {
		log.Fatalf("close mysql output err: %v", err.Error())
	}
	m.conn = nil
	m.connLock.Unlock()
	m.wg.Wait()
	log.Infof("close mysql output goroutine.")
	log.Infof("close mysql output metrics goroutine.")
}

func (m *Mysql) AddRule(config map[string]interface{}) error {
	m.ruleLock.Lock()
	defer m.ruleLock.Unlock()
	mr := &rule.MysqlRule{Deleted: false}
	if err := mapstructure.Decode(config, mr); err != nil {
		return errors.Trace(errors.New(fmt.Sprintf("add rule config parsing failed. err: %v", err.Error())))
	}
	// if exists, return
	ruleKey := rule.RuleKeyFormat(mr.SourceSchema, mr.SourceTable)
	for _, ruleObj := range m.rulesMap {
		tmpRuleKey := rule.RuleKeyFormat(ruleObj.SourceSchema, ruleObj.SourceTable)
		if ruleKey == tmpRuleKey {
			if ruleObj.Deleted {
				// if deleted is true, break, waiting to cover.
				break
			}
			return errors.New("output table rule already exists.")
		}
	}
	m.rulesMap[rule.RuleKeyFormat(mr.SourceSchema, mr.SourceTable)] = mr
	return nil
}

func (m *Mysql) DeleteRule(config map[string]interface{}) error {
	m.ruleLock.Lock()
	defer m.ruleLock.Unlock()
	dsr := &rule.DorisRule{}
	if err := mapstructure.Decode(config, dsr); err != nil {
		return errors.Trace(errors.New(fmt.Sprintf("delete rule config parsing failed. err: %v", err.Error())))
	}
	// if exists delete
	ruleKey := rule.RuleKeyFormat(dsr.SourceSchema, dsr.SourceTable)
	for _, ruleObj := range m.rulesMap {
		tmpRuleKey := rule.RuleKeyFormat(ruleObj.SourceSchema, ruleObj.SourceTable)
		if ruleKey == tmpRuleKey {
			// delete(m.rulesMap, ruleKey)
			// only mark deleted
			m.rulesMap[ruleKey].Deleted = true
			return nil
		}
	}
	return errors.New("output table rule not exists.")
}

func (m *Mysql) GetRules() interface{} {
	return m.rulesMap
}

func (m *Mysql) GetTable(db string, table string) (*schema.Table, error) {
	key := fmt.Sprintf("%s.%s", db, table)
	m.tableLock.RLock()
	t, ok := m.tables[key]
	m.tableLock.RUnlock()
	if ok {
		return t, nil
	}

	r, err := m.ExecuteSQL(fmt.Sprintf("show full columns from `%s`.`%s`", db, table))
	if err != nil {
		return nil, err
	}
	ta := &schema.Table{
		Schema:  db,
		Name:    table,
		Columns: make([]schema.TableColumn, 0, 16),
	}
	for i := 0; i < r.RowNumber(); i++ {
		name, _ := r.GetString(i, 0)
		ta.Columns = append(ta.Columns, schema.TableColumn{Name: name})
	}
	m.tableLock.Lock()
	m.tables[key] = ta
	m.tableLock.Unlock()
	return ta, nil
}

func (m *Mysql) generateSQL(event *msg.Msg, table *schema.Table) (string, []interface{}, error) {
	// datetime 0000-00-00 00:00:00 write err handle
	err := m.datetimeHandle(event, table)
	if err != nil {
		log.Fatalf("datetime type handle failed: %v", err.Error())
	}
	schemaTable := rule.RuleKeyFormat(event.Database, event.Table)
	ruleMap, _ := m.rulesMap[schemaTable]

	if len(ruleMap.PrimaryKeys) <= 0 {
		return "", nil, errors.Errorf("only support data has primary key")
	}
	pks := make(map[string]interface{}, len(ruleMap.PrimaryKeys))
	for _, pk := range ruleMap.PrimaryKeys {
		pks[pk] = event.DmlMsg.Data[pk]
	}

	switch event.DmlMsg.Action {
	case msg.InsertAction, msg.UpdateAction:
		// TODO insert on duplicate key
		updateColumnsIdx := 0
		columnNamesAssignWithoutPks := make([]string, len(ruleMap.TargetColumns)-len(ruleMap.PrimaryKeys))
		argsWithoutPks := make([]interface{}, len(ruleMap.TargetColumns)-len(ruleMap.PrimaryKeys))
		allColumnNamesInSQL := make([]string, 0, len(ruleMap.TargetColumns))
		allColumnPlaceHolder := make([]string, len(ruleMap.TargetColumns))
		args := make([]interface{}, 0, len(ruleMap.TargetColumns))
		for i, column := range ruleMap.TargetColumns {
			columnNameInSQL := fmt.Sprintf("`%s`", column)
			allColumnNamesInSQL = append(allColumnNamesInSQL, columnNameInSQL)
			allColumnPlaceHolder[i] = "?"
			columnData := event.DmlMsg.Data[ruleMap.SourceColumns[i]]
			args = append(args, columnData)
			_, ok := pks[ruleMap.SourceColumns[i]]
			if !ok {
				columnNamesAssignWithoutPks[updateColumnsIdx] = fmt.Sprintf("%s = ?", columnNameInSQL)
				// columnData := event.DmlMsg.Data[ruleMap.SourceColumns[i]]
				argsWithoutPks[updateColumnsIdx] = columnData
				updateColumnsIdx++
			}
		}
		sqlInsert := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
			ruleMap.TargetSchema,
			ruleMap.TargetTable,
			strings.Join(allColumnNamesInSQL, ","),
			strings.Join(allColumnPlaceHolder, ","))
		sqlUpdate := fmt.Sprintf("ON DUPLICATE KEY UPDATE %s", strings.Join(columnNamesAssignWithoutPks, ","))
		return fmt.Sprintf("%s %s", sqlInsert, sqlUpdate), append(args, argsWithoutPks...), nil
	case msg.DeleteAction:
		// TODO delete
		var whereSql []string
		var args []interface{}
		for i, column := range ruleMap.TargetColumns {
			pkData, ok := pks[ruleMap.SourceColumns[i]]
			if !ok {
				continue
			}
			whereSql = append(whereSql, fmt.Sprintf("`%s` = ?", column))
			args = append(args, pkData)
		}
		if len(whereSql) == 0 {
			return "", nil, errors.Errorf("where sql is empty, probably missing pk")
		}

		stmt := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s", ruleMap.TargetSchema, ruleMap.TargetTable, strings.Join(whereSql, " AND "))
		return stmt, args, nil
	default:
		log.Fatalf("unhandled message type: %v", event)
	}
	return "", nil, errors.Errorf("err handle data: %v", event)
}

func (m *Mysql) StartMetrics() {
	m.promTimingMetrics()
}

func (m *Mysql) promTimingMetrics() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(time.Second * 3)
		defer ticker.Stop()
		var newDelaySeconds uint32
		for {
			select {
			case <-ticker.C:
				// prom write delay set
				now := time.Now()
				if m.syncTimestamp.IsZero() || m.ackTimestamp.IsZero() || m.syncTimestamp == m.ackTimestamp {
					newDelaySeconds = 0
				} else {
					newDelaySeconds = uint32(now.Sub(m.ackTimestamp).Seconds())
				}
				// log.Debugf("write delay %vs", newDelay)
				metrics.DelayWriteTime.Set(float64(newDelaySeconds))
			case <-m.ctx.Done():
				return
			}
		}
	}()
}

func (m *Mysql) ExecuteSQL(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
	m.connLock.Lock()
	defer m.connLock.Unlock()
	argF := make([]func(*client.Conn), 0)
	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if m.conn == nil {
			m.conn, err = client.Connect(fmt.Sprintf("%s:%d", m.Host, m.Port), m.UserName, m.Password, "", argF...)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		rr, err = m.conn.Execute(cmd, args...)
		if err != nil && !mysql.ErrorEqual(err, mysql.ErrBadConn) {
			return
		} else if mysql.ErrorEqual(err, mysql.ErrBadConn) {
			err := m.conn.Close()
			if err != nil {
				return nil, err
			}
			m.conn = nil
			continue
		} else {
			return
		}
	}
	return
}

func (m *Mysql) datetimeHandle(ev *msg.Msg, table *schema.Table) error {
	for _, column := range table.Columns {
		if column.Type == schema.TypeDatetime {
			if ev.DmlMsg.Data[column.Name] == "0000-00-00 00:00:00" {
				ev.DmlMsg.Data[column.Name] = "0000-01-01 00:00:00"
			}
		}
	}
	return nil
}

func (m *Mysql) Pause() error {
	m.pauseC <- true
	return nil
}

func (m *Mysql) Resume() error {
	m.resumeC <- true
	return nil
}

func (m *Mysql) IsPaused() bool {
	return m.paused
}
