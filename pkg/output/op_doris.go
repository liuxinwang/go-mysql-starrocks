package output

import (
	"context"
	"encoding/base64"
	"encoding/json"
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
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Doris struct {
	*config.DorisConfig
	tableLock     sync.RWMutex
	tables        map[string]*schema.Table
	ruleLock      sync.RWMutex
	rulesMap      map[string]*rule.DorisRule
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

const Name = "doris"

func init() {
	registry.RegisterPlugin(registry.OutputPlugin, Name, &Doris{})
}

func (ds *Doris) Configure(pipelineName string, configOutput map[string]interface{}) error {
	ds.DorisConfig = &config.DorisConfig{}
	var target = configOutput["target"]
	err := mapstructure.Decode(target, ds.DorisConfig)
	if err != nil {
		log.Fatal("output.target config parsing failed. err: %v", err.Error())
	}
	return nil
}

func (ds *Doris) NewOutput(outputConfig interface{}, rulesMap map[string]interface{}, inSchema core.Schema) {
	// init map obj
	ds.tables = make(map[string]*schema.Table)
	ds.rulesMap = make(map[string]*rule.DorisRule)

	ds.ctx, ds.cancel = context.WithCancel(context.Background())

	ds.close = false
	ds.StartMetrics()
	var err error
	// init conn
	ds.conn, err = client.Connect(fmt.Sprintf("%s:%d", ds.Host, ds.Port), ds.UserName, ds.Password, "")
	if err != nil {
		log.Fatal("output config conn failed. err: ", err.Error())
	}
	// init rulesMap
	if err = mapstructure.Decode(rulesMap, &ds.rulesMap); err != nil {
		log.Fatal(err)
	}
	ds.inSchema = inSchema
	ds.pauseC = make(chan bool, 1)
	ds.resumeC = make(chan bool, 1)
	ds.paused = false
}

func (ds *Doris) StartOutput(outputChan *channel.OutputChannel) {
	ds.wg.Add(1)

	ticker := time.NewTicker(time.Second * time.Duration(outputChan.FLushCHanMaxWaitSecond))
	defer ticker.Stop()
	defer ds.wg.Done()

	eventsLen := 0
	schemaTableEvents := make(map[string][]*msg.Msg)
	for {
		needFlush := false
		select {
		case v := <-outputChan.SyncChan:
			switch data := v.(type) {
			case *msg.Msg:
				if data.Type == msg.MsgCtl {
					ds.lastCtlMsg = data
					continue
				}

				ds.syncTimestamp = data.Timestamp

				schemaTable := rule.RuleKeyFormat(data.Database, data.Table)
				rowsData, ok := schemaTableEvents[schemaTable]
				if !ok {
					schemaTableEvents[schemaTable] = make([]*msg.Msg, 0, outputChan.ChannelSize)
				}
				schemaTableEvents[schemaTable] = append(rowsData, data)
				eventsLen += 1

				if eventsLen >= outputChan.ChannelSize {
					needFlush = true
				}
			}
		case <-ds.ctx.Done():
			needFlush = true
			log.Infof("wait last one flush output data...")
			ds.close = true
		case <-ticker.C:
			needFlush = true
		case <-ds.pauseC:
			ds.paused = true
			<-ds.resumeC
			select {
			default:
				ds.paused = false
				continue
			}
		}

		if needFlush {
			for schemaTable := range schemaTableEvents {
				ruleMap, ok := ds.rulesMap[schemaTable]
				if !ok {
					log.Fatalf("get ruleMap failed: %v", schemaTable)
				}
				tableObj, err := ds.inSchema.GetTable(ruleMap.SourceSchema, ruleMap.SourceTable)
				if err != nil {
					log.Fatal(err)
				}

				err = ds.Execute(schemaTableEvents[schemaTable], tableObj, ruleMap.TargetSchema, ruleMap.TargetTable)
				if err != nil {
					log.Fatalf("do doris bulk err %v, close sync", err)
					ds.cancel()
					return
				}
				delete(schemaTableEvents, schemaTable)
			}

			// only start lastCtlMsg is nil
			/*if ds.lastCtlMsg == nil {
				if ds.close {
					log.Infof("not found lastCtlMsg and output data, not last one flush.")
					return
				} else {
					continue
				}
			}*/

			if ds.lastCtlMsg.AfterCommitCallback != nil {
				err := ds.lastCtlMsg.AfterCommitCallback(ds.lastCtlMsg)
				if err != nil {
					log.Fatalf("ack msg failed: %v", errors.ErrorStack(err))
				}
				// log.Debugf("after commit callback lastCtl: %v", ds.lastCtlMsg.InputContext)
			} else {
				log.Fatalf("not found AfterCommitCallback func")
			}

			ds.ackTimestamp = ds.syncTimestamp
			eventsLen = 0
			// ticker.Reset(time.Second * time.Duration(outputChan.FLushCHanMaxWaitSecond))
			if ds.close {
				log.Infof("last one flush output data done.")
				return
			}
		}
	}
}

func (ds *Doris) Execute(msgs []*msg.Msg, table *schema.Table, targetSchema string, targetTable string) error {
	if len(msgs) == 0 {
		return nil
	}
	var jsonList []string

	jsonList = ds.generateJSON(msgs, table)
	log.Debugf("doris bulk custom %s.%s row data num: %d", targetSchema, targetTable, len(jsonList))
	for _, s := range jsonList {
		log.Debugf("doris custom %s.%s row data: %v", targetSchema, targetTable, s)
	}
	//TODO ignoreColumns handle
	return ds.SendData(jsonList, table, targetSchema, targetTable, nil)
}

func (ds *Doris) Close() {
	ds.cancel()
	ds.connLock.Lock()
	err := ds.conn.Close()
	if err != nil {
		log.Fatalf("close doris output err: %v", err.Error())
	}
	ds.conn = nil
	ds.connLock.Unlock()
	ds.wg.Wait()
	log.Infof("close doris output goroutine.")
	log.Infof("close doris output metrics goroutine.")
}

func (ds *Doris) AddRule(config map[string]interface{}) error {
	ds.ruleLock.Lock()
	defer ds.ruleLock.Unlock()
	dsr := &rule.DorisRule{Deleted: false}
	if err := mapstructure.Decode(config, dsr); err != nil {
		return errors.Trace(errors.New(fmt.Sprintf("add rule config parsing failed. err: %v", err.Error())))
	}
	// if exists, return
	ruleKey := rule.RuleKeyFormat(dsr.SourceSchema, dsr.SourceTable)
	for _, ruleObj := range ds.rulesMap {
		tmpRuleKey := rule.RuleKeyFormat(ruleObj.SourceSchema, ruleObj.SourceTable)
		if ruleKey == tmpRuleKey {
			if ruleObj.Deleted {
				// if deleted is true, break, waiting to cover.
				break
			}
			return errors.New("output table rule already exists.")
		}
	}
	ds.rulesMap[rule.RuleKeyFormat(dsr.SourceSchema, dsr.SourceTable)] = dsr
	return nil
}

func (ds *Doris) DeleteRule(config map[string]interface{}) error {
	ds.ruleLock.Lock()
	defer ds.ruleLock.Unlock()
	dsr := &rule.DorisRule{}
	if err := mapstructure.Decode(config, dsr); err != nil {
		return errors.Trace(errors.New(fmt.Sprintf("delete rule config parsing failed. err: %v", err.Error())))
	}
	// if exists delete
	ruleKey := rule.RuleKeyFormat(dsr.SourceSchema, dsr.SourceTable)
	for _, ruleObj := range ds.rulesMap {
		tmpRuleKey := rule.RuleKeyFormat(ruleObj.SourceSchema, ruleObj.SourceTable)
		if ruleKey == tmpRuleKey {
			// delete(ds.rulesMap, ruleKey)
			// only mark deleted
			ds.rulesMap[ruleKey].Deleted = true
			return nil
		}
	}
	return errors.New("output table rule not exists.")
}

func (ds *Doris) GetRules() interface{} {
	return ds.rulesMap
}

func (ds *Doris) GetTable(db string, table string) (*schema.Table, error) {
	key := fmt.Sprintf("%s.%s", db, table)
	ds.tableLock.RLock()
	t, ok := ds.tables[key]
	ds.tableLock.RUnlock()
	if ok {
		return t, nil
	}

	r, err := ds.ExecuteSQL(fmt.Sprintf("show full columns from `%s`.`%s`", db, table))
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
	ds.tableLock.Lock()
	ds.tables[key] = ta
	ds.tableLock.Unlock()
	return ta, nil
}

func (ds *Doris) generateJSON(msgs []*msg.Msg, table *schema.Table) []string {
	var jsonList []string

	for _, event := range msgs {
		// datetime 0000-00-00 00:00:00 write err handle
		err := ds.datetimeHandle(event, table)
		if err != nil {
			log.Fatalf("datetime type handle failed: %v", err.Error())
		}

		switch event.DmlMsg.Action {
		case msg.InsertAction:
			// 增加虚拟列，标识操作类型 (stream load opType：UPSERT 0，DELETE：1)
			event.DmlMsg.Data[DeleteColumn] = 0
			b, _ := json.Marshal(event.DmlMsg.Data)
			jsonList = append(jsonList, string(b))
		case msg.UpdateAction:
			// 增加虚拟列，标识操作类型 (stream load opType：UPSERT 0，DELETE：1)
			event.DmlMsg.Data[DeleteColumn] = 0
			b, _ := json.Marshal(event.DmlMsg.Data)
			jsonList = append(jsonList, string(b))
		case msg.DeleteAction: // starrocks2.4版本只支持primary key模型load delete
			// 增加虚拟列，标识操作类型 (stream load opType：UPSERT 0，DELETE：1)
			event.DmlMsg.Data[DeleteColumn] = 1
			b, _ := json.Marshal(event.DmlMsg.Data)
			jsonList = append(jsonList, string(b))
		case msg.ReplaceAction: // for mongo
			// 增加虚拟列，标识操作类型 (stream load opType：UPSERT 0，DELETE：1)
			event.DmlMsg.Data[DeleteColumn] = 0
			b, _ := json.Marshal(event.DmlMsg.Data)
			jsonList = append(jsonList, string(b))
		default:
			log.Fatalf("unhandled message type: %v", event)
		}
	}
	return jsonList
}

func (ds *Doris) SendData(content []string, table *schema.Table, targetSchema string, targetTable string, ignoreColumns []string) error {
	cli := &http.Client{
		/** CheckRedirect: func(req *http.Request, via []*http.Request) error {
			req.Header.Add("Authorization", "Basic "+sr.auth())
			return nil // return nil nil回重定向。
		}, */
	}
	loadUrl := fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load",
		ds.Host, ds.LoadPort, targetSchema, targetTable)
	newContent := `[` + strings.Join(content, ",") + `]`
	req, _ := http.NewRequest("PUT", loadUrl, strings.NewReader(newContent))

	// req.Header.Add
	req.Header.Add("Authorization", "Basic "+ds.auth())
	req.Header.Add("Expect", "100-continue")
	req.Header.Add("strict_mode", "true")
	// req.Header.Add("label", "39c25a5c-7000-496e-a98e-348a264c81de")
	req.Header.Add("format", "json")
	req.Header.Add("strip_outer_array", "true")
	req.Header.Add("merge_type", "MERGE")
	req.Header.Add("delete", DeleteCondition)
	var columnArray []string
	for _, column := range table.Columns {
		if ds.isContain(ignoreColumns, column.Name) {
			continue
		}
		columnArray = append(columnArray, column.Name)
	}
	columnArray = append(columnArray, DeleteColumn)
	columns := fmt.Sprintf("%s", strings.Join(columnArray, ","))
	req.Header.Add("columns", columns)

	response, err := cli.Do(req)
	if err != nil {
		return errors.Trace(err)
	}
	returnMap, err := ds.parseResponse(response)
	if returnMap["Status"] != "Success" {
		message := returnMap["Message"]
		errorUrl := returnMap["ErrorURL"]
		errorMsg := message.(string) +
			fmt.Sprintf(", targetTable: %s.%s", targetSchema, targetTable) +
			fmt.Sprintf(", visit ErrorURL to view error details, ErrorURL: %s", errorUrl)
		return errors.Trace(errors.New(errorMsg))
	}
	// prom write event number counter
	numberLoadedRows := returnMap["NumberLoadedRows"]
	metrics.OpsWriteProcessed.Add(numberLoadedRows.(float64))
	return nil
}

func (ds *Doris) auth() string {
	s := ds.UserName + ":" + ds.Password
	b := []byte(s)

	sEnc := base64.StdEncoding.EncodeToString(b)
	return sEnc
}

func (ds *Doris) isContain(items []string, item string) bool {
	if len(items) == 0 {
		return false
	}
	for _, eachItem := range items {
		if eachItem == item {
			return true
		}
	}
	return false
}

func (ds *Doris) parseResponse(response *http.Response) (map[string]interface{}, error) {
	var result map[string]interface{}
	body, err := io.ReadAll(response.Body)
	if err == nil {
		err = json.Unmarshal(body, &result)
	}

	return result, err
}

func (ds *Doris) StartMetrics() {
	ds.promTimingMetrics()
}

func (ds *Doris) promTimingMetrics() {
	ds.wg.Add(1)
	go func() {
		defer ds.wg.Done()
		ticker := time.NewTicker(time.Second * 3)
		defer ticker.Stop()
		var newDelaySeconds uint32
		for {
			select {
			case <-ticker.C:
				// prom write delay set
				now := time.Now()
				if ds.syncTimestamp.IsZero() || ds.ackTimestamp.IsZero() || ds.syncTimestamp == ds.ackTimestamp {
					newDelaySeconds = 0
				} else {
					newDelaySeconds = uint32(now.Sub(ds.ackTimestamp).Seconds())
				}
				// log.Debugf("write delay %vs", newDelay)
				metrics.DelayWriteTime.Set(float64(newDelaySeconds))
			case <-ds.ctx.Done():
				return
			}
		}
	}()
}

func (ds *Doris) ExecuteSQL(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
	ds.connLock.Lock()
	defer ds.connLock.Unlock()
	argF := make([]func(*client.Conn), 0)
	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if ds.conn == nil {
			ds.conn, err = client.Connect(fmt.Sprintf("%s:%d", ds.Host, ds.Port), ds.UserName, ds.Password, "", argF...)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		rr, err = ds.conn.Execute(cmd, args...)
		if err != nil && !mysql.ErrorEqual(err, mysql.ErrBadConn) {
			return
		} else if mysql.ErrorEqual(err, mysql.ErrBadConn) {
			err := ds.conn.Close()
			if err != nil {
				return nil, err
			}
			ds.conn = nil
			continue
		} else {
			return
		}
	}
	return
}

func (ds *Doris) datetimeHandle(ev *msg.Msg, table *schema.Table) error {
	for _, column := range table.Columns {
		if column.Type == schema.TypeDatetime {
			if ev.DmlMsg.Data[column.Name] == "0000-00-00 00:00:00" {
				ev.DmlMsg.Data[column.Name] = "0000-01-01 00:00:00"
			}
		}
	}
	return nil
}

func (ds *Doris) Pause() error {
	ds.pauseC <- true
	return nil
}

func (ds *Doris) Resume() error {
	ds.resumeC <- true
	return nil
}

func (ds *Doris) IsPaused() bool {
	return ds.paused
}
