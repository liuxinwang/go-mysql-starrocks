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
	"github.com/liuxinwang/go-mysql-starrocks/pkg/rule"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/schema"
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Starrocks struct {
	*config.StarrocksConfig
	tableLock     sync.RWMutex
	tables        map[string]*schema.Table
	ruleLock      sync.RWMutex
	rulesMap      map[string]*rule.StarrocksRule
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
	tr            *http.Transport
	cli           *http.Client
}

func (sr *Starrocks) NewOutput(outputConfig interface{}, rulesMap map[string]interface{}, inSchema core.Schema) {
	// init map obj
	sr.tables = make(map[string]*schema.Table)
	sr.rulesMap = make(map[string]*rule.StarrocksRule)

	sr.ctx, sr.cancel = context.WithCancel(context.Background())

	sr.StarrocksConfig = &config.StarrocksConfig{}
	err := mapstructure.Decode(outputConfig, sr.StarrocksConfig)
	if err != nil {
		log.Fatal("output config parsing failed. err: ", err.Error())
	}
	sr.close = false
	sr.StartMetrics()
	// init conn
	sr.conn, err = client.Connect(fmt.Sprintf("%s:%d", sr.Host, sr.Port), sr.UserName, sr.Password, "")
	if err != nil {
		log.Fatal("output config conn failed. err: ", err.Error())
	}
	// init rulesMap
	if err = mapstructure.Decode(rulesMap, &sr.rulesMap); err != nil {
		log.Fatal(err)
	}
	sr.inSchema = inSchema
	sr.pauseC = make(chan bool, 1)
	sr.resumeC = make(chan bool, 1)
	sr.paused = false
	sr.tr = &http.Transport{}
	sr.cli = &http.Client{
		Transport: sr.tr,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			req.Header.Add("Authorization", "Basic "+sr.auth())
			// log.Debugf("重定向请求到be: %v", req.URL)
			return nil // return nil nil回重定向。
		},
	}
}

func (sr *Starrocks) StartOutput(outputChan *channel.OutputChannel) {
	sr.wg.Add(1)

	ticker := time.NewTicker(time.Second * time.Duration(outputChan.FLushCHanMaxWaitSecond))
	defer ticker.Stop()
	defer sr.wg.Done()

	eventsLen := 0
	schemaTableEvents := make(map[string][]*msg.Msg)
	for {
		needFlush := false
		select {
		case v := <-outputChan.SyncChan:
			switch data := v.(type) {
			case *msg.Msg:
				if data.Type == msg.MsgCtl {
					sr.lastCtlMsg = data
					continue
				}

				sr.syncTimestamp = data.Timestamp

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
		case <-sr.ctx.Done():
			needFlush = true
			log.Infof("wait last one flush output data...")
			sr.close = true
		case <-ticker.C:
			needFlush = true
		case <-sr.pauseC:
			sr.paused = true
			<-sr.resumeC
			select {
			default:
				sr.paused = false
				continue
			}
		}

		if needFlush {
			for schemaTable := range schemaTableEvents {
				ruleMap, ok := sr.rulesMap[schemaTable]
				if !ok {
					log.Fatalf("get ruleMap failed: %v", schemaTable)
				}
				tableObj, err := sr.inSchema.GetTable(ruleMap.SourceSchema, ruleMap.SourceTable)
				// tableObj, err := sr.GetTable(ruleMap.TargetSchema, ruleMap.TargetTable)
				if err != nil {
					log.Fatal(err)
				}

				err = sr.Execute(schemaTableEvents[schemaTable], tableObj, ruleMap.TargetSchema, ruleMap.TargetTable)
				if err != nil {
					log.Fatalf("do starrocks bulk err %v, close sync", err)
					sr.cancel()
					return
				}
				delete(schemaTableEvents, schemaTable)
			}

			// only start lastCtlMsg is nil
			/*if sr.lastCtlMsg == nil {
				if sr.close {
					log.Infof("not found lastCtlMsg and output data, not last one flush.")
					return
				} else {
					continue
				}
			}*/

			if sr.lastCtlMsg.AfterCommitCallback != nil {
				err := sr.lastCtlMsg.AfterCommitCallback(sr.lastCtlMsg)
				if err != nil {
					log.Fatalf("ack msg failed: %v", errors.ErrorStack(err))
				}
				// log.Debugf("after commit callback lastCtl: %v", sr.lastCtlMsg.InputContext)
			} else {
				log.Fatalf("not found AfterCommitCallback func")
			}

			sr.ackTimestamp = sr.syncTimestamp
			eventsLen = 0
			// ticker.Reset(time.Second * time.Duration(outputChan.FLushCHanMaxWaitSecond))
			if sr.close {
				log.Infof("last one flush output data done.")
				return
			}
		}
	}
}

func (sr *Starrocks) Execute(msgs []*msg.Msg, table *schema.Table, targetSchema string, targetTable string) error {
	if len(msgs) == 0 {
		return nil
	}
	var jsonList []string

	jsonList = sr.generateJSON(msgs)
	log.Debugf("starrocks bulk custom %s.%s row data num: %d", targetSchema, targetTable, len(jsonList))
	for _, s := range jsonList {
		log.Debugf("starrocks custom %s.%s row data: %v", targetSchema, targetTable, s)
	}
	return sr.SendData(jsonList, table, targetSchema, targetTable, nil)
}

func (sr *Starrocks) Close() {
	sr.cancel()
	sr.connLock.Lock()
	err := sr.conn.Close()
	if err != nil {
		log.Fatalf("close starrocks output err: %v", err.Error())
	}
	sr.conn = nil
	sr.connLock.Unlock()
	sr.wg.Wait()
	log.Infof("close starrocks output goroutine.")
	log.Infof("close starrocks output metrics goroutine.")
}

func (sr *Starrocks) AddRule(config map[string]interface{}) error {
	sr.ruleLock.Lock()
	defer sr.ruleLock.Unlock()
	srr := &rule.StarrocksRule{Deleted: false}
	if err := mapstructure.Decode(config, srr); err != nil {
		return errors.Trace(errors.New(fmt.Sprintf("add rule config parsing failed. err: %v", err.Error())))
	}
	// if exists, return
	ruleKey := rule.RuleKeyFormat(srr.SourceSchema, srr.SourceTable)
	for _, ruleObj := range sr.rulesMap {
		tmpRuleKey := rule.RuleKeyFormat(ruleObj.SourceSchema, ruleObj.SourceTable)
		if ruleKey == tmpRuleKey {
			if ruleObj.Deleted {
				// if deleted is true, break, waiting to cover.
				break
			}
			return errors.New("output table rule already exists.")
		}
	}
	sr.rulesMap[rule.RuleKeyFormat(srr.SourceSchema, srr.SourceTable)] = srr
	return nil
}

func (sr *Starrocks) DeleteRule(config map[string]interface{}) error {
	sr.ruleLock.Lock()
	defer sr.ruleLock.Unlock()
	srr := &rule.StarrocksRule{}
	if err := mapstructure.Decode(config, srr); err != nil {
		return errors.Trace(errors.New(fmt.Sprintf("add rule config parsing failed. err: %v", err.Error())))
	}
	// if exists, return
	ruleKey := rule.RuleKeyFormat(srr.SourceSchema, srr.SourceTable)
	for _, ruleObj := range sr.rulesMap {
		tmpRuleKey := rule.RuleKeyFormat(ruleObj.SourceSchema, ruleObj.SourceTable)
		if ruleKey == tmpRuleKey {
			// delete(sr.rulesMap, ruleKey)
			// only mark deleted
			sr.rulesMap[ruleKey].Deleted = true
			return nil
		}
	}
	return errors.New("output table rule not exists.")
}

func (sr *Starrocks) GetRules() interface{} {
	return sr.rulesMap
}

func (sr *Starrocks) GetTable(db string, table string) (*schema.Table, error) {
	key := fmt.Sprintf("%s.%s", db, table)
	sr.tableLock.RLock()
	t, ok := sr.tables[key]
	sr.tableLock.RUnlock()
	if ok {
		return t, nil
	}
	r, err := sr.ExecuteSQL(fmt.Sprintf("show full columns from `%s`.`%s`", db, table))
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
	sr.tableLock.Lock()
	sr.tables[key] = ta
	sr.tableLock.Unlock()
	return ta, nil
}

func (sr *Starrocks) generateJSON(msgs []*msg.Msg) []string {
	var jsonList []string

	for _, event := range msgs {
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

func (sr *Starrocks) SendData(content []string, table *schema.Table, targetSchema string, targetTable string, ignoreColumns []string) error {
	/**cli := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			req.Header.Add("Authorization", "Basic "+sr.auth())
			req.URL.Host = "127.0.0.1:8040"
			log.Infof("重定向请求到be: %v", req.URL)
			return nil // return nil nil回重定向。
		},
	}*/
	loadUrl := fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load",
		sr.Host, sr.LoadPort, targetSchema, targetTable)
	newContent := `[` + strings.Join(content, ",") + `]`
	req, _ := http.NewRequest("PUT", loadUrl, strings.NewReader(newContent))

	// req.Header.Add
	req.Header.Add("Authorization", "Basic "+sr.auth())
	req.Header.Add("Expect", "100-continue")
	req.Header.Add("strict_mode", "true")
	// req.Header.Add("label", "39c25a5c-7000-496e-a98e-348a264c81de")
	req.Header.Add("format", "json")
	req.Header.Add("strip_outer_array", "true")

	var columnArray []string
	for _, column := range table.Columns {
		if sr.isContain(ignoreColumns, column.Name) {
			continue
		}
		columnArray = append(columnArray, column.Name)
	}
	columnArray = append(columnArray, DeleteColumn)
	columns := fmt.Sprintf("%s, __op = %s", strings.Join(columnArray, ","), DeleteColumn)
	req.Header.Add("columns", columns)

	response, err := sr.cli.Do(req)
	if err != nil {
		return errors.Trace(err)
	}
	defer response.Body.Close()
	returnMap, err := sr.parseResponse(response)
	if err != nil {
		return errors.Trace(err)
	}
	if returnMap["Status"] != "Success" {
		log.Error(err.Error())
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

func (sr *Starrocks) auth() string {
	s := sr.UserName + ":" + sr.Password
	b := []byte(s)

	sEnc := base64.StdEncoding.EncodeToString(b)
	return sEnc
}

func (sr *Starrocks) isContain(items []string, item string) bool {
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

func (sr *Starrocks) parseResponse(response *http.Response) (map[string]interface{}, error) {
	var result map[string]interface{}
	body, err := ioutil.ReadAll(response.Body)
	if err == nil {
		err = json.Unmarshal(body, &result)
	}

	return result, err
}

func (sr *Starrocks) StartMetrics() {
	sr.promTimingMetrics()
}

func (sr *Starrocks) promTimingMetrics() {
	sr.wg.Add(1)
	go func() {
		defer sr.wg.Done()
		ticker := time.NewTicker(time.Second * 3)
		defer ticker.Stop()
		var newDelaySeconds uint32
		for {
			select {
			case <-ticker.C:
				// prom write delay set
				now := time.Now()
				if sr.syncTimestamp.IsZero() || sr.ackTimestamp.IsZero() || sr.syncTimestamp == sr.ackTimestamp {
					newDelaySeconds = 0
				} else {
					newDelaySeconds = uint32(now.Sub(sr.ackTimestamp).Seconds())
				}
				// log.Debugf("write delay %vs", newDelay)
				metrics.DelayWriteTime.Set(float64(newDelaySeconds))
			case <-sr.ctx.Done():
				return
			}
		}
	}()
}

func (sr *Starrocks) ExecuteSQL(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
	sr.connLock.Lock()
	defer sr.connLock.Unlock()
	argF := make([]func(*client.Conn), 0)
	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if sr.conn == nil {
			sr.conn, err = client.Connect(fmt.Sprintf("%s:%d", sr.Host, sr.Port), sr.UserName, sr.Password, "", argF...)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		rr, err = sr.conn.Execute(cmd, args...)
		if err != nil && !mysql.ErrorEqual(err, mysql.ErrBadConn) {
			return
		} else if mysql.ErrorEqual(err, mysql.ErrBadConn) {
			err := sr.conn.Close()
			if err != nil {
				return nil, err
			}
			sr.conn = nil
			continue
		} else {
			return
		}
	}
	return
}

func (sr *Starrocks) Pause() error {
	sr.pauseC <- true
	return nil
}

func (sr *Starrocks) Resume() error {
	sr.resumeC <- true
	return nil
}

func (sr *Starrocks) IsPaused() bool {
	return sr.paused
}
