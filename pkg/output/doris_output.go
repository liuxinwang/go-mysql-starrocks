package output

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/juju/errors"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/channel"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/metrics"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/rule"
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Doris struct {
	*config.DorisConfig
	tableLock     sync.RWMutex
	tables        map[string]*Table
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	rulesMap      map[string]*rule.DorisRule
	lastCtlMsg    *msg.Msg
	close         bool
	syncTimestamp time.Time // sync chan中last event timestamp
	ackTimestamp  time.Time // sync data ack的 event timestamp
}

var DeleteColumn = "_delete_sign_"
var DeleteCondition = fmt.Sprintf("%s=1", DeleteColumn)

func (ds *Doris) NewOutput(outputConfig interface{}) {
	// init map obj
	ds.tables = make(map[string]*Table)
	ds.rulesMap = make(map[string]*rule.DorisRule)

	ds.ctx, ds.cancel = context.WithCancel(context.Background())

	ds.DorisConfig = &config.DorisConfig{}
	err := mapstructure.Decode(outputConfig, ds.DorisConfig)
	if err != nil {
		log.Fatal("output config parsing failed. err: ", err.Error())
	}
	ds.close = false
	ds.StartMetrics()
}

func (ds *Doris) StartOutput(outputChan *channel.OutputChannel, rulesMap map[string]interface{}) {
	if err := mapstructure.Decode(rulesMap, &ds.rulesMap); err != nil {
		log.Fatal(err)
	}

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

				// prom read event number counter
				metrics.OpsReadProcessed.Inc()
				ds.syncTimestamp = data.Timestamp

				schemaTable := data.Database + ":" + data.Table
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
		}

		if needFlush {
			for schemaTable := range schemaTableEvents {
				// schema := strings.Split(schemaTable, ":")[0]
				// table := strings.Split(schemaTable, ":")[1]
				ruleMap, ok := ds.rulesMap[schemaTable]
				if !ok {
					log.Fatalf("get ruleMap failed: %v", schemaTable)
				}
				tableObj, err := ds.GetTable(ruleMap.TargetSchema, ruleMap.TargetTable)
				if err != nil {
					log.Fatal(err)
				}

				err = ds.Execute(schemaTableEvents[schemaTable], tableObj)
				if err != nil {
					log.Errorf("do doris bulk err %v, close sync", err)
					ds.cancel()
					return
				}
				delete(schemaTableEvents, schemaTable)
			}

			// only start lastCtlMsg is nil
			if ds.lastCtlMsg == nil {
				if ds.close {
					log.Infof("not found lastCtlMsg and output data, not last one flush.")
					return
				} else {
					continue
				}
			}

			if ds.lastCtlMsg.AfterCommitCallback != nil {
				err := ds.lastCtlMsg.AfterCommitCallback(ds.lastCtlMsg)
				if err != nil {
					log.Fatalf("ack msg failed: %v", errors.ErrorStack(err))
				}
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

func (ds *Doris) Execute(msgs []*msg.Msg, table *Table) error {
	if len(msgs) == 0 {
		return nil
	}
	var jsonList []string

	jsonList = ds.generateJSON(msgs)
	log.Debugf("doris bulk custom %s.%s row data num: %d", table.Schema, table.Name, len(jsonList))
	for _, s := range jsonList {
		log.Debugf("doris custom %s.%s row data: %v", table.Schema, table.Name, s)
	}
	//TODO ignoreColumns handle
	return ds.sendData(jsonList, table, nil)
}

func (ds *Doris) Close() {
	ds.cancel()
	ds.wg.Wait()
	log.Infof("close doris output goroutine.")
	log.Infof("close doris output metrics goroutine.")
}

func (ds *Doris) GetTable(db string, table string) (*Table, error) {
	key := fmt.Sprintf("%s.%s", db, table)
	ds.tableLock.RLock()
	t, ok := ds.tables[key]
	ds.tableLock.RUnlock()
	if ok {
		return t, nil
	}

	conn, err := client.Connect(
		fmt.Sprintf("%s:%d", ds.Host, ds.Port),
		ds.UserName, ds.Password, db)
	if err != nil {
		return nil, err
	}
	err = conn.Ping()
	if err != nil {
		return nil, err
	}
	var rs *mysql.Result
	rs, err = conn.Execute(fmt.Sprintf("show full columns from `%s`.`%s`", db, table))
	if err != nil {
		return nil, err
	}
	ta := &Table{
		Schema:  db,
		Name:    table,
		Columns: make([]string, 0, 2),
	}
	for i := 0; i < rs.RowNumber(); i++ {
		name, _ := rs.GetString(i, 0)
		ta.Columns = append(ta.Columns, name)
	}
	ds.tableLock.Lock()
	ds.tables[key] = ta
	ds.tableLock.Unlock()
	defer rs.Close()
	return ta, nil
}

func (ds *Doris) generateJSON(msgs []*msg.Msg) []string {
	var jsonList []string

	for _, event := range msgs {
		switch event.DmlMsg.Action {
		case canal.InsertAction:
			// 增加虚拟列，标识操作类型 (stream load opType：UPSERT 0，DELETE：1)
			event.DmlMsg.Data[DeleteColumn] = 0
			b, _ := json.Marshal(event.DmlMsg.Data)
			jsonList = append(jsonList, string(b))
		case canal.UpdateAction:
			// 增加虚拟列，标识操作类型 (stream load opType：UPSERT 0，DELETE：1)
			event.DmlMsg.Data[DeleteColumn] = 0
			b, _ := json.Marshal(event.DmlMsg.Data)
			jsonList = append(jsonList, string(b))
		case canal.DeleteAction: // starrocks2.4版本只支持primary key模型load delete
			// 增加虚拟列，标识操作类型 (stream load opType：UPSERT 0，DELETE：1)
			event.DmlMsg.Data[DeleteColumn] = 1
			b, _ := json.Marshal(event.DmlMsg.Data)
			jsonList = append(jsonList, string(b))
		}
	}
	return jsonList
}

func (ds *Doris) sendData(content []string, table *Table, ignoreColumns []string) error {
	cli := &http.Client{
		/** CheckRedirect: func(req *http.Request, via []*http.Request) error {
			req.Header.Add("Authorization", "Basic "+sr.auth())
			return nil // return nil nil回重定向。
		}, */
	}
	loadUrl := fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load",
		ds.Host, ds.LoadPort, table.Schema, table.Name)
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
		if ds.isContain(ignoreColumns, column) {
			continue
		}
		columnArray = append(columnArray, column)
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
			fmt.Sprintf(", targetTable: %s.%s", table.Schema, table.Name) +
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
	body, err := ioutil.ReadAll(response.Body)
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
