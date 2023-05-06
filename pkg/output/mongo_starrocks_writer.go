package output

import (
	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"go-mysql-starrocks/pkg/msg"
	"go-mysql-starrocks/pkg/rule"
	"net/http"
	"strings"
)

func (sr *Starrocks) MongoExecute(msgs []*msg.MongoMsg, rule *rule.MongoToSrRule, coll *msg.Coll) error {
	// fill column
	sr.fillColumn(msgs, coll.Columns)

	var jsonList []string
	jsonList = sr.generateMongoJSON(msgs)
	log.Debugf("starrocks bulk custom %s.%s row data num: %d", rule.TargetSchema, rule.TargetTable, len(jsonList))
	for _, s := range jsonList {
		log.Debugf("starrocks bulk custom %s.%s row data: %v", rule.TargetSchema, rule.TargetTable, s)
	}
	return sr.sendMongoData(jsonList, coll, rule)
}

func (sr *Starrocks) fillColumn(msgs []*msg.MongoMsg, columns []msg.CollColumn) {
	for _, mongoMsg := range msgs {
		for _, column := range columns {
			_, ok := mongoMsg.Data[column.Name]
			if !ok {
				mongoMsg.Data[column.Name] = nil
			}
		}
	}
}

func (sr *Starrocks) generateMongoJSON(msgs []*msg.MongoMsg) []string {
	var jsonList []string

	for _, event := range msgs {
		switch event.OperationType {
		case msg.MongoInsertAction:
			// 增加虚拟列，标识操作类型 (stream load opType：UPSERT 0，DELETE：1)
			event.Data[tmpColumn] = 0
			b, _ := json.Marshal(event.Data)
			jsonList = append(jsonList, string(b))
		case msg.MongoUpdateAction:
			// 增加虚拟列，标识操作类型 (stream load opType：UPSERT 0，DELETE：1)
			event.Data[tmpColumn] = 0
			b, _ := json.Marshal(event.Data)
			jsonList = append(jsonList, string(b))
		case msg.MongoDeleteAction: // starrocks2.4版本只支持primary key模型load delete
			// 增加虚拟列，标识操作类型 (stream load opType：UPSERT 0，DELETE：1)
			event.Data[tmpColumn] = 1
			b, _ := json.Marshal(event.Data)
			jsonList = append(jsonList, string(b))
		case msg.MongoReplaceAction:
			// 增加虚拟列，标识操作类型 (stream load opType：UPSERT 0，DELETE：1)
			event.Data[tmpColumn] = 0
			b, _ := json.Marshal(event.Data)
			jsonList = append(jsonList, string(b))
		}
	}
	return jsonList
}

func (sr *Starrocks) sendMongoData(content []string, coll *msg.Coll, rule *rule.MongoToSrRule) error {
	client := &http.Client{
		/** CheckRedirect: func(req *http.Request, via []*http.Request) error {
			req.Header.Add("Authorization", "Basic "+sr.auth())
			return nil // return nil nil回重定向。
		}, */
	}
	loadUrl := fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load",
		sr.Host, sr.Port, rule.TargetSchema, rule.TargetTable)
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
	for _, column := range coll.Columns {
		/*if sr.isContain(ignoreColumns, column) {
			continue
		}*/
		columnArray = append(columnArray, column.Name)
	}
	columnArray = append(columnArray, tmpColumn)
	columns := fmt.Sprintf("%s, __op = %s", strings.Join(columnArray, ","), tmpColumn)
	req.Header.Add("columns", columns)

	response, err := client.Do(req)
	if err != nil {
		return errors.Trace(err)
	}
	returnMap, err := sr.parseResponse(response)
	if returnMap["Status"] != "Success" {
		message := returnMap["Message"]
		errorUrl := returnMap["ErrorURL"]
		errorMsg := message.(string) + fmt.Sprintf(", targetTable: %s", rule.TargetString()) +
			fmt.Sprintf(", visit ErrorURL to view error details, ErrorURL: %s", errorUrl)
		if strings.Contains(fmt.Sprintf("%v", message), "unknown table") {
			// 生成建表语句
			sr.generateReferenceDDL(rule, coll)
		}
		return errors.Trace(errors.New(errorMsg))
	}
	return nil
}

func (sr *Starrocks) generateReferenceDDL(rule *rule.MongoToSrRule, coll *msg.Coll) {
	// 生成建表语句
	tableDDL := fmt.Sprintf("create table %s(\n", rule.TargetTable)
	tableDDL += "_id string,\n"
	for _, column := range coll.Columns {
		if column.RawType == "primitive.DateTime" {
			tableDDL += column.Name + " datetime,\n"
		} else if column.RawType == "primitive.A" || column.RawType == "map[string]interface {}" {
			tableDDL += column.Name + " json,\n"
		} else if column.RawType == "bool" {
			tableDDL += column.Name + " boolean,\n"
		} else if column.RawType == "int32" {
			tableDDL += column.Name + " int,\n"
		} else if column.Name == "_id" && column.RawType == "primitive.ObjectID" {
			continue
		} else if column.RawType == "int64" {
			tableDDL += column.Name + " bigint,\n"
		} else if column.RawType == "primitive.ObjectID" {
			tableDDL += column.Name + " string,\n"
		} else {
			tableDDL += column.Name + " " + column.RawType + ",\n"
		}
	}
	tableDDL += ") ENGINE=OLAP\nUNIQUE KEY(`_id`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`_id`) BUCKETS 4\nPROPERTIES (\n\"replication_num\" = \"1\",\n\"in_memory\" = \"false\",\n\"storage_format\" = \"DEFAULT\",\n\"enable_persistent_index\" = \"false\"\n);"

	log.Debugf("%s", tableDDL)
}
