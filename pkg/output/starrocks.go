package output

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-log/log"
	"go-mysql-starrocks/pkg/config"
	"go-mysql-starrocks/pkg/rule"
	"io/ioutil"
	"net/http"
	"strings"
)

type Starrocks struct {
	*config.Starrocks
}

var tmpColumn = "_sl_optype"

func (sr *Starrocks) Execute(events []*canal.RowsEvent, rule *rule.MysqlToSrRule, table *schema.Table) error {
	if len(events) == 0 {
		return nil
	}
	var jsonList []string

	jsonList = sr.generateJSON(events)
	log.Infof("starrocks bulk custom row data num: %d", len(jsonList))
	return sr.sendData(jsonList, table, rule)
}

func (sr *Starrocks) generateJSON(events []*canal.RowsEvent) []string {
	var jsonList []string

	for _, event := range events {
		switch event.Action {
		case canal.InsertAction:
			dataMap := make(map[string]interface{})
			for _, row := range event.Rows {
				for j, column := range event.Table.Columns {
					dataMap[column.Name] = row[j]
				}
				// 增加虚拟列，标识操作类型 (stream load opType：UPSERT 0，DELETE：1)
				dataMap[tmpColumn] = 0
				b, _ := json.Marshal(dataMap)
				jsonList = append(jsonList, string(b))
			}
		case canal.UpdateAction:
			dataMap := make(map[string]interface{})
			for i, row := range event.Rows {
				if i == 0 || i%2 == 0 {
					continue
				}
				for j, column := range event.Table.Columns {
					dataMap[column.Name] = row[j]
				}
				// 增加虚拟列，标识操作类型 (stream load opType：UPSERT 0，DELETE：1)
				dataMap[tmpColumn] = 0
				b, _ := json.Marshal(dataMap)
				jsonList = append(jsonList, string(b))
			}
		case canal.DeleteAction:
			dataMap := make(map[string]interface{})
			for _, row := range event.Rows {
				for j, column := range event.Table.Columns {
					dataMap[column.Name] = row[j]
				}
				// 增加虚拟列，标识操作类型 (stream load opType：UPSERT 0，DELETE：1)
				dataMap[tmpColumn] = 1
				b, _ := json.Marshal(dataMap)
				jsonList = append(jsonList, string(b))
			}
		}
	}
	return jsonList
}

func (sr *Starrocks) auth() string {
	s := sr.UserName + ":" + sr.Password
	b := []byte(s)

	sEnc := base64.StdEncoding.EncodeToString(b)
	return sEnc
}

func (sr *Starrocks) sendData(content []string, table *schema.Table, rule *rule.MysqlToSrRule) error {
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
	// req.Header.Add("label", "39c25a5c-7000-496e-a98e-348a264c81de")
	req.Header.Add("format", "json")
	req.Header.Add("strip_outer_array", "true")
	var columnArray []string
	for _, column := range table.Columns {
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
		msg := returnMap["Message"]
		return errors.Trace(errors.New(msg.(string)))
	}
	return nil
}

func (sr *Starrocks) parseResponse(response *http.Response) (map[string]interface{}, error) {
	var result map[string]interface{}
	body, err := ioutil.ReadAll(response.Body)
	if err == nil {
		err = json.Unmarshal(body, &result)
	}

	return result, err
}
