package api

import (
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/juju/errors"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/core"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/input"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/output"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/rule"
	"github.com/siddontang/go-log/log"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

func AddRuleHandle(ip core.Input, oo core.Output, schema core.Schema) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// read input param
		var addRuleMap = make(map[string]interface{}, 1)
		addRuleMap["RuleType"] = rule.TypeDynamicAdd
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			_, err = w.Write([]byte(fmt.Sprintf("result: add rule json data read err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}
		err = json.Unmarshal(data, &addRuleMap)
		if err != nil {
			_, err := w.Write([]byte(fmt.Sprintf("result: rule json data to json struct err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}

		isFullSync := false
		fullSync, ok := addRuleMap["full_sync"]
		if ok {
			switch fullSync.(type) {
			case bool:
				isFullSync, err = strconv.ParseBool(fmt.Sprintf("%v", fullSync))
				if err != nil {
					_, err = w.Write([]byte(fmt.Sprintf("result: full_sync incorrect type failed err: %v\n", err.Error())))
					if err != nil {
						log.Errorf("http response write err: ", err.Error())
						return
					}
					return
				}
			default:
				_, err = w.Write([]byte(fmt.Sprintf("result: param 'full_sync' incorrect type failed\n")))
				if err != nil {
					log.Errorf("http response write err: ", err.Error())
					return
				}
				return
			}
		}

		// schema table add
		sourceSchema := fmt.Sprintf("%v", addRuleMap["source-schema"])
		sourceTable := fmt.Sprintf("%v", addRuleMap["source-table"])
		_, err = schema.AddTable(sourceSchema, sourceTable)
		if err != nil {
			_, err := w.Write([]byte(fmt.Sprintf("result: add rule table meta handle failed err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}
		log.Infof("add schema table meta data: %v.%v", sourceSchema, sourceTable)

		// output rule map add
		err = oo.AddRule(addRuleMap)
		if err != nil {
			_, err := w.Write([]byte(fmt.Sprintf("result: add rule handle failed err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}

		addRuleFmt, _ := json.Marshal(addRuleMap)
		log.Infof("add rule map: %v", string(addRuleFmt))

		// input table regex add
		var reg *regexp.Regexp
		reg, err = ip.SetIncludeTableRegex(addRuleMap)
		if err != nil {
			_, err := w.Write([]byte(fmt.Sprintf("result: add rule handle failed err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}
		log.Infof("add rule includeTableRegex: %v", reg.String())
		log.Infof("add rule successfully")

		syncRows := 0
		if isFullSync {
			err = oo.Pause()
			log.Infof("pause output write")
			if err != nil {
				_, err = w.Write([]byte(fmt.Sprintf("result: pause err: %v\n", err.Error())))
				if err != nil {
					log.Errorf("http response write err: ", err.Error())
					return
				}
				return
			}

			// waiting handle full sync
			err, syncRows = FullSync(ip, oo, addRuleMap, schema)
			if err != nil {
				_, err := w.Write([]byte(fmt.Sprintf(
					"result: add rule full sync handle failed err: %v, full sync rows: %d\n",
					err.Error(), syncRows)))
				if err != nil {
					log.Errorf("http response write err: ", err.Error())
					return
				}

				err = oo.Resume()
				if err != nil {
					_, err = w.Write([]byte(fmt.Sprintf("result: resume err: %v\n", err.Error())))
					if err != nil {
						log.Errorf("http response write err: ", err.Error())
						return
					}
					return
				}
				log.Infof("resume output write")

				return
			}

			err = oo.Resume()
			if err != nil {
				_, err = w.Write([]byte(fmt.Sprintf("result: resume err: %v\n", err.Error())))
				if err != nil {
					log.Errorf("http response write err: ", err.Error())
					return
				}
				return
			}
			log.Infof("resume output write")
		}

		// result http msg
		if isFullSync {
			_, err = w.Write([]byte(fmt.Sprintf(
				"result: add rule handle successfully, full sync rows: %d.\n", syncRows)))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
		} else {
			_, err = w.Write([]byte("result: add rule handle successfully.\n"))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
		}

		return
	}
}

// A DelRuleHandle for delete rule handle.
func DelRuleHandle(ip core.Input, oo core.Output, schema core.Schema) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var delRule = make(map[string]interface{}, 1)
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			_, err = w.Write([]byte(fmt.Sprintf("result: delete rule json data read err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}
		err = json.Unmarshal(data, &delRule)
		if err != nil {
			_, err := w.Write([]byte(fmt.Sprintf("result: rule json data to json struct err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}

		// handle delete rule
		var reg *regexp.Regexp
		reg, err = ip.RemoveIncludeTableRegex(delRule)
		if err != nil {
			_, err := w.Write([]byte(fmt.Sprintf("result: delete rule handle failed err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}
		log.Infof("delete rule includeTableRegex: %v", reg.String())

		err = oo.DeleteRule(delRule)
		if err != nil {
			_, err := w.Write([]byte(fmt.Sprintf("result: delete rule table meta handle failed err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}
		delRuleFmt, _ := json.Marshal(delRule)
		log.Infof("delete rule map: %v", string(delRuleFmt))

		// schema table add
		sourceSchema := fmt.Sprintf("%v", delRule["source-schema"])
		sourceTable := fmt.Sprintf("%v", delRule["source-table"])
		err = schema.DelTable(sourceSchema, sourceTable)
		if err != nil {
			_, err := w.Write([]byte(fmt.Sprintf("result: delete rule table meta handle failed err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}

		log.Infof("delete rule table meta: %v", string(delRuleFmt))

		log.Infof("delete rule successfully")

		_, err = w.Write([]byte("result: delete rule handle successfully.\n"))
		if err != nil {
			log.Errorf("http response write err: ", err.Error())
			return
		}
		return
	}
}

func GetRuleHandle(oo core.Output) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		rules, err := json.Marshal(oo.GetRules())
		if err != nil {
			_, err = w.Write([]byte(fmt.Sprintf("result: rules to json err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}
		_, err = w.Write(rules)
		if err != nil {
			log.Errorf("http response write err: ", err.Error())
			return
		}
		return
	}
}

func PauseHandle(oo core.Output) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		err := oo.Pause()
		if err != nil {
			_, err = w.Write([]byte(fmt.Sprintf("result: pause err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}
		_, err = w.Write([]byte("result: pause handle successfully.\n"))
		if err != nil {
			log.Errorf("http response write err: ", err.Error())
			return
		}
		return
	}
}

func ResumeHandle(oo core.Output) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		err := oo.Resume()
		if err != nil {
			_, err = w.Write([]byte(fmt.Sprintf("result: resume err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}
		_, err = w.Write([]byte("result: resume handle successfully.\n"))
		if err != nil {
			log.Errorf("http response write err: ", err.Error())
			return
		}
		return
	}
}

func FullSync(ip core.Input, oo core.Output, ruleMap map[string]interface{}, s core.Schema) (e error, fullRows int) {
	// handle full data sync
	log.Infof("start handle full data sync...")
	switch inputPlugin := ip.(type) {
	case *input.MysqlInputPlugin:
		sourceSchema := fmt.Sprintf("%v", ruleMap["source-schema"])
		targetSchema := fmt.Sprintf("%v", ruleMap["target-schema"])
		sourceTable := fmt.Sprintf("%v", ruleMap["source-table"])
		targetTable := fmt.Sprintf("%v", ruleMap["target-table"])
		// 同步历史全量数据
		// init conn
		conn, err := client.Connect(fmt.Sprintf("%s:%d", inputPlugin.Host, inputPlugin.Port),
			inputPlugin.UserName, inputPlugin.Password, "", func(c *client.Conn) { c.SetCharset("utf8") })
		if err != nil {
			log.Errorf("rule map init conn failed. err: ", err.Error())
			return err, 0
		}
		// bug fix: c.SetCharset no set utf8mb4, separate set utf8mb4 support emoji
		_, _ = conn.Execute("set names utf8mb4")
		// get primary key
		primarySql := fmt.Sprintf("SELECT COLUMN_NAME, DATA_TYPE "+
			"FROM INFORMATION_SCHEMA.COLUMNS "+
			"WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND COLUMN_KEY = 'PRI'", sourceSchema, sourceTable)
		rs, err := conn.Execute(primarySql)
		if err != nil {
			log.Errorf("add rule get primary key failed. err: ", err.Error())
			return err, 0
		}

		if rs.RowNumber() == 0 {
			log.Errorf("handling not primary keys table is not currently supported")
			return errors.New("handling not primary keys table is not currently supported"), 0
		}

		var primaryKeyColumns []string
		for i := 0; i < rs.RowNumber(); i++ {
			columnName, _ := rs.GetString(i, 0)
			primaryKeyColumns = append(primaryKeyColumns, columnName)
		}

		/* if rs.RowNumber() > 1 {
			log.Errorf("handling multiple primary keys is not currently supported")
			return errors.New("handling multiple primary keys is not currently supported")
		}

		primaryKeyColumnName, _ := rs.GetString(0, 0)
		primaryKeyDataType, _ := rs.GetString(0, 1)
		if !strings.Contains(primaryKeyDataType, "int") {
			log.Errorf("handling primary key not int type is not currently supported")
			return errors.New("handling primary key not int type is not currently supported")
		} */

		queryColumnsSql := fmt.Sprintf("SELECT COLUMN_NAME "+
			"FROM information_schema.columns "+
			"WHERE table_schema = '%s' "+
			"AND table_name = '%s' ORDER BY ORDINAL_POSITION", sourceSchema, sourceTable)
		rs, err = conn.Execute(queryColumnsSql)
		if err != nil {
			log.Errorf("add rule get columns failed. err: ", err.Error())
			return err, 0
		}
		var columns []string
		for i := 0; i < rs.RowNumber(); i++ {
			columnName, _ := rs.GetString(i, 0)
			columns = append(columns, columnName)
		}
		querySql := fmt.Sprintf("SELECT * "+
			"FROM %s.%s ORDER BY %s", sourceSchema, sourceTable, strings.Join(primaryKeyColumns, ","))
		var result mysql.Result
		batchSize := 10000
		tmpIndex := 0
		var jsonRows []string
		tableObj, err := s.GetTable(sourceSchema, sourceTable)
		if err != nil {
			return err, 0
		}
		switch outputPlugin := oo.(type) {
		case *output.Doris:
			err = conn.ExecuteSelectStreaming(querySql, &result, func(row []mysql.FieldValue) error {
				m := make(map[string]interface{})
				for idx, val := range row {
					ret := val.Value()
					if val.Type == 4 {
						_, ok := val.Value().([]uint8)
						if ok {
							ret = string(val.Value().([]uint8))
						}
					}
					m[columns[idx]] = ret
				}
				m[output.DeleteColumn] = 0
				b, _ := json.Marshal(m)
				jsonRows = append(jsonRows, string(b))
				tmpIndex += 1
				if tmpIndex%batchSize == 0 {
					err = outputPlugin.SendData(jsonRows, tableObj, targetSchema, targetTable, nil)
					if err != nil {
						return err
					}
					fullRows = tmpIndex
					jsonRows = jsonRows[0:0]
				}
				return nil
			}, nil)

			if err != nil {
				log.Errorf("handling execute select streaming failed. err: %v", err.Error())
				return err, tmpIndex
			}

			if len(jsonRows) > 0 {
				err = outputPlugin.SendData(jsonRows, tableObj, targetSchema, targetTable, nil)
				if err != nil {
					return err, tmpIndex
				}
				fullRows = tmpIndex
			}
		case *output.Starrocks:
			err = conn.ExecuteSelectStreaming(querySql, &result, func(row []mysql.FieldValue) error {
				m := make(map[string]interface{})
				for idx, val := range row {
					ret := val.Value()
					if val.Type == 4 {
						_, ok := val.Value().([]uint8)
						if ok {
							ret = string(val.Value().([]uint8))
						}
					}
					m[columns[idx]] = ret
				}
				m[output.DeleteColumn] = 0
				b, _ := json.Marshal(m)
				jsonRows = append(jsonRows, string(b))
				tmpIndex += 1
				if tmpIndex%batchSize == 0 {
					err = outputPlugin.SendData(jsonRows, tableObj, targetSchema, targetTable, nil)
					if err != nil {
						return err
					}
					fullRows = tmpIndex
					jsonRows = jsonRows[0:0]
				}
				return nil
			}, nil)

			if err != nil {
				log.Errorf("handling execute select streaming failed. err: %v", err.Error())
				return err, tmpIndex
			}

			if len(jsonRows) > 0 {
				err = outputPlugin.SendData(jsonRows, tableObj, targetSchema, targetTable, nil)
				if err != nil {
					return err, tmpIndex
				}
				fullRows = tmpIndex
			}
		default:
			// TODO
		}
		log.Infof("full data sync total rows: %d", tmpIndex)
		err = conn.Close()
		if err != nil {
			return err, tmpIndex
		}
		log.Infof("close conn")
	}
	log.Infof("end handle full data sync")
	return nil, fullRows
}
