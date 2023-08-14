package api

import (
	"encoding/json"
	"fmt"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/input"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/output"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/rule"
	"github.com/siddontang/go-log/log"
	"io/ioutil"
	"net/http"
	"regexp"
)

func AddRuleHandle(ip input.Plugin, oo output.Plugin) func(http.ResponseWriter, *http.Request) {
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

		// result http msg
		_, err = w.Write([]byte("result: add rule handle successfully.\n"))
		if err != nil {
			log.Errorf("http response write err: ", err.Error())
			return
		}
		/**
		// handle full data sync
		switch inputPlugin := ip.(type) {
		case *input.MysqlInputPlugin:
			// 创建go_mysql_sr_表， example：CREATE TABLE go_ods_case_info_mysql_sr_ LIKE ods_case_info;
			sourceSchema := addRuleMap["source-schema"]
			targetSchema := addRuleMap["target-schema"]
			sourceTable := addRuleMap["source-table"]
			targetTable := addRuleMap["target-table"]
			createSql := fmt.Sprintf("CREATE TABLE %s.go_%s_mysql_sr_ LIKE %s", targetSchema, targetTable, targetTable)
			switch outputPlugin := oo.(type) {
			case *output.Doris:
				_, err := outputPlugin.ExecuteSQL(createSql)
				if err != nil {
					log.Errorf("execute sql: %s failed, err: %v", createSql, err.Error())
					return
				}
				// 同步历史全量数据
				// init conn
				conn, err := client.Connect(fmt.Sprintf("%s:%d", inputPlugin.Host, inputPlugin.Port),
					inputPlugin.UserName, inputPlugin.Password, "", func(c *client.Conn) { c.SetCharset("utf8") })
				if err != nil {
					log.Errorf("add rule init conn failed. err: ", err.Error())
					return
				}
				// get primary key
				primarySql := fmt.Sprintf("SELECT COLUMN_NAME, DATA_TYPE "+
					"FROM INFORMATION_SCHEMA.COLUMNS "+
					"WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND COLUMN_KEY = 'PRI'", sourceSchema, sourceTable)
				rs, err := conn.Execute(primarySql)
				if err != nil {
					log.Errorf("add rule get primary key failed. err: ", err.Error())
					return
				}
				if rs.RowNumber() > 1 {
					log.Errorf("handling multiple primary keys is not currently supported")
					return
				}
				primaryKeyColumnName, _ := rs.GetString(0, 0)
				primaryKeyDataType, _ := rs.GetString(0, 1)
				if !strings.Contains(primaryKeyDataType, "int") {
					log.Errorf("handling primary key not int type is not currently supported")
					return
				}
				// get min key value

			case *output.Starrocks:
				_, err := outputPlugin.ExecuteSQL(createSql)
				if err != nil {
					log.Errorf("execute sql: %s failed, err: %v", createSql, err.Error())
				}
			}
				-- 同步历史全量数据

				-- 通过insert into select 把增量数据覆盖到go_mysql_sr_表
				insert into go_ods_case_info_mysql_sr_ select * from ods_case_info;
				-- 备份增量表
				ALTER TABLE ods_case_info RENAME go_ods_case_info_del_bak_mysql_sr_;
				-- 重命名go_mysql_sr_表
				ALTER TABLE go_ods_case_info_mysql_sr_ RENAME ods_case_info;
				-- 删除备份增量表
				DROP TABLE go_ods_case_info_del_bak_mysql_sr_;
		}
		*/
		return
	}
}

// A DelRuleHandle for delete rule handle.
func DelRuleHandle(ip input.Plugin, oo output.Plugin) func(http.ResponseWriter, *http.Request) {
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
		delRuleFmt, _ := json.Marshal(delRule)
		log.Infof("delete rule map: %v", string(delRuleFmt))
		log.Infof("delete rule successfully")

		_, err = w.Write([]byte("result: delete rule handle successfully.\n"))
		if err != nil {
			log.Errorf("http response write err: ", err.Error())
			return
		}
		return
	}
}

func GetRuleHandle(oo output.Plugin) func(http.ResponseWriter, *http.Request) {
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

func PauseHandle(oo output.Plugin) func(http.ResponseWriter, *http.Request) {
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

func ResumeHandle(oo output.Plugin) func(http.ResponseWriter, *http.Request) {
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
