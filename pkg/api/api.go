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
