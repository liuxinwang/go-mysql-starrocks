package api

import (
	"encoding/json"
	"fmt"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/input"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/rule"
	"github.com/siddontang/go-log/log"
	"io/ioutil"
	"net/http"
	"regexp"
)

func AddRuleHandle(h *input.MyEventHandler) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var addRule rule.MysqlToSrRule
		addRule.RuleType = "dynamic add"
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, err = w.Write([]byte(fmt.Sprintf("result: add rule json data read err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}
		err = json.Unmarshal(data, &addRule)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, err := w.Write([]byte(fmt.Sprintf("result: rule json data to json struct err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}

		// handle add rule
		c := h.C()

		reg, err := regexp.Compile(addRule.SourceSchema + "\\." + addRule.SourceTable + "$")
		if err != nil {
			_, err := w.Write([]byte(fmt.Sprintf("result: add rule handle failed err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}

		key := fmt.Sprintf("%s.%s", addRule.SourceSchema, addRule.SourceTable)
		_, err = c.AddIncludeTableRegex(key, reg)
		if err != nil {
			_, err := w.Write([]byte(fmt.Sprintf("result: add rule handle failed err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}
		log.Infof("add rule includeTableRegex: %v successfully", reg.String())

		h.RuleMap()[addRule.SourceSchema+":"+addRule.SourceTable] = &addRule
		addRuleFmt, _ := json.Marshal(addRule)
		log.Infof("add rule map: %v successfully", string(addRuleFmt))

		_, err = w.Write([]byte("result: add rule handle successfully.\n"))
		if err != nil {
			log.Errorf("http response write err: ", err.Error())
			return
		}
		return
	}
}

func DelRuleHandle(h *input.MyEventHandler) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var delRule rule.MysqlToSrRule
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, err = w.Write([]byte(fmt.Sprintf("result: delete rule json data read err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}
		err = json.Unmarshal(data, &delRule)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, err := w.Write([]byte(fmt.Sprintf("result: rule json data to json struct err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}

		// handle delete rule
		c := h.C()

		reg, err := regexp.Compile(delRule.SourceSchema + "\\." + delRule.SourceTable + "$")
		if err != nil {
			_, err := w.Write([]byte(fmt.Sprintf("result: delete rule handle failed err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}

		key := fmt.Sprintf("%s.%s", delRule.SourceSchema, delRule.SourceTable)
		_, err = c.DelIncludeTableRegex(key, reg)
		if err != nil {
			_, err := w.Write([]byte(fmt.Sprintf("result: delete rule handle failed err: %v\n", err.Error())))
			if err != nil {
				log.Errorf("http response write err: ", err.Error())
				return
			}
			return
		}
		log.Infof("delete rule includeTableRegex: %v successfully", reg.String())

		delete(h.RuleMap(), delRule.SourceSchema+":"+delRule.SourceTable)
		delRuleFmt, _ := json.Marshal(delRule)
		log.Infof("delete rule map: %v successfully", string(delRuleFmt))

		_, err = w.Write([]byte("result: delete rule handle successfully.\n"))
		if err != nil {
			log.Errorf("http response write err: ", err.Error())
			return
		}
		return
	}
}

func GetRuleHandle(h *input.MyEventHandler) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		rules, err := json.Marshal(h.RuleMap())
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
