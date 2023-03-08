package filter

import (
	"fmt"
	"github.com/siddontang/go-log/log"
	"go-mysql-starrocks/pkg/msg"
	"regexp"
)

type ChangeStreamFilter interface {
	Filter(msg *msg.MongoMsg) bool
}

type ChangeStreamFilterMatcher []ChangeStreamFilter

type RuleFilter struct {
	includeTableRegex []*regexp.Regexp
}

func (matcher ChangeStreamFilterMatcher) IterateFilter(msg *msg.MongoMsg) bool {
	for _, filter := range matcher {
		if filter.Filter(msg) {
			log.Debugf("filter change stream event %v", msg.Data)
			return true
		}
	}
	return false
}

func NewRuleFilter(includeTableRegex []string) *RuleFilter {
	// init table filter
	ruleFilter := &RuleFilter{}
	if n := len(includeTableRegex); n > 0 {
		ruleFilter.includeTableRegex = make([]*regexp.Regexp, n)
		for i, val := range includeTableRegex {
			reg, err := regexp.Compile(val)
			if err != nil {
				log.Fatal(err)
			}
			ruleFilter.includeTableRegex[i] = reg
		}
	}

	return ruleFilter
}

func (filter *RuleFilter) Filter(msg *msg.MongoMsg) bool {
	key := fmt.Sprintf("%s.%s", msg.Ns.Database, msg.Ns.Collection)
	return filter.checkTableMatch(key)
}

func (filter *RuleFilter) checkTableMatch(key string) bool {
	matchFlag := true
	// check include
	if filter.includeTableRegex != nil {
		for _, reg := range filter.includeTableRegex {
			if reg.MatchString(key) {
				matchFlag = false
				break
			}
		}
	}
	return matchFlag
}
