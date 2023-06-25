package rule

import (
	"fmt"
	"github.com/siddontang/go-log/log"
)

type MysqlToDorisRule struct {
	SourceSchema string `toml:"source-schema" json:"source-schema"`
	SourceTable  string `toml:"source-table" json:"source-table"`
	TargetSchema string `toml:"target-schema" json:"target-schema"`
	TargetTable  string `toml:"target-table" json:"target-table"`
	RuleType     string `default:"init"` // init、dynamic add
}

type MysqlToDorisRules []*MysqlToDorisRule

func (rules MysqlToDorisRules) NewRule() []string {
	if len(rules) == 0 {
		log.Fatal("rule config cannot be empty")
	}
	var includeTableRegex []string
	for _, r := range rules {
		// cfg.IncludeTableRegex[0] = "test\\..*"
		includeTableRegex = append(includeTableRegex, r.SourceSchema+"\\."+r.SourceTable+"$")
	}
	return includeTableRegex
}

func (mtd *MysqlToDorisRule) TargetString() string {
	return fmt.Sprintf("%s.%s", mtd.TargetSchema, mtd.TargetTable)
}
