package rule

import (
	"fmt"
	"github.com/siddontang/go-log/log"
)

type MysqlToSrRule struct {
	SourceSchema string `toml:"source-schema"`
	SourceTable  string `toml:"source-table"`
	TargetSchema string `toml:"target-schema"`
	TargetTable  string `toml:"target-table"`
}

func NewMysqlToSrRule(rules []*MysqlToSrRule) []string {
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

func (mts *MysqlToSrRule) TargetString() string {
	return fmt.Sprintf("%s.%s", mts.TargetSchema, mts.TargetTable)
}
