package rule

import (
	"github.com/siddontang/go-log/log"
)

type MongoToSrRule struct {
	SourceSchema string `toml:"source-schema"`
	SourceTable  string `toml:"source-table"`
	TargetSchema string `toml:"target-schema"`
	TargetTable  string `toml:"target-table"`
}

func NewMongoToSrRule(rules []*MongoToSrRule) []string {
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
