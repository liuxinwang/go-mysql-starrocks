package rule

import "github.com/siddontang/go-log/log"

type RuleType string

const (
	TypeInit       RuleType = "init"
	TypeDynamicAdd RuleType = "dynamic add"
)

func SchemaTableToStrRegex(schema string, table string) string {
	if schema == "" || table == "" {
		log.Fatal("rule cannot be empty")
	}
	// cfg.IncludeTableRegex[0] = "test\\..*"
	return "^" + schema + "\\." + table + "$"
}

func RuleKeyFormat(schema string, table string) string {
	return schema + ":" + table
}
