package rule

import (
	"errors"
	"fmt"
	"github.com/siddontang/go-log/log"
	"strings"
)

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

func StrRegexToSchemaTable(regex string) (string, string) {
	if regex == "" {
		log.Fatal("regex cannot be empty")
	}
	tmpRegex := strings.ReplaceAll(regex, "^", "")
	tmpRegex = strings.ReplaceAll(tmpRegex, "$", "")
	tmpRegex = strings.ReplaceAll(tmpRegex, "\\", "")
	if strings.Index(tmpRegex, ".") == -1 {
		log.Fatalf("regex: %s delimiter '.' cannot be found.", regex)
	}
	splitRegex := strings.Split(tmpRegex, ".")
	return splitRegex[0], splitRegex[1]
}

func RuleKeyFormat(schema string, table string) string {
	return schema + ":" + table
}

func GetRuleKeySchemaTable(ruleKey string) (string, string, error) {
	if ruleKey == "" || !strings.Contains(ruleKey, ":") {
		return "", "", errors.New(fmt.Sprintf("rulekey:%s is invalid", ruleKey))
	}
	schemaTable := strings.Split(ruleKey, ":")
	return schemaTable[0], schemaTable[1], nil
}
