package rule

import (
	"github.com/liuxinwang/go-mysql-starrocks/pkg/registry"
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
)

type MysqlRules struct {
	Rules      []*MysqlRule
	RulesRegex []string
	RulesMap   map[string]interface{}
}

type MysqlRule struct {
	SourceSchema  string   `toml:"source-schema" json:"source-schema" mapstructure:"source-schema"`
	SourceTable   string   `toml:"source-table" json:"source-table" mapstructure:"source-table"`
	TargetSchema  string   `toml:"target-schema" json:"target-schema" mapstructure:"target-schema"`
	TargetTable   string   `toml:"target-table" json:"target-table" mapstructure:"target-table"`
	PrimaryKeys   []string `toml:"primary-keys" json:"primary-keys" mapstructure:"primary-keys"`
	SourceColumns []string `toml:"source-columns" json:"source-columns" mapstructure:"source-columns"`
	TargetColumns []string `toml:"target-columns" json:"target-columns" mapstructure:"target-columns"`
	RuleType      RuleType `default:"init" json:"rule-type"` // default: init, init、dynamic add
	// for api delete rule, only logical deleted, fix output get ruleMap failed problem. when add the same rule physical deleted
	Deleted bool `default:"false" json:"deleted"`
}

const MysqlRuleName = "mysql"

func init() {
	registry.RegisterPlugin(registry.OutputRulePlugin, MysqlRuleName, &MysqlRules{})
}

func (mrs *MysqlRules) Configure(pipelineName string, configOutput map[string]interface{}) error {
	configRules := configOutput["rule"]
	err := mapstructure.Decode(configRules, &mrs.Rules)
	if err != nil {
		log.Fatal("output.config.rule config parsing failed. err: ", err.Error())
	}
	// init
	for i := range mrs.Rules {
		mrs.Rules[i].RuleType = TypeInit
		mrs.Rules[i].Deleted = false
	}
	mrs.RuleToRegex()
	mrs.RuleToMap()
	return nil
}

func (mrs *MysqlRules) NewRule(config map[string]interface{}) {
	configRules := config["rule"]
	err := mapstructure.Decode(configRules, &mrs.Rules)
	if err != nil {
		log.Fatal("output.config.rule config parsing failed. err: ", err.Error())
	}
	// init
	for i := range mrs.Rules {
		mrs.Rules[i].RuleType = TypeInit
		mrs.Rules[i].Deleted = false
	}
	mrs.RuleToRegex()
	mrs.RuleToMap()
}

func (mrs *MysqlRules) RuleToRegex() {
	if len(mrs.Rules) == 0 {
		log.Fatal("rule config cannot be empty")
	}
	for _, r := range mrs.Rules {
		// cfg.IncludeTableRegex[0] = "test\\..*"
		mrs.RulesRegex = append(mrs.RulesRegex, SchemaTableToStrRegex(r.SourceSchema, r.SourceTable))
	}
}

func (mrs *MysqlRules) RuleToMap() {
	if len(mrs.Rules) == 0 {
		log.Fatal("rule config cannot be empty")
	}
	mrs.RulesMap = make(map[string]interface{})
	for _, r := range mrs.Rules {
		mrs.RulesMap[RuleKeyFormat(r.SourceSchema, r.SourceTable)] = r
	}
}

func (mrs *MysqlRules) GetRuleToRegex() []string {
	return mrs.RulesRegex
}

func (mrs *MysqlRules) GetRuleToMap() map[string]interface{} {
	return mrs.RulesMap
}

func (mrs *MysqlRules) GetRule(schemaTable string) interface{} {
	v, ok := mrs.RulesMap[schemaTable]
	if ok {
		return v
	}
	log.Fatalf("get rule failed. target rule for %v not find.", schemaTable)
	return nil
}

func (mrs *MysqlRules) TargetString() string {
	return ""
}
