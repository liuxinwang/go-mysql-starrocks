package rule

import (
	"github.com/liuxinwang/go-mysql-starrocks/pkg/registry"
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
)

type StarrocksRules struct {
	Rules      []*StarrocksRule
	RulesRegex []string
	RulesMap   map[string]interface{}
}

type StarrocksRule struct {
	SourceSchema string   `toml:"source-schema" json:"source-schema" mapstructure:"source-schema"`
	SourceTable  string   `toml:"source-table" json:"source-table" mapstructure:"source-table"`
	TargetSchema string   `toml:"target-schema" json:"target-schema" mapstructure:"target-schema"`
	TargetTable  string   `toml:"target-table" json:"target-table" mapstructure:"target-table"`
	RuleType     RuleType `default:"init" json:"rule-type"` // init、dynamic add
	// for api delete rule, only logical deleted, fix output get ruleMap failed problem. when add the same rule physical deleted
	Deleted bool `default:"false" json:"deleted"`
}

const StarrocksRuleName = "starrocks"

func init() {
	registry.RegisterPlugin(registry.OutputRulePlugin, StarrocksRuleName, &StarrocksRules{})
}

func (srs *StarrocksRules) Configure(pipelineName string, configOutput map[string]interface{}) error {
	configRules := configOutput["rule"]
	err := mapstructure.Decode(configRules, &srs.Rules)
	if err != nil {
		log.Fatal("output.config.rule config parsing failed. err: ", err.Error())
	}
	// init
	for i := range srs.Rules {
		srs.Rules[i].RuleType = TypeInit
		srs.Rules[i].Deleted = false
	}
	srs.RuleToRegex()
	srs.RuleToMap()
	return nil
}

func (srs *StarrocksRules) NewRule(config map[string]interface{}) {
	configRules := config["rule"]
	err := mapstructure.Decode(configRules, &srs.Rules)
	if err != nil {
		log.Fatal("output.config.rule config parsing failed. err: ", err.Error())
	}
	// init
	for i := range srs.Rules {
		srs.Rules[i].RuleType = TypeInit
		srs.Rules[i].Deleted = false
	}
	srs.RuleToRegex()
	srs.RuleToMap()
}

func (srs *StarrocksRules) RuleToRegex() {
	if len(srs.Rules) == 0 {
		log.Fatal("rule config cannot be empty")
	}
	for _, r := range srs.Rules {
		srs.RulesRegex = append(srs.RulesRegex, SchemaTableToStrRegex(r.SourceSchema, r.SourceTable))
	}
}

func (srs *StarrocksRules) RuleToMap() {
	if len(srs.Rules) == 0 {
		log.Fatal("rule config cannot be empty")
	}
	srs.RulesMap = make(map[string]interface{})
	for _, r := range srs.Rules {
		srs.RulesMap[RuleKeyFormat(r.SourceSchema, r.SourceTable)] = r
	}
}

func (srs *StarrocksRules) GetRuleToRegex() []string {
	return srs.RulesRegex
}

func (srs *StarrocksRules) GetRuleToMap() map[string]interface{} {
	return srs.RulesMap
}

func (srs *StarrocksRules) GetRule(schemaTable string) interface{} {
	v, ok := srs.RulesMap[schemaTable]
	if ok {
		return v
	}
	log.Fatalf("get rule failed. target rule for %v not find.", schemaTable)
	return nil
}

func (srs *StarrocksRules) TargetString() string {
	return ""
}
