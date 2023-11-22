package rule

import (
	"github.com/liuxinwang/go-mysql-starrocks/pkg/registry"
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
)

type DorisRules struct {
	Rules      []*DorisRule
	RulesRegex []string
	RulesMap   map[string]interface{}
}

type DorisRule struct {
	SourceSchema string   `toml:"source-schema" json:"source-schema" mapstructure:"source-schema"`
	SourceTable  string   `toml:"source-table" json:"source-table" mapstructure:"source-table"`
	TargetSchema string   `toml:"target-schema" json:"target-schema" mapstructure:"target-schema"`
	TargetTable  string   `toml:"target-table" json:"target-table" mapstructure:"target-table"`
	RuleType     RuleType `default:"init" json:"rule-type"` // default: init, init、dynamic add
	// for api delete rule, only logical deleted, fix output get ruleMap failed problem. when add the same rule physical deleted
	Deleted bool `default:"false" json:"deleted"`
}

const DorisRuleName = "doris"

func init() {
	registry.RegisterPlugin(registry.OutputRulePlugin, DorisRuleName, &DorisRules{})
}

func (drs *DorisRules) Configure(pipelineName string, configOutput map[string]interface{}) error {
	configRules := configOutput["rule"]
	err := mapstructure.Decode(configRules, &drs.Rules)
	if err != nil {
		log.Fatal("output.config.rule config parsing failed. err: ", err.Error())
	}
	// init
	for i := range drs.Rules {
		drs.Rules[i].RuleType = TypeInit
		drs.Rules[i].Deleted = false
	}
	drs.RuleToRegex()
	drs.RuleToMap()
	return nil
}

func (drs *DorisRules) NewRule(config map[string]interface{}) {
	configRules := config["rule"]
	err := mapstructure.Decode(configRules, &drs.Rules)
	if err != nil {
		log.Fatal("output.config.rule config parsing failed. err: ", err.Error())
	}
	// init
	for i := range drs.Rules {
		drs.Rules[i].RuleType = TypeInit
		drs.Rules[i].Deleted = false
	}
	drs.RuleToRegex()
	drs.RuleToMap()
}

func (drs *DorisRules) RuleToRegex() {
	if len(drs.Rules) == 0 {
		log.Fatal("rule config cannot be empty")
	}
	for _, r := range drs.Rules {
		// cfg.IncludeTableRegex[0] = "test\\..*"
		drs.RulesRegex = append(drs.RulesRegex, SchemaTableToStrRegex(r.SourceSchema, r.SourceTable))
	}
}

func (drs *DorisRules) RuleToMap() {
	if len(drs.Rules) == 0 {
		log.Fatal("rule config cannot be empty")
	}
	drs.RulesMap = make(map[string]interface{})
	for _, r := range drs.Rules {
		drs.RulesMap[RuleKeyFormat(r.SourceSchema, r.SourceTable)] = r
	}
}

func (drs *DorisRules) GetRuleToRegex() []string {
	return drs.RulesRegex
}

func (drs *DorisRules) GetRuleToMap() map[string]interface{} {
	return drs.RulesMap
}

func (drs *DorisRules) GetRule(schemaTable string) interface{} {
	v, ok := drs.RulesMap[schemaTable]
	if ok {
		return v
	}
	log.Fatalf("get rule failed. target rule for %v not find.", schemaTable)
	return nil
}

func (drs *DorisRules) TargetString() string {
	return ""
}
