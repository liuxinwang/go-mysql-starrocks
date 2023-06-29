package rule

import (
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
)

type StarrocksRules struct {
	Rules      []*StarrocksRule
	RulesRegex []string
	RulesMap   map[string]interface{}
}

type StarrocksRule struct {
	SourceSchema string `toml:"source-schema" json:"source-schema" mapstructure:"source-schema"`
	SourceTable  string `toml:"source-table" json:"source-table" mapstructure:"source-table"`
	TargetSchema string `toml:"target-schema" json:"target-schema" mapstructure:"target-schema"`
	TargetTable  string `toml:"target-table" json:"target-table" mapstructure:"target-table"`
	RuleType     string `default:"init"` // init、dynamic add
}

func (srs *StarrocksRules) NewRule(config map[string]interface{}) {
	configRules := config["rule"]
	err := mapstructure.Decode(configRules, &srs.Rules)
	if err != nil {
		log.Fatal("output.config.rule config parsing failed. err: ", err.Error())
	}
	srs.RuleToRegex()
	srs.RuleToMap()
}

func (srs *StarrocksRules) RuleToRegex() {
	if len(srs.Rules) == 0 {
		log.Fatal("rule config cannot be empty")
	}
	for _, r := range srs.Rules {
		// cfg.IncludeTableRegex[0] = "test\\..*"
		srs.RulesRegex = append(srs.RulesRegex, "^"+r.SourceSchema+"\\."+r.SourceTable+"$")
	}
}

func (srs *StarrocksRules) RuleToMap() {
	if len(srs.Rules) == 0 {
		log.Fatal("rule config cannot be empty")
	}
	srs.RulesMap = make(map[string]interface{})
	for _, r := range srs.Rules {
		srs.RulesMap[r.SourceSchema+":"+r.SourceTable] = r
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
