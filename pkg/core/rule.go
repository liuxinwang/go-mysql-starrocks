package core

type Rule interface {
	NewRule(map[string]interface{})
	GetRuleToRegex() []string
	GetRuleToMap() map[string]interface{}
	GetRule(key string) interface{}
	TargetString() string
}
