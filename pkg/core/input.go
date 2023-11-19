package core

import (
	"github.com/liuxinwang/go-mysql-starrocks/pkg/channel"
	"regexp"
)

type Input interface {
	NewInput(config interface{}, ruleRegex []string, inSchema Schema)
	StartInput(pos Position, syncChan *channel.SyncChannel) Position
	StartMetrics()
	Close()
	SetIncludeTableRegex(map[string]interface{}) (*regexp.Regexp, error)    // for add rule
	RemoveIncludeTableRegex(map[string]interface{}) (*regexp.Regexp, error) // for delete rule
}
