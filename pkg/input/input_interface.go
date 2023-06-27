package input

import (
	"github.com/liuxinwang/go-mysql-starrocks/pkg/channel"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/position"
)

type PluginInput interface {
	NewInput(config interface{}, ruleRegex []string)
	StartInput(pos position.Position, syncChan *channel.SyncChannel) position.Position
	Close()
}
