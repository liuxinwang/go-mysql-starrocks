package output

import (
	"github.com/liuxinwang/go-mysql-starrocks/pkg/channel"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
)

type PluginOutput interface {
	NewOutput(config interface{})
	StartOutput(outputChan *channel.OutputChannel, rulesMap map[string]interface{})
	Execute(msgs []*msg.Msg, tableObj *Table) error
	Close()
}
