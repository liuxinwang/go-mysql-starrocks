package core

import (
	"github.com/liuxinwang/go-mysql-starrocks/pkg/channel"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/schema"
)

type Output interface {
	NewOutput(config interface{}, rulesMap map[string]interface{}, inSchema Schema)
	StartOutput(outputChan *channel.OutputChannel)
	Execute(msgs []*msg.Msg, tableObj *schema.Table, targetSchema string, targetTable string) error
	Close()
	AddRule(map[string]interface{}) error
	DeleteRule(map[string]interface{}) error
	GetRules() interface{}
	Pause() error
	Resume() error
	IsPaused() bool
}
