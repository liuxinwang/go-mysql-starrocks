package core

import (
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/schema"
)

type Schema interface {
	NewSchemaTables(config *config.BaseConfig, pluginConfig map[string]interface{}, startPos string, rulesMap map[string]interface{})
	AddTableForMsg(msg *msg.Msg) error
	AddTable(db string, table string) (*schema.Table, error)
	DelTable(db string, table string) error
	UpdateTable(db string, table string, args interface{}, pos string) error
	GetTable(db string, table string) (*schema.Table, error)
	RefreshTable(db string, table string)
	SaveMeta(data string) error
	Close()
}
