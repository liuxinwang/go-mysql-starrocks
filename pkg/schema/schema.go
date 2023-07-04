package schema

import "github.com/liuxinwang/go-mysql-starrocks/pkg/msg"

type Schema interface {
	NewSchemaTables(config interface{})
	AddTableForMsg(msg *msg.Msg) error
	AddTable(db string, table string) (*Table, error)
	GetTable(db string, table string) (*Table, error)
	RefreshTable(db string, table string)
	Close()
}

type Table struct {
	Schema  string
	Name    string
	Columns []TableColumn
}

type TableColumn struct {
	Name string
}
