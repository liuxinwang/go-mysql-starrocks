package msg

import (
	"github.com/pingcap/parser/ast"
	"time"
)

type MsgType string
type ActionType string
type PluginName string

const (
	MsgDML MsgType = "dml"
	MsgDDL MsgType = "ddl"
	MsgCtl MsgType = "ctl" // control operate

	InsertAction  ActionType = "insert"
	UpdateAction  ActionType = "update"
	DeleteAction  ActionType = "delete"
	ReplaceAction ActionType = "replace"

	MysqlPlugin PluginName = "Mysql"
	MongoPlugin PluginName = "Mongo"
)

type Msg struct {
	Database            string
	Table               string
	Type                MsgType
	DmlMsg              *DMLMsg
	DdlMsg              *DDLMsg
	PluginName          PluginName
	ResumeToken         *WatchId `bson:"_id"`
	Timestamp           time.Time
	InputContext        interface{}
	AfterCommitCallback MsgCallbackFunc
}

type DMLMsg struct {
	Action ActionType
	Data   map[string]interface{}
	Old    map[string]interface{}
}

type DDLMsg struct {
	Statement string
	AST       ast.StmtNode
}

type WatchId struct {
	Data string `bson:"_data"`
}

type MsgCallbackFunc func(m *Msg) error
