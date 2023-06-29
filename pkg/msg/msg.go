package msg

import "time"

type MsgType string
type ActionType string

const (
	MsgDML MsgType = "dml"
	MsgDDL MsgType = "ddl"
	MsgCtl MsgType = "ctl" // control operate

	InsertAction  ActionType = "insert"
	UpdateAction  ActionType = "update"
	DeleteAction  ActionType = "delete"
	ReplaceAction ActionType = "replace"
)

type Msg struct {
	Database            string
	Table               string
	Type                MsgType
	DmlMsg              *DMLMsg
	DdlMsg              *DDLMsg
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
}

type WatchId struct {
	Data string `bson:"_data"`
}

type MsgCallbackFunc func(m *Msg) error
