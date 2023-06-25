package msg

import (
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/schema"
)

type MysqlMsg struct {
	Table         *schema.Table
	Action        string
	IgnoreColumns []string
	Data          map[string]interface{}
	Old           map[string]interface{}
	Timestamp     uint32
}

func (mm *MysqlMsg) NewMsg() interface{} {
	return &MysqlMsg{}
}

func (mm *MysqlMsg) String() string {
	b, _ := json.Marshal(mm.Data)
	return fmt.Sprintf(`{"table": "%v", "action": "%v", "data": "%v"}`,
		mm.Table.String(), mm.Action, string(b))
}
