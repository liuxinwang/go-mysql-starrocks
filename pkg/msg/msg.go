package msg

import (
	"github.com/go-mysql-org/go-mysql/schema"
)

type Msg struct {
	Table         *schema.Table
	Action        string
	IgnoreColumns []string
	Data          map[string]interface{}
	Old           map[string]interface{}
}
