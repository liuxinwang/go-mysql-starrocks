package msg

import (
	"github.com/go-mysql-org/go-mysql/canal"
)

type Msg struct {
	*canal.RowsEvent
	IgnoreColumns []string
	Data          map[string]interface{}
	Old           map[string]interface{}
}
