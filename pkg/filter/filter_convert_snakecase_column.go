package filter

import (
	"github.com/iancoleman/strcase"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
)

const ConvertSnakeCaseColumnFilterName = "convert-snakecase-column"

type ConvertSnakeCaseColumnFilter struct {
	name string
}

func (cdcf *ConvertSnakeCaseColumnFilter) NewFilter(config map[string]interface{}) error {
	cdcf.name = ConvertSnakeCaseColumnFilterName
	return nil
}

func (cdcf *ConvertSnakeCaseColumnFilter) Filter(m *msg.Msg) bool {
	if m.Type == msg.MsgCtl {
		return false
	}
	for v := range m.DmlMsg.Data {
		snakeName := strcase.ToSnake(v)
		if snakeName != v {
			m.DmlMsg.Data[snakeName] = m.DmlMsg.Data[v]
			delete(m.DmlMsg.Data, v)
		}
	}
	return false
}
