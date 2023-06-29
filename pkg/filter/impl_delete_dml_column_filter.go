package filter

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/utils"
)

const DeleteDMLColumnFilterName = "delete-dml-column"

type DeleteDmlColumnFilter struct {
	name        string
	matchSchema string
	matchTable  string
	columns     []string
}

func (ddcf *DeleteDmlColumnFilter) NewFilter(config map[string]interface{}) error {
	columns := config["columns"]
	c, ok := utils.CastToSlice(columns)
	if !ok {
		return errors.Trace(errors.New("'column' should be an array"))
	}

	columnsString, err := utils.CastSliceInterfaceToSliceString(c)
	if err != nil {
		return errors.Trace(errors.New("'column' should be an array of string"))
	}
	ddcf.name = DeleteDMLColumnFilterName
	ddcf.matchSchema = fmt.Sprintf("%v", config["match-schema"])
	ddcf.matchTable = fmt.Sprintf("%v", config["match-table"])
	ddcf.columns = columnsString
	return nil
}

func (ddcf *DeleteDmlColumnFilter) Filter(msg *msg.Msg) bool {
	if ddcf.matchSchema == msg.Database && ddcf.matchTable == msg.Table {
		for _, column := range ddcf.columns {
			value := FindColumn(msg.DmlMsg.Data, column)
			if value != nil {
				delete(msg.DmlMsg.Data, column)
				// msg.IgnoreColumns = append(msg.IgnoreColumns, column)
			}
		}
	}
	return false
}
