package filter

import (
	"fmt"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/utils"
	"github.com/pingcap/errors"
)

const RenameDmlColumnFilterName = "rename-dml-column"

type RenameDmlColumnFilter struct {
	name        string
	matchSchema string
	matchTable  string
	columns     []string
	renameAs    []string
}

func (rdcf *RenameDmlColumnFilter) NewFilter(config map[string]interface{}) error {
	columns, ok := config["columns"]
	if !ok {
		return errors.Trace(errors.New("'columns' is not configured"))
	}
	renameAs, ok := config["rename-as"]
	if !ok {
		return errors.Trace(errors.New("'rename-as' is not configured"))
	}

	c, ok := utils.CastToSlice(columns)
	if !ok {
		return errors.Trace(errors.New("'columns' should be an array"))
	}

	columnsString, err := utils.CastSliceInterfaceToSliceString(c)
	if err != nil {
		return errors.Trace(errors.New("'columns' should be an array of string"))
	}

	ra, ok := utils.CastToSlice(renameAs)
	if !ok {
		return errors.Trace(errors.New("'rename-as' should be an array"))
	}

	renameAsString, err := utils.CastSliceInterfaceToSliceString(ra)
	if err != nil {
		return errors.Trace(errors.New("'cast-as' should be an array of string"))
	}

	if len(c) != len(ra) {
		return errors.Trace(errors.New("'columns' should have the same length of 'rename-as'"))
	}

	rdcf.name = RenameDmlColumnFilterName
	rdcf.matchSchema = fmt.Sprintf("%v", config["match-schema"])
	rdcf.matchTable = fmt.Sprintf("%v", config["match-table"])
	rdcf.columns = columnsString
	rdcf.renameAs = renameAsString
	return nil
}

func (rdcf *RenameDmlColumnFilter) Filter(msg *msg.Msg) bool {
	if rdcf.matchSchema == msg.Database && rdcf.matchTable == msg.Table {
		for i, column := range rdcf.columns {
			value := FindColumn(msg.DmlMsg.Data, column)
			if value != nil {
				renameAsColumn := rdcf.renameAs[i]
				msg.DmlMsg.Data[renameAsColumn] = msg.DmlMsg.Data[column]
				delete(msg.DmlMsg.Data, column)
			}
		}
	}
	return false
}
