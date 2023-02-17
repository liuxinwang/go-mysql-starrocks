package filter

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-log/log"
	"go-mysql-starrocks/pkg/msg"
	"go-mysql-starrocks/pkg/utils"
)

type BinlogFilter interface {
	Filter(msg *msg.Msg) bool
}

type BinlogFilterMatcher []BinlogFilter

type DeleteDmlColumnFilter struct {
	matchSchema string
	matchTable  string
	columns     []string
}

func (matcher BinlogFilterMatcher) IterateFilter(msg *msg.Msg) bool {
	for _, filter := range matcher {
		if filter.Filter(msg) {
			log.Debugf("filter binlog event %v", msg.Data)
			return true
		}
	}
	return false
}

func NewDeleteDmlColumnFilter(config map[string]interface{}) (*DeleteDmlColumnFilter, error) {
	columns := config["columns"]
	c, ok := utils.CastToSlice(columns)
	if !ok {
		return nil, errors.Trace(errors.New("'column' should be an array"))
	}

	columnsString, err := utils.CastSliceInterfaceToSliceString(c)
	if err != nil {
		return nil, errors.Trace(errors.New("'column' should be an array of string"))
	}
	return &DeleteDmlColumnFilter{
		matchSchema: fmt.Sprintf("%v", config["match-schema"]),
		matchTable:  fmt.Sprintf("%v", config["match-table"]),
		columns:     columnsString,
	}, nil
}

func (filter *DeleteDmlColumnFilter) Filter(msg *msg.Msg) bool {
	if filter.matchSchema == msg.Table.Schema && filter.matchTable == msg.Table.Name {
		for _, column := range filter.columns {
			colIndex := msg.Table.FindColumn(column)
			if colIndex > -1 {
				delete(msg.Data, column)
				msg.IgnoreColumns = append(msg.IgnoreColumns, column)
			}
		}
	}
	return false
}
