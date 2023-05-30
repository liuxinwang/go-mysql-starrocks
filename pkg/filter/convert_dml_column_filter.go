package filter

import (
	"encoding/json"
	"fmt"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/utils"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-log/log"
)

type ConvertDmlColumnFilter struct {
	matchSchema string
	matchTable  string
	columns     []string
	castAs      []string
}

func NewConvertDmlColumnFilter(config map[string]interface{}) (*ConvertDmlColumnFilter, error) {
	columns, ok := config["columns"]
	if !ok {
		return nil, errors.Trace(errors.New("'columns' is not configured"))
	}
	castAs, ok := config["cast-as"]
	if !ok {
		return nil, errors.Trace(errors.New("'cast-as' is not configured"))
	}

	c, ok := utils.CastToSlice(columns)
	if !ok {
		return nil, errors.Trace(errors.New("'columns' should be an array"))
	}

	columnsString, err := utils.CastSliceInterfaceToSliceString(c)
	if err != nil {
		return nil, errors.Trace(errors.New("'columns' should be an array of string"))
	}

	ca, ok := utils.CastToSlice(castAs)
	if !ok {
		return nil, errors.Trace(errors.New("'cast-as' should be an array"))
	}

	castAsString, err := utils.CastSliceInterfaceToSliceString(ca)
	if err != nil {
		return nil, errors.Trace(errors.New("'cast-as' should be an array of string"))
	}

	if len(c) != len(ca) {
		return nil, errors.Trace(errors.New("'columns' should have the same length of 'cast-as'"))
	}

	return &ConvertDmlColumnFilter{
		matchSchema: fmt.Sprintf("%v", config["match-schema"]),
		matchTable:  fmt.Sprintf("%v", config["match-table"]),
		columns:     columnsString,
		castAs:      castAsString,
	}, nil
}

func (filter *ConvertDmlColumnFilter) Filter(msg *msg.Msg) bool {
	if filter.matchSchema == msg.Table.Schema && filter.matchTable == msg.Table.Name {
		for i, column := range filter.columns {
			colIndex := msg.Table.FindColumn(column)
			if colIndex > -1 {
				columnValue := msg.Data[column]
				if columnValue == "" || columnValue == nil {
					continue
				}

				switch filter.castAs[i] {
				case "json":
					var columnJson map[string]interface{}
					err := json.Unmarshal([]byte(fmt.Sprintf("%v", columnValue)), &columnJson)
					if err != nil {
						log.Warnf("convert-dml-column filter error: %v, column '%s' value: '%v' cast as json error, row event: %v",
							err.Error(), column, columnValue, msg.String())
					}
					msg.Data[column] = columnJson
				case "arrayJson":
					var columnArrayJson []map[string]interface{}
					err := json.Unmarshal([]byte(fmt.Sprintf("%v", columnValue)), &columnArrayJson)
					if err != nil {
						log.Warnf("convert-dml-column filter error: %v, column '%s' value: '%v' cast as json error, row event: %v",
							err.Error(), column, columnValue, msg.String())
					}
					msg.Data[column] = columnArrayJson
				}
			}
		}
	}
	return false
}
