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

func (cdcf *ConvertDmlColumnFilter) NewFilter(config map[string]interface{}) error {
	columns, ok := config["columns"]
	if !ok {
		return errors.Trace(errors.New("'columns' is not configured"))
	}
	castAs, ok := config["cast-as"]
	if !ok {
		return errors.Trace(errors.New("'cast-as' is not configured"))
	}

	c, ok := utils.CastToSlice(columns)
	if !ok {
		return errors.Trace(errors.New("'columns' should be an array"))
	}

	columnsString, err := utils.CastSliceInterfaceToSliceString(c)
	if err != nil {
		return errors.Trace(errors.New("'columns' should be an array of string"))
	}

	ca, ok := utils.CastToSlice(castAs)
	if !ok {
		return errors.Trace(errors.New("'cast-as' should be an array"))
	}

	castAsString, err := utils.CastSliceInterfaceToSliceString(ca)
	if err != nil {
		return errors.Trace(errors.New("'cast-as' should be an array of string"))
	}

	if len(c) != len(ca) {
		return errors.Trace(errors.New("'columns' should have the same length of 'cast-as'"))
	}

	cdcf.matchSchema = fmt.Sprintf("%v", config["match-schema"])
	cdcf.matchTable = fmt.Sprintf("%v", config["match-table"])
	cdcf.columns = columnsString
	cdcf.castAs = castAsString
	return nil
}

func (cdcf *ConvertDmlColumnFilter) Filter(msg *msg.Msg) bool {
	if cdcf.matchSchema == msg.Database && cdcf.matchTable == msg.Table {
		for i, column := range cdcf.columns {
			value := FindColumn(msg.DmlMsg.Data, column)
			if value != nil {
				if value == "" {
					continue
				}

				switch cdcf.castAs[i] {
				case "json":
					var columnJson map[string]interface{}
					err := json.Unmarshal([]byte(fmt.Sprintf("%v", value)), &columnJson)
					if err != nil {
						log.Warnf("convert-dml-column filter error: %v, column '%s' value: '%v' cast as json error, row event: %v",
							err.Error(), column, value, msg.DmlMsg.Data)
					}
					msg.DmlMsg.Data[column] = columnJson
				case "arrayJson":
					var columnArrayJson []map[string]interface{}
					err := json.Unmarshal([]byte(fmt.Sprintf("%v", value)), &columnArrayJson)
					if err != nil {
						log.Warnf("convert-dml-column filter error: %v, column '%s' value: '%v' cast as json error, row event: %v",
							err.Error(), column, value, msg.DmlMsg.Data)
					}
					msg.DmlMsg.Data[column] = columnArrayJson
				}
			}
		}
	}
	return false
}
