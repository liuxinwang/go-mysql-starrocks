package input

import (
	"github.com/go-mysql-org/go-mysql/schema"
	schema2 "github.com/liuxinwang/go-mysql-starrocks/pkg/schema"
)

type inputContext struct {
	BinlogName string `toml:"binlog-name"`
	BinlogPos  uint32 `toml:"binlog-pos"`
	BinlogGTID string `toml:"binlog-gtid"`
	force      bool
}

func deserialize(raw interface{}, column schema.TableColumn) interface{} {
	if raw == nil {
		return nil
	}

	ret := raw
	if column.RawType == "text" || column.RawType == "longtext" || column.RawType == "mediumtext" || column.RawType == "json" {
		_, ok := raw.([]uint8)
		if ok {
			ret = string(raw.([]uint8))
		}
	}
	return ret
}

func deserializeForLocal(raw interface{}, column schema2.TableColumn) interface{} {
	if raw == nil {
		return nil
	}

	ret := raw
	if column.RawType == "text" || column.RawType == "longtext" || column.RawType == "mediumtext" || column.RawType == "json" {
		_, ok := raw.([]uint8)
		if ok {
			ret = string(raw.([]uint8))
		}
	}
	return ret
}

type node struct {
	db       string
	table    string
	newDb    string
	newTable string
}
