package schema

import (
	"encoding/json"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
)

type ColumnType = int

const (
	TypeNumber    ColumnType = iota + 1 // tinyint, smallint, int, bigint, year
	TypeMediumInt                       // medium int
	TypeFloat                           // float, double
	TypeEnum                            // enum
	TypeSet                             // set
	TypeString                          // other
	TypeDatetime                        // datetime
	TypeTimestamp                       // timestamp
	TypeDate                            // date
	TypeTime                            // time
	TypeBit                             // bit
	TypeJson                            // json
	TypeDecimal                         // decimal
	TypeBinary                          // binary
)

type Table struct {
	Schema            string        `toml:"schema" json:"schema"`
	Name              string        `toml:"name" json:"name"`
	Comment           string        `toml:"comment" json:"comment"`
	Columns           []TableColumn `toml:"columns" json:"columns"`
	PrimaryKeyColumns []TableColumn `toml:"primary_key_columns" json:"primary_key_columns"`
}

type TableColumn struct {
	Name         string     `toml:"name" json:"name"`
	Type         ColumnType `toml:"type" json:"type"`
	RawType      string     `toml:"raw_type" json:"raw_type"`
	Comment      string     `toml:"comment" json:"comment"`
	IsPrimaryKey bool       `toml:"is_primary_key" json:"is_primary_key"`
}

type DdlStatement struct {
	Schema            string `toml:"schema" json:"schema"`
	Name              string `toml:"name" json:"name"`
	RawSql            string `toml:"raw_sql" json:"raw_sql"`
	IsCreateTable     bool   `toml:"is_create_table" json:"is_create_table"`
	IsLikeCreateTable bool   `toml:"is_like_create_table" json:"is_like_create_table"`
	ReferTable        struct {
		Schema string `toml:"schema" json:"schema"`
		Name   string `toml:"name" json:"name"`
	} `toml:"refer_table" json:"refer_table"`
	IsSelectCreateTable bool   `toml:"is_select_create_table" json:"is_select_create_table"`
	SelectRawSql        string `toml:"select_raw_sql" json:"select_raw_sql"`
	IsDropTable         bool   `toml:"is_drop_table" json:"is_drop_table"`
	IsRenameTable       bool   `toml:"is_rename_table" json:"is_rename_table"`
}

func (t *Table) GetTableColumnsName() []string {
	columns := make([]string, 0, 16)
	for _, column := range t.Columns {
		columns = append(columns, column.Name)
	}
	return columns
}

func (t *Table) FindColumn(name string) int {
	for i, col := range t.Columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}

func (t *Table) DelColumn(name string) error {
	colIndex := t.FindColumn(name)
	if colIndex > -1 {
		t.Columns = append(t.Columns[:colIndex], t.Columns[colIndex+1:]...)
		log.Infof("table: %s.%s delete column: %s", t.Schema, t.Name, name)
		return nil
	}
	return errors.New("column: %s not found")
}

func (t *Table) ToString() string {
	if t == nil {
		return ""
	}
	marshal, err := json.Marshal(t)
	if err != nil {
		return ""
	}
	return string(marshal)
}
