package schema

import (
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
)

var MemDbHost = "localhost"
var MemDbPort = 5166 // default 5166

type Table struct {
	Schema  string        `toml:"schema" json:"schema"`
	Name    string        `toml:"name" json:"name"`
	Columns []TableColumn `toml:"columns" json:"columns"`
}

type TableColumn struct {
	Name    string     `toml:"name" json:"name"`
	Type    ColumnType `toml:"type" json:"type"`
	RawType string     `toml:"raw_type" json:"raw_type"`
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
