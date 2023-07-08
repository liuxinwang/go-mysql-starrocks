package schema

import (
	"bytes"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/juju/errors"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/mitchellh/mapstructure"
	"github.com/pingcap/parser/ast"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go/ioutil2"
	"strings"
	"sync"
)

type MysqlTablesMeta struct {
	tables map[string]*Table
}

type MysqlTables struct {
	sync.RWMutex
	*config.MysqlConfig
	tablesLock sync.RWMutex
	*MysqlTablesMeta
	connLock sync.Mutex
	conn     *client.Conn
	FilePath string
}

func (mts *MysqlTables) NewSchemaTables(conf *config.BaseConfig, pluginConfig interface{}) {
	mts.MysqlTablesMeta = &MysqlTablesMeta{tables: make(map[string]*Table)}
	mts.MysqlConfig = &config.MysqlConfig{}
	err := mapstructure.Decode(pluginConfig, mts.MysqlConfig)
	if err != nil {
		log.Fatalf("new schema tables config parsing failed. err: %v", err.Error())
	}
	// init conn
	mts.conn, err = client.Connect(fmt.Sprintf("%s:%d", mts.Host, mts.Port), mts.UserName, mts.Password, "")
	if err != nil {
		log.Fatalf("new schema tables conn failed. err: %v", err.Error())
	}
	mts.LoadMetaFromLocal(conf)
	if len(mts.MysqlTablesMeta.tables) == 0 {
		mts.LoadMetaFromDB()
		if err = mts.SaveMeta(); err != nil {
			log.Fatalf("save tables meta failed. err: %v", err.Error())
		}
	}
}

func (mts *MysqlTables) AddTableForMsg(msg *msg.Msg) error {
	_, err := mts.GetTable(msg.Database, msg.Table)
	if err != nil {
		return err
	}
	return nil
}

func (mts *MysqlTables) AddTable(db string, table string) (*Table, error) {
	key := fmt.Sprintf("%s.%s", db, table)
	r, err := mts.ExecuteSQL(fmt.Sprintf("show full columns from `%s`.`%s`", db, table))
	if err != nil {
		return nil, err
	}
	ta := &Table{
		Schema:  db,
		Name:    table,
		Columns: make([]TableColumn, 0, 16),
	}
	for i := 0; i < r.RowNumber(); i++ {
		name, _ := r.GetString(i, 0)
		rawType, _ := r.GetString(i, 1)

		var column = TableColumn{Name: name, RawType: rawType}
		column.Type = mts.GetColumnTypeFromRawType(rawType)

		ta.Columns = append(ta.Columns, column)
	}
	mts.tablesLock.Lock()
	mts.tables[key] = ta
	mts.tablesLock.Unlock()
	return ta, nil
}

func (mts *MysqlTables) UpdateTable(db string, table string, spec interface{}) (err error) {
	key := fmt.Sprintf("%s.%s", db, table)
	mts.tablesLock.RLock()
	t := mts.tables[key]
	mts.tablesLock.RUnlock()
	alterSpec := &ast.AlterTableSpec{}
	err = mapstructure.Decode(spec, alterSpec)
	if err != nil {
		return err
	}
	switch alterSpec.Tp {
	case ast.AlterTableAddColumns:
		for _, newColumn := range alterSpec.NewColumns {
			// fix stop main column is added done
			if t.FindColumn(newColumn.Name.String()) > -1 {
				continue
			}
			switch alterSpec.Position.Tp {
			case ast.ColumnPositionNone:
				rawType := newColumn.Tp.String()
				columnType := mts.GetColumnTypeFromRawType(rawType)
				tableColumn := &TableColumn{Name: newColumn.Name.String(), Type: columnType, RawType: rawType}
				mts.tablesLock.Lock()
				t.Columns = append(t.Columns, *tableColumn)
				mts.tablesLock.Unlock()
			case ast.ColumnPositionFirst:
				// TODO
			case ast.ColumnPositionAfter:
				afterColumnName := alterSpec.Position.RelativeColumn.Name.String()
				for i, oldColumn := range t.Columns {
					if oldColumn.Name == afterColumnName {
						rawType := newColumn.Tp.String()
						columnType := mts.GetColumnTypeFromRawType(rawType)
						newTableColumn := TableColumn{Name: newColumn.Name.String(), Type: columnType, RawType: rawType}
						mts.tablesLock.Lock()
						t.Columns = append(t.Columns[:i+1], append([]TableColumn{newTableColumn}, t.Columns[i+1:]...)...)
						mts.tablesLock.Unlock()
						break
					}
				}

			}
		}
		err = mts.SaveMeta()
		if err != nil {
			return err
		}
	}
	return nil
}

func (mts *MysqlTables) GetTable(db string, table string) (*Table, error) {
	key := fmt.Sprintf("%s.%s", db, table)
	mts.tablesLock.RLock()
	t, ok := mts.tables[key]
	mts.tablesLock.RUnlock()
	if ok {
		return t, nil
	}
	ta, err := mts.AddTable(db, table)
	if err != nil {
		return nil, err
	}
	return ta, nil
}

func (mts *MysqlTables) RefreshTable(db string, table string) {

}

func (mts *MysqlTables) Close() {
	if mts.conn != nil {
		err := mts.conn.Close()
		if err != nil {
			log.Fatalf("schema tables close conn failed: %v", err.Error())
		}
		log.Infof("schema tables conn is closed.")
	}
}

func (mts *MysqlTables) ExecuteSQL(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
	mts.connLock.Lock()
	defer mts.connLock.Unlock()
	argF := make([]func(*client.Conn), 0)
	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if mts.conn == nil {
			mts.conn, err = client.Connect(fmt.Sprintf("%s:%d", mts.Host, mts.Port), mts.UserName, mts.Password, "", argF...)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		rr, err = mts.conn.Execute(cmd, args...)
		if err != nil && !mysql.ErrorEqual(err, mysql.ErrBadConn) {
			return
		} else if mysql.ErrorEqual(err, mysql.ErrBadConn) {
			err := mts.conn.Close()
			if err != nil {
				return nil, err
			}
			mts.conn = nil
			continue
		} else {
			return
		}
	}
	return
}

func (mts *MysqlTables) LoadMetaFromLocal(conf *config.BaseConfig) {
	// load meta from local
	metaFilePath := GetMetaFilePath(conf)
	mts.FilePath = metaFilePath
	log.Debugf("start load tables meta from local: %s", metaFilePath)
	initMetaData := ""
	FindMetaFileNotCreate(metaFilePath, initMetaData)
	mysqlTablesMeta := &MysqlTablesMeta{tables: make(map[string]*Table)}
	if _, err := toml.DecodeFile(metaFilePath, &mysqlTablesMeta.tables); err != nil {
		log.Fatal(err)
	}
	if len(mysqlTablesMeta.tables) == 0 {
		log.Warnf("local tables meta is empty")
		return
	}
	mts.MysqlTablesMeta = mysqlTablesMeta
	log.Debugf("end load tables meta from local")
}

func (mts *MysqlTables) LoadMetaFromDB() {
	// load meta from db
	log.Debugf("start load tables meta from db, waiting...")
	// get schemas
	schemaSql := "select schema_name FROM information_schema.schemata " +
		"where schema_name not in ('mysql', 'sys', 'information_schema', 'performance_schema')"
	r, err := mts.ExecuteSQL(schemaSql)
	if err != nil {
		log.Fatalf("%v", err)
	}
	for i := 0; i < r.RowNumber(); i++ {
		schemaName, _ := r.GetString(i, 0)
		// get schema tables
		tableSql := fmt.Sprintf("select table_name from information_schema.tables "+
			"where table_schema = '%s' and table_type = 'BASE TABLE'", schemaName)
		rr, err := mts.ExecuteSQL(tableSql)
		if err != nil {
			log.Fatalf("%v", err)
		}
		for j := 0; j < rr.RowNumber(); j++ {
			tableName, _ := rr.GetString(j, 0)
			// add schema table
			_, err := mts.AddTable(schemaName, tableName)
			if err != nil {
				log.Fatalf("%v", err)
			}
		}
	}
	log.Debugf("end load tables meta from db: %v", mts.tables)
}

func (mts *MysqlTables) SaveMeta() error {
	// persistence now meta
	mts.Lock()
	defer mts.Unlock()

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	// jsonData, _ := json.Marshal(mts.tables)
	if err := e.Encode(mts.tables); err != nil {
		return err
	}
	var err error
	if err = ioutil2.WriteFileAtomic(mts.FilePath, buf.Bytes(), 0644); err != nil {
		log.Errorf("save tables meta to file %s err %v", mts.FilePath, err)
	}
	log.Debugf("flush tables meta to file: %s", mts.tables)
	return errors.Trace(err)
}

// ClearTableCache clear table cache
func (mts *MysqlTables) ClearTableCache(db []byte, table []byte) {
	key := fmt.Sprintf("%s.%s", db, table)
	mts.tablesLock.Lock()
	delete(mts.tables, key)
	mts.tablesLock.Unlock()
}

func (mts *MysqlTables) GetColumnTypeFromRawType(rawType string) int {
	var columnType int
	if strings.HasPrefix(rawType, "float") ||
		strings.HasPrefix(rawType, "double") {
		columnType = TypeFloat
	} else if strings.HasPrefix(rawType, "decimal") {
		columnType = TypeDecimal
	} else if strings.HasPrefix(rawType, "enum") {
		columnType = TypeEnum
	} else if strings.HasPrefix(rawType, "set") {
		columnType = TypeSet
	} else if strings.HasPrefix(rawType, "datetime") {
		columnType = TypeDatetime
	} else if strings.HasPrefix(rawType, "timestamp") {
		columnType = TypeTimestamp
	} else if strings.HasPrefix(rawType, "time") {
		columnType = TypeTime
	} else if "date" == rawType {
		columnType = TypeDate
	} else if strings.HasPrefix(rawType, "bit") {
		columnType = TypeBit
	} else if strings.HasPrefix(rawType, "json") {
		columnType = TypeJson
	} else if strings.Contains(rawType, "mediumint") {
		columnType = TypeMediumInt
	} else if strings.Contains(rawType, "int") || strings.HasPrefix(rawType, "year") {
		columnType = TypeNumber
	} else {
		columnType = TypeString
	}
	return columnType
}
