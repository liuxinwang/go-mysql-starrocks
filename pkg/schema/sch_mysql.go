package schema

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/juju/errors"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
	"sync"
)

type MysqlTables struct {
	*config.MysqlConfig
	tablesLock sync.RWMutex
	tables     map[string]*Table
	connLock   sync.Mutex
	conn       *client.Conn
}

func (mts *MysqlTables) NewSchemaTables(conf interface{}) {
	mts.tables = make(map[string]*Table)
	mts.MysqlConfig = &config.MysqlConfig{}
	err := mapstructure.Decode(conf, mts.MysqlConfig)
	if err != nil {
		log.Fatalf("new schema tables config parsing failed. err: %v", err.Error())
	}
	// init conn
	mts.conn, err = client.Connect(fmt.Sprintf("%s:%d", mts.Host, mts.Port), mts.UserName, mts.Password, "")
	if err != nil {
		log.Fatalf("new schema tables conn failed. err: %v", err.Error())
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
		ta.Columns = append(ta.Columns, TableColumn{Name: name})
	}
	mts.tablesLock.Lock()
	mts.tables[key] = ta
	mts.tablesLock.Unlock()
	return ta, nil
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
