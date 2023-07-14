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

type DorisTables struct {
	*config.DorisConfig
	tablesLock sync.RWMutex
	tables     map[string]*Table
	connLock   sync.Mutex
	conn       *client.Conn
}

func (dts *DorisTables) NewSchemaTables(conf *config.BaseConfig, pluginConfig interface{}) {
	dts.tables = make(map[string]*Table)
	dts.DorisConfig = &config.DorisConfig{}
	err := mapstructure.Decode(pluginConfig, dts.DorisConfig)
	if err != nil {
		log.Fatalf("new schema tables config parsing failed. err: %v", err.Error())
	}
	// init conn
	dts.conn, err = client.Connect(fmt.Sprintf("%s:%d", dts.Host, dts.Port), dts.UserName, dts.Password, "")
	if err != nil {
		log.Fatalf("new schema tables conn failed. err: ", err.Error())
	}
	// TODO LoadMeta
}

func (dts *DorisTables) AddTableForMsg(msg *msg.Msg) error {
	_, err := dts.GetTable(msg.Database, msg.Table)
	if err != nil {
		return err
	}
	return nil
}

func (dts *DorisTables) AddTable(db string, table string) (*Table, error) {
	key := fmt.Sprintf("%s.%s", db, table)
	r, err := dts.ExecuteSQL(fmt.Sprintf("show full columns from `%s`.`%s`", db, table))
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
	dts.tablesLock.Lock()
	dts.tables[key] = ta
	dts.tablesLock.Unlock()
	return ta, nil
}

func (dts *DorisTables) UpdateTable(db string, table string, args interface{}) (err error) {
	return nil
}

func (dts *DorisTables) GetTable(db string, table string) (*Table, error) {
	key := fmt.Sprintf("%s.%s", db, table)
	dts.tablesLock.RLock()
	t, ok := dts.tables[key]
	dts.tablesLock.RUnlock()
	if ok {
		return t, nil
	}
	ta, err := dts.AddTable(db, table)
	if err != nil {
		return nil, err
	}
	return ta, nil
}

func (dts *DorisTables) RefreshTable(db string, table string) {

}

func (dts *DorisTables) Close() {
	if dts.conn != nil {
		err := dts.conn.Close()
		if err != nil {
			log.Fatalf("schema tables close conn failed: %v", err.Error())
		}
		log.Infof("schema tables conn is closed")
	}
}

func (dts *DorisTables) ExecuteSQL(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
	dts.connLock.Lock()
	defer dts.connLock.Unlock()
	argF := make([]func(*client.Conn), 0)
	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if dts.conn == nil {
			dts.conn, err = client.Connect(fmt.Sprintf("%s:%d", dts.Host, dts.Port), dts.UserName, dts.Password, "", argF...)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		rr, err = dts.conn.Execute(cmd, args...)
		if err != nil && !mysql.ErrorEqual(err, mysql.ErrBadConn) {
			return
		} else if mysql.ErrorEqual(err, mysql.ErrBadConn) {
			err := dts.conn.Close()
			if err != nil {
				return nil, err
			}
			dts.conn = nil
			continue
		} else {
			return
		}
	}
	return
}

func (dts *DorisTables) SaveMeta(data string) error {
	return nil
}
