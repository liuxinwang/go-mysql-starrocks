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

type StarrocksTables struct {
	*config.StarrocksConfig
	tablesLock sync.RWMutex
	tables     map[string]*Table
	connLock   sync.Mutex
	conn       *client.Conn
}

func (sts *StarrocksTables) NewSchemaTables(conf interface{}) {
	sts.tables = make(map[string]*Table)
	sts.StarrocksConfig = &config.StarrocksConfig{}
	err := mapstructure.Decode(conf, sts.StarrocksConfig)
	if err != nil {
		log.Fatalf("new schema tables config parsing failed. err: %v", err.Error())
	}
	// init conn
	sts.conn, err = client.Connect(fmt.Sprintf("%s:%d", sts.Host, sts.Port), sts.UserName, sts.Password, "")
	if err != nil {
		log.Fatalf("new schema tables conn failed. err: ", err.Error())
	}
}

func (sts *StarrocksTables) AddTableForMsg(msg *msg.Msg) error {
	_, err := sts.GetTable(msg.Database, msg.Table)
	if err != nil {
		return err
	}
	return nil
}

func (sts *StarrocksTables) AddTable(db string, table string) (*Table, error) {
	key := fmt.Sprintf("%s.%s", db, table)
	r, err := sts.ExecuteSQL(fmt.Sprintf("show full columns from `%s`.`%s`", db, table))
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
	sts.tablesLock.Lock()
	sts.tables[key] = ta
	sts.tablesLock.Unlock()
	return ta, nil
}

func (sts *StarrocksTables) GetTable(db string, table string) (*Table, error) {
	key := fmt.Sprintf("%s.%s", db, table)
	sts.tablesLock.RLock()
	t, ok := sts.tables[key]
	sts.tablesLock.RUnlock()
	if ok {
		return t, nil
	}
	ta, err := sts.AddTable(db, table)
	if err != nil {
		return nil, err
	}
	return ta, nil
}

func (sts *StarrocksTables) RefreshTable(db string, table string) {

}

func (sts *StarrocksTables) Close() {
	if sts.conn != nil {
		err := sts.conn.Close()
		if err != nil {
			log.Fatalf("schema tables close conn failed: %v", err.Error())
		}
		log.Infof("schema tables conn is closed.")
	}
}

func (sts *StarrocksTables) ExecuteSQL(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
	sts.connLock.Lock()
	defer sts.connLock.Unlock()
	argF := make([]func(*client.Conn), 0)
	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if sts.conn == nil {
			sts.conn, err = client.Connect(fmt.Sprintf("%s:%d", sts.Host, sts.Port), sts.UserName, sts.Password, "", argF...)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		rr, err = sts.conn.Execute(cmd, args...)
		if err != nil && !mysql.ErrorEqual(err, mysql.ErrBadConn) {
			return
		} else if mysql.ErrorEqual(err, mysql.ErrBadConn) {
			err := sts.conn.Close()
			if err != nil {
				return nil, err
			}
			sts.conn = nil
			continue
		} else {
			return
		}
	}
	return
}
