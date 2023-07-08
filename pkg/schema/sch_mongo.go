package schema

import (
	"context"
	"fmt"
	"github.com/juju/errors"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
	"sync"
	"time"
)

type MongoTables struct {
	*config.MongoConfig
	tablesLock sync.RWMutex
	tables     map[string]*Table
	connLock   sync.Mutex
	conn       *mongo.Client
	ctx        context.Context
	cancel     context.CancelFunc
}

func (mts *MongoTables) NewSchemaTables(conf *config.BaseConfig, pluginConfig interface{}) {
	mts.tables = make(map[string]*Table)
	mts.MongoConfig = &config.MongoConfig{}
	err := mapstructure.Decode(pluginConfig, mts.MongoConfig)
	if err != nil {
		log.Fatal("new schema tables config parsing failed. err: %v", err.Error())
	}
	// init conn
	uri := fmt.Sprintf("mongodb://%s:%s@%s", mts.UserName, mts.Password, mts.Uri)
	mts.ctx, mts.cancel = context.WithTimeout(context.Background(), 10*time.Second)
	mts.conn, err = mongo.Connect(mts.ctx, options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatal("new schema tables conn failed. err: ", err.Error())
	}
	// TODO LoadMeta
}

func (mts *MongoTables) AddTableForMsg(msg *msg.Msg) error {
	key := fmt.Sprintf("%s.%s", msg.Database, msg.Table)
	mts.tablesLock.RLock()
	t, ok := mts.tables[key]
	mts.tablesLock.RUnlock()

	if ok {
		ta := &Table{
			Schema:  msg.Database,
			Name:    msg.Table,
			Columns: make([]TableColumn, 0, 16),
		}
		mts.getColumns(ta, msg.DmlMsg.Data)
		if reflect.DeepEqual(t.Columns, ta.Columns) {
			return nil
		}
		mts.unionColumns(t, ta)
		return nil
	}

	// add coll to colls
	ta := &Table{
		Schema:  msg.Database,
		Name:    msg.Table,
		Columns: make([]TableColumn, 0, 16),
	}
	mts.getColumns(ta, msg.DmlMsg.Data)
	mts.tablesLock.RLock()
	mts.tables[key] = ta
	mts.tablesLock.RUnlock()
	return nil
}

func (mts *MongoTables) AddTable(db string, table string) (*Table, error) {
	return nil, nil
}

func (mts *MongoTables) UpdateTable(db string, table string, args interface{}) (err error) {
	return nil
}

func (mts *MongoTables) GetTable(db string, table string) (*Table, error) {
	key := fmt.Sprintf("%s.%s", db, table)
	mts.tablesLock.RLock()
	t, ok := mts.tables[key]
	mts.tablesLock.RUnlock()
	if ok {
		return t, nil
	}
	return nil, errors.New("get table meta missing")
}

func (mts *MongoTables) RefreshTable(db string, table string) {
	//TODO
}

func (mts *MongoTables) Close() {
	mts.cancel()
	err := mts.conn.Disconnect(context.TODO())
	if err != nil {
		log.Fatalf("schema tables close conn failed: %v", err.Error())
	}
	log.Infof("schema tables conn is closed")
}

func (mts *MongoTables) getColumns(table *Table, data map[string]interface{}) {
	for k := range data {
		table.Columns = append(table.Columns, TableColumn{Name: k})
	}
}

func (mts *MongoTables) unionColumns(cacheTable *Table, msgTable *Table) {
	columnsMap := make(map[string]bool)
	for _, c := range cacheTable.Columns {
		columnsMap[c.Name] = true
	}
	for _, c := range msgTable.Columns {
		if _, ok := columnsMap[c.Name]; !ok {
			columnsMap[c.Name] = true
			cacheTable.Columns = append(cacheTable.Columns, c)
		}
	}
}
