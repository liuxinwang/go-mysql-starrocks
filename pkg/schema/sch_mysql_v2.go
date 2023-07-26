package schema

import (
	"context"
	"encoding/json"
	"fmt"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	memServer "github.com/dolthub/go-mysql-server/server"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/juju/errors"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/position"
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
	"strings"
	"sync"
	"time"
)

type MysqlTablesMetaV2 struct {
	tables map[string]*Table
}

type MysqlTablesV2 struct {
	sync.RWMutex
	*config.MysqlConfig
	tablesLock sync.RWMutex
	*MysqlTablesMetaV2
	posId       int
	connLock    sync.Mutex
	conn        *client.Conn
	memConnLock sync.Mutex
	memConn     *client.Conn
	FilePath    string
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

func (mts *MysqlTablesV2) NewSchemaTables(conf *config.BaseConfig, pluginConfig interface{}) {
	mts.MysqlTablesMetaV2 = &MysqlTablesMetaV2{tables: make(map[string]*Table)}
	mts.MysqlConfig = &config.MysqlConfig{}
	err := mapstructure.Decode(pluginConfig, mts.MysqlConfig)
	if err != nil {
		log.Fatalf("new schema tables config parsing failed. err: %v", err.Error())
	}
	mts.ctx, mts.cancel = context.WithCancel(context.Background())
	// init conn
	mts.conn, err = client.Connect(fmt.Sprintf("%s:%d", mts.Host, mts.Port), mts.UserName, mts.Password, "")
	if err != nil {
		log.Fatalf("new schema tables conn failed. err: %v", err.Error())
	}
	_ = mts.conn.SetCharset("utf8")

	// init vm memory mysql server for schema meta
	go mts.newMemMyServer()

	time.Sleep(time.Second * 3)
	// init vm memory mysql server conn
	mts.memConn, err = client.Connect(fmt.Sprintf("%s:%d", MemDbHost, MemDbPort), "", "", "")
	if err != nil {
		log.Fatalf("new schema tables conn failed. err: %v", err.Error())
	}

	// get last checkpoint data
	getLastTimeSql := fmt.Sprintf("select tc.`tables_meta`, tc.`updated_at` "+
		"from `%s`.`table_checkpoints` tc "+
		"inner join `%s`.`positions` po on tc.pos_id = po.id "+
		"where po.name = '%s' order by tc.updated_at desc limit 1", position.DbName, position.DbName, conf.Name)
	r, err := mts.ExecuteSQL(getLastTimeSql)
	if err != nil {
		log.Fatal("query last checkpoint data failed. err: ", err.Error())
	}
	// get pos_id
	getPosIdSql := fmt.Sprintf("select id from `%s`.positions where name = '%s'", position.DbName, conf.Name)
	posIdRs, err := mts.ExecuteSQL(getPosIdSql)
	if err != nil {
		log.Fatal("query last checkpoint data failed. err: ", err.Error())
	}
	posId, err := posIdRs.GetInt(0, 0)
	if err != nil {
		log.Fatalf("get pos_id failed. err: %v", err.Error())
	}
	mts.posId = int(posId)

	var tablesMeta string
	var updatedAt string
	if r.RowNumber() == 0 {
		// from mts.tables init memory mysql server
		marshal, err := json.Marshal(mts.LoadMetaFromDB())
		if err != nil {
			return
		}
		tablesMeta = string(marshal)

		// save meta
		err = mts.SaveMeta(tablesMeta)
		if err != nil {
			log.Fatalf("save tables meta failed. err: ", err.Error())
		}
	} else {
		tablesMeta, err = r.GetString(0, 0)
		if err != nil {
			log.Fatal("get last checkpoint data failed. err: ", err.Error())
		}
		updatedAt, _ = r.GetString(0, 1)
	}

	// init memory mysql data
	mts.loadTablesMetaToMemDB(tablesMeta)

	// handle increment ddl
	if updatedAt != "" {
		log.Infof("load last table meta for time: %s", updatedAt)
		incrementDdlSql := fmt.Sprintf("select db, table_ddl "+
			"from `%s`.table_increment_ddl where updated_at >= '%s'", position.DbName, updatedAt)
		idr, err := mts.ExecuteSQL(incrementDdlSql)
		if err != nil {
			log.Fatal("get increment ddl failed. err: ", err.Error())
		}
		for i := 0; i < idr.RowNumber(); i++ {
			db, _ := idr.GetString(i, 0)
			ddl, _ := idr.GetString(i, 1)
			err = mts.incrementDdlExec(db, "", ddl)
			if err != nil {
				log.Fatalf("handle increment ddl failed. err: %v", err.Error())
			}
		}
		log.Infof("replay increment ddl done, exec ddl events: %d", idr.RowNumber())
	}
	mts.StartTimerSaveMeta()
}

func (mts *MysqlTablesV2) newMemMyServer() {
	// ctx := sql.NewEmptyContext()
	engine := sqle.NewDefault(
		memory.NewDBProvider(
			memory.NewDatabase("test"),
		))

	conf := memServer.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", MemDbHost, MemDbPort),
	}
	log.Infof("start vm memory mysql server port: %d for schema meta", MemDbPort)
	s, err := memServer.NewDefaultServer(conf, engine)
	if err != nil {
		log.Fatal(err)
	}
	if err = s.Start(); err != nil {
		log.Fatal(err)
	}
}

func (mts *MysqlTablesV2) AddTableForMsg(msg *msg.Msg) error {
	return nil
}

func (mts *MysqlTablesV2) AddTable(db string, table string) (*Table, error) {
	return nil, nil
}

func (mts *MysqlTablesV2) GetTableCreateDDL(db string, table string) (string, error) {
	r, err := mts.ExecuteSQL(fmt.Sprintf("show create table `%s`.`%s`", db, table))
	if err != nil {
		return "", err
	}
	createDDL, err := r.GetString(0, 1)
	if err != nil {
		return "", err
	}
	return createDDL, nil
}

func (mts *MysqlTablesV2) UpdateTable(db string, table string, ddl interface{}) (err error) {
	if err = mts.memConn.UseDB(db); err != nil {
		return err
	}
	_ = mts.memConn.SetCharset("utf8")
	_, err = mts.ExecuteSQLForMemDB(fmt.Sprintf("%v", ddl))
	if err != nil {
		return err
	}
	insSql := fmt.Sprintf("insert "+
		"into `%s`.table_increment_ddl(`pos_id`, `db`, `table_ddl`)values(?, ?, ?)", position.DbName)
	_, err = mts.ExecuteSQL(insSql, mts.posId, db, fmt.Sprintf("%v", ddl))
	if err != nil {
		return err
	}
	return nil
}

func (mts *MysqlTablesV2) incrementDdlExec(db string, table string, ddl interface{}) (err error) {
	if err = mts.memConn.UseDB(db); err != nil {
		return err
	}
	_, err = mts.ExecuteSQLForMemDB(fmt.Sprintf("%v", ddl))
	if err != nil {
		return err
	}
	return nil
}

func (mts *MysqlTablesV2) GetTable(db string, table string) (*Table, error) {
	sql := fmt.Sprintf(fmt.Sprintf("show full columns from `%s`.`%s`", db, table))
	r, err := mts.ExecuteSQLForMemDB(sql)
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
	return ta, nil
}

func (mts *MysqlTablesV2) RefreshTable(db string, table string) {

}

func (mts *MysqlTablesV2) Close() {
	mts.cancel()
	mts.wg.Wait()
	log.Infof("close mysql save table meta ticker goroutine.")
	if mts.conn != nil {
		err := mts.conn.Close()
		if err != nil {
			log.Fatalf("schema tables close conn failed: %v", err.Error())
		}
		log.Infof("schema tables conn is closed.")
	}
}

func (mts *MysqlTablesV2) ExecuteSQL(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
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

func (mts *MysqlTablesV2) ExecuteSQLForMemDB(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
	mts.memConnLock.Lock()
	defer mts.memConnLock.Unlock()
	argF := make([]func(*client.Conn), 0)
	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if mts.memConn == nil {
			mts.memConn, err = client.Connect(fmt.Sprintf("%s:%d", mts.Host, mts.Port), mts.UserName, mts.Password, "", argF...)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		rr, err = mts.memConn.Execute(cmd, args...)
		if err != nil && !mysql.ErrorEqual(err, mysql.ErrBadConn) {
			return
		} else if mysql.ErrorEqual(err, mysql.ErrBadConn) {
			err := mts.memConn.Close()
			if err != nil {
				return nil, err
			}
			mts.memConn = nil
			continue
		} else {
			return
		}
	}
	return
}

func (mts *MysqlTablesV2) LoadMetaFromDB() map[string]interface{} {
	// load meta from db
	log.Debugf("start load tables meta from db, waiting...")
	createDDLMap := make(map[string]interface{})
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
			createDDL, err := mts.GetTableCreateDDL(schemaName, tableName)
			if err != nil {
				log.Fatalf("%v", err)
			}
			key := fmt.Sprintf("%s.%s", schemaName, tableName)
			createDDLMap[key] = createDDL
		}
	}
	log.Debugf("end load tables meta from db: %v", mts.tables)
	return createDDLMap
}

func (mts *MysqlTablesV2) loadTablesMetaToMemDB(tablesMeta string) {
	tablesMetaMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(tablesMeta), &tablesMetaMap)
	if err != nil {
		log.Fatalf("load tables meta to memDB failed, err: %v", err.Error())
	}
	for k, v := range tablesMetaMap {
		dbName := strings.Split(k, ".")[0]
		tableName := strings.Split(k, ".")[1]
		// tableName := strings.SplitN(k, ".", 1)
		_, err = mts.ExecuteSQLForMemDB(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName))
		if err != nil {
			log.Fatalf("load tables meta to memDB failed, db name: %s, err: %v", dbName, err.Error())
		}
		log.Debugf("load tables meta to memDB for create database: %s", dbName)
		createDDL := strings.Replace(fmt.Sprintf("%v", v), "CREATE TABLE ", fmt.Sprintf("CREATE TABLE `%s`.", dbName), 1)
		_, err = mts.ExecuteSQLForMemDB(createDDL)
		if err != nil {
			log.Warnf("load tables meta to memDB failed, create ddl: %s, err: %v", createDDL, err.Error())
		}
		log.Debugf("load tables meta to memDB for create table: %s.%s", dbName, tableName)
	}
}

func (mts *MysqlTablesV2) SaveMeta(tablesMeta string) error {
	// persistence now meta
	mts.Lock()
	defer mts.Unlock()

	sql := fmt.Sprintf("insert "+
		"into `%s`.table_checkpoints(`pos_id`, `tables_meta`)values(?, ?)", position.DbName)
	_, err := mts.ExecuteSQL(sql, mts.posId, tablesMeta)
	if err != nil {
		return err
	}
	log.Infof("flush tables meta to db")
	return nil
}

func (mts *MysqlTablesV2) GetColumnTypeFromRawType(rawType string) int {
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

func (mts *MysqlTablesV2) StartTimerSaveMeta() {
	mts.wg.Add(1)
	go func() {
		defer mts.wg.Done()
		ticker := time.NewTicker(time.Second * 86400) // 24h
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				marshal, err := json.Marshal(mts.LoadMetaFromDB())
				if err != nil {
					log.Fatalf("save tables meta failed. err: ", err.Error())
				}
				tablesMeta := string(marshal)

				// save meta
				err = mts.SaveMeta(tablesMeta)
				if err != nil {
					log.Fatalf("save tables meta failed. err: ", err.Error())
				}
				log.Infof("timer save meta to db successfully")
			case <-mts.ctx.Done():
				return
			}
		}
	}()
}
