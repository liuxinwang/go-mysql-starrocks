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
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/juju/errors"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/position"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/rule"
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
	"strings"
	"sync"
	"time"
)

type MysqlTablesMetaV3 struct {
	tables map[string]*Table
}

type MysqlTablesV3 struct {
	sync.RWMutex
	*config.MysqlConfig
	tablesLock sync.RWMutex
	*MysqlTablesMetaV3
	metaConfig   *config.MysqlConfig
	posId        int
	connLock     sync.Mutex
	conn         *client.Conn
	metaConnLock sync.Mutex
	metaConn     *client.Conn
	memConnLock  sync.Mutex
	memConn      *client.Conn
	FilePath     string
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

func (mts *MysqlTablesV3) NewSchemaTables(conf *config.BaseConfig, pluginConfig map[string]interface{}, startPos string, rulesMap map[string]interface{}) {
	mts.MysqlTablesMetaV3 = &MysqlTablesMetaV3{tables: make(map[string]*Table)}
	mts.MysqlConfig = &config.MysqlConfig{}
	mts.metaConfig = &config.MysqlConfig{}
	err := mapstructure.Decode(pluginConfig["source"], mts.MysqlConfig)
	if err != nil {
		log.Fatalf("new schema tables config parsing failed. err: %v", err.Error())
	}
	err = mapstructure.Decode(pluginConfig["meta"], mts.metaConfig)
	if err != nil {
		log.Fatalf("new schema tables meta config parsing failed. err: %v", err.Error())
	}
	mts.ctx, mts.cancel = context.WithCancel(context.Background())
	// init conn
	mts.conn, err = client.Connect(fmt.Sprintf("%s:%d", mts.Host, mts.Port), mts.UserName, mts.Password, "")
	if err != nil {
		log.Fatalf("new schema tables conn failed. err: %v", err.Error())
	}
	_ = mts.conn.SetCharset("utf8")

	// init meta conn
	mts.metaConn, err = client.Connect(
		fmt.Sprintf("%s:%d", mts.metaConfig.Host, mts.metaConfig.Port),
		mts.metaConfig.UserName, mts.metaConfig.Password, "")
	if err != nil {
		log.Fatalf("new schema tables meta conn failed. err: %v", err.Error())
	}
	_ = mts.metaConn.SetCharset("utf8")

	// init vm memory mysql server for schema meta
	go mts.newMemMyServer()

	time.Sleep(time.Second * 3)
	// init vm memory mysql server conn
	mts.memConn, err = client.Connect(fmt.Sprintf("%s:%d", MemDbHost, MemDbPort), "", "", "")
	if err != nil {
		log.Fatalf("new schema tables conn failed. err: %v", err.Error())
	}

	// init tables
	for k, _ := range rulesMap {
		schemaName, tableName, err := rule.GetRuleKeySchemaTable(k)
		if err != nil {
			log.Fatalf("%v", err.Error())
		}
		table := &Table{
			Schema: schemaName,
			Name:   tableName,
		}
		mts.tables[k] = table
	}

	// if position exists, get position timestamp
	gtidTime := time.Now().Format("2006-01-02 15:04:05")
	if startPos != "" {
		gtidTimestamp := mts.getTimestampForGtid(startPos)
		tm := time.Unix(int64(gtidTimestamp), 0)
		gtidTime = tm.Format("2006-01-02 15:04:05")
	}

	// get last checkpoint data
	getLastTimeSql := fmt.Sprintf("select tc.`tables_meta`, tc.`updated_at` "+
		"from `%s`.`table_checkpoints` tc "+
		"inner join `%s`.`positions` po on tc.pos_id = po.id "+
		"where po.name = '%s' and tc.updated_at < '%s' "+
		"order by tc.updated_at desc limit 1", position.DbName, position.DbName, conf.Name, gtidTime)
	r, err := mts.ExecuteSQLForMetaDB(getLastTimeSql)
	if err != nil {
		log.Fatal("query last checkpoint data failed. err: ", err.Error())
	}
	// get pos_id
	getPosIdSql := fmt.Sprintf("select id from `%s`.positions where name = '%s'", position.DbName, conf.Name)
	posIdRs, err := mts.ExecuteSQLForMetaDB(getPosIdSql)
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
		marshal, err := json.Marshal(mts.LoadMetaFromDB(rulesMap))
		if err != nil {
			log.Fatalf("load meta from db error. err: %v", err.Error())
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
		// 对比tablesMeta和ruleMap，只加载ruleMap中有的
		tablesMetaMap := make(map[string]interface{})
		err := json.Unmarshal([]byte(tablesMeta), &tablesMetaMap)
		if err != nil {
			log.Fatalf("tables meta unmarshal map error. err: %v", err.Error())
		}
		tmpTablesMeta := make(map[string]interface{})
		for k, _ := range rulesMap {
			schemaName, tableName, err := rule.GetRuleKeySchemaTable(k)
			if err != nil {
				log.Fatalf("%v", err.Error())
			}
			key := fmt.Sprintf("%s.%s", schemaName, tableName)
			if value, ok := tablesMetaMap[key]; ok {
				tmpTablesMeta[key] = value
				log.Debugf("from [meta] table load table meta for %v.%v", schemaName, tableName)
			} else {
				// from db load table meta
				createDDL, err := mts.GetTableCreateDDL(schemaName, tableName)
				if err != nil {
					log.Fatalf("%v", err.Error())
				}
				tmpTablesMeta[key] = createDDL
				log.Debugf("from [source db] load table meta for %v.%v", schemaName, tableName)
			}
		}

		marshal, err := json.Marshal(tmpTablesMeta)
		if err != nil {
			log.Fatalf("load meta error. err: %v", err.Error())
		}
		tablesMeta = string(marshal)

		updatedAt, _ = r.GetString(0, 1)
	}

	// init memory mysql data
	mts.loadTablesMetaToMemDB(tablesMeta)

	// handle increment ddl
	if updatedAt != "" {
		log.Infof("load last table meta for time: %s", updatedAt)
		incrementDdlSql := fmt.Sprintf("select db, table_ddl "+
			"from `%s`.table_increment_ddl where pos_id = '%d' and updated_at >= '%s' "+
			"and updated_at < '%s'", position.DbName, posId, updatedAt, gtidTime)
		idr, err := mts.ExecuteSQLForMetaDB(incrementDdlSql)
		if err != nil {
			log.Fatal("get increment ddl failed. err: ", err.Error())
		}
		for i := 0; i < idr.RowNumber(); i++ {
			db, _ := idr.GetString(i, 0)
			ddl, _ := idr.GetString(i, 1)
			err = mts.incrementDdlExec(db, "", ddl)
			if err != nil {
				log.Warnf("handle increment ddl failed, ddl: %v, err: %v", ddl, err.Error())
			}
		}
		log.Infof("replay increment ddl done, exec ddl events: %d", idr.RowNumber())
	}

	mts.StartTimerSaveMeta()
}

func (mts *MysqlTablesV3) newMemMyServer() {
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

func (mts *MysqlTablesV3) AddTableForMsg(msg *msg.Msg) error {
	return nil
}

func (mts *MysqlTablesV3) AddTable(db string, table string) (*Table, error) {
	ddl, err := mts.GetTableCreateDDL(db, table)
	if err != nil {
		return nil, err
	}
	if err := mts.memConn.UseDB(db); err != nil {
		// db not found handle: create database
		if strings.Contains(err.Error(), "database not found") {
			log.Infof("memory db: database not found, create database %s", db)
			err = mts.createDbForMemDB(db)
			if err != nil {
				return nil, err
			}
			if err = mts.memConn.UseDB(db); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	_ = mts.memConn.SetCharset("utf8")
	_, err = mts.ExecuteSQLForMemDB(fmt.Sprintf("%v", ddl))
	if err != nil {
		return nil, err
	}

	t := &Table{
		Schema: db,
		Name:   table,
	}
	mts.tables[rule.RuleKeyFormat(db, table)] = t
	return t, nil
}

func (mts *MysqlTablesV3) DelTable(db string, table string) (err error) {
	delete(mts.tables, rule.RuleKeyFormat(db, table))

	_ = mts.memConn.SetCharset("utf8")
	_, err = mts.ExecuteSQLForMemDB(fmt.Sprintf("drop table %v.%v", db, table))
	if err != nil {
		return err
	}
	return nil
}

func (mts *MysqlTablesV3) GetTableCreateDDL(db string, table string) (string, error) {
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

func (mts *MysqlTablesV3) UpdateTable(db string, table string, ddl interface{}, pos string) (err error) {
	if err = mts.memConn.UseDB(db); err != nil {
		// db not found handle: create database
		if strings.Contains(err.Error(), "database not found") {
			log.Infof("memory db: database not found, create database %s", db)
			err = mts.createDbForMemDB(db)
			if err != nil {
				return err
			}
			if err = mts.memConn.UseDB(db); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	_ = mts.memConn.SetCharset("utf8")
	_, err = mts.ExecuteSQLForMemDB(fmt.Sprintf("%v", ddl))
	if err != nil {
		return err
	}
	insSql := fmt.Sprintf("insert ignore "+
		"into `%s`.table_increment_ddl(`pos_id`, `db`, `table_ddl`, `ddl_pos`)values(?, ?, ?, ?)", position.DbName)
	_, err = mts.ExecuteSQLForMetaDB(insSql, mts.posId, db, fmt.Sprintf("%v", ddl), pos)
	if err != nil {
		return err
	}
	return nil
}

func (mts *MysqlTablesV3) createDbForMemDB(db string) (err error) {
	_ = mts.memConn.SetCharset("utf8")
	ddl := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", db)
	_, err = mts.ExecuteSQLForMemDB(ddl)
	if err != nil {
		return err
	}
	return nil
}

func (mts *MysqlTablesV3) incrementDdlExec(db string, table string, ddl interface{}) (err error) {
	if err = mts.memConn.UseDB(db); err != nil {
		// db not found handle: create database
		if strings.Contains(err.Error(), "database not found") {
			log.Infof("memory db: database not found, create database %s", db)
			err = mts.createDbForMemDB(db)
			if err != nil {
				return err
			}
			if err = mts.memConn.UseDB(db); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	_, err = mts.ExecuteSQLForMemDB(fmt.Sprintf("%v", ddl))
	if err != nil {
		return err
	}
	return nil
}

func (mts *MysqlTablesV3) GetTable(db string, table string) (*Table, error) {
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

func (mts *MysqlTablesV3) RefreshTable(db string, table string) {

}

func (mts *MysqlTablesV3) Close() {
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
	if mts.metaConn != nil {
		err := mts.metaConn.Close()
		if err != nil {
			log.Fatalf("schema tables close meta conn failed: %v", err.Error())
		}
		log.Infof("schema tables meta conn is closed.")
	}
}

func (mts *MysqlTablesV3) ExecuteSQL(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
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

func (mts *MysqlTablesV3) ExecuteSQLForMetaDB(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
	mts.metaConnLock.Lock()
	defer mts.metaConnLock.Unlock()
	argF := make([]func(*client.Conn), 0)
	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if mts.metaConn == nil {
			mts.metaConn, err = client.Connect(
				fmt.Sprintf("%s:%d", mts.metaConfig.Host, mts.metaConfig.Port),
				mts.metaConfig.UserName, mts.metaConfig.Password, "", argF...)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		rr, err = mts.metaConn.Execute(cmd, args...)
		if err != nil && !mysql.ErrorEqual(err, mysql.ErrBadConn) {
			return
		} else if mysql.ErrorEqual(err, mysql.ErrBadConn) {
			err := mts.metaConn.Close()
			if err != nil {
				return nil, err
			}
			mts.metaConn = nil
			continue
		} else {
			return
		}
	}
	return
}

func (mts *MysqlTablesV3) ExecuteSQLForMemDB(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
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

func (mts *MysqlTablesV3) LoadMetaFromDB(rulesMap map[string]interface{}) map[string]interface{} {
	// load meta from db
	log.Debugf("start load tables meta from db, waiting...")
	createDDLMap := make(map[string]interface{})
	var tables []string
	for k, _ := range rulesMap {
		schemaName, tableName, err := rule.GetRuleKeySchemaTable(k)
		if err != nil {
			log.Fatalf("%v", err)
		}
		createDDL, err := mts.GetTableCreateDDL(schemaName, tableName)
		if err != nil {
			log.Fatalf("%v", err)
		}
		key := fmt.Sprintf("%s.%s", schemaName, tableName)
		tables = append(tables, key)
		createDDLMap[key] = createDDL
	}
	log.Debugf("end load tables meta from db: %v", tables)
	return createDDLMap
}

func (mts *MysqlTablesV3) LoadSyncTableMetaFromDB() map[string]interface{} {
	// load meta from db
	log.Debugf("start load sync tables meta from db, waiting...")
	createDDLMap := make(map[string]interface{})
	var tables []string
	for k, _ := range mts.tables {
		schemaName, tableName, err := rule.GetRuleKeySchemaTable(k)
		if err != nil {
			log.Fatalf("%v", err)
		}
		createDDL, err := mts.GetTableCreateDDL(schemaName, tableName)
		if err != nil {
			log.Fatalf("%v", err)
		}
		key := fmt.Sprintf("%s.%s", schemaName, tableName)
		tables = append(tables, key)
		createDDLMap[key] = createDDL
	}
	log.Debugf("end load sync tables meta from db: %v", tables)
	return createDDLMap
}

func (mts *MysqlTablesV3) loadTablesMetaToMemDB(tablesMeta string) {
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

func (mts *MysqlTablesV3) SaveMeta(tablesMeta string) error {
	// persistence now meta
	mts.Lock()
	defer mts.Unlock()

	sql := fmt.Sprintf("insert "+
		"into `%s`.table_checkpoints(`pos_id`, `tables_meta`)values(?, ?)", position.DbName)
	_, err := mts.ExecuteSQLForMetaDB(sql, mts.posId, tablesMeta)
	if err != nil {
		return err
	}
	log.Infof("flush tables meta to db")
	return nil
}

func (mts *MysqlTablesV3) GetColumnTypeFromRawType(rawType string) int {
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

func (mts *MysqlTablesV3) StartTimerSaveMeta() {
	mts.wg.Add(1)
	go func() {
		defer mts.wg.Done()
		ticker := time.NewTicker(time.Second * 86400) // 24h
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				marshal, err := json.Marshal(mts.LoadSyncTableMetaFromDB())
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

func (mts *MysqlTablesV3) getTimestampForGtid(gtid string) uint32 {
	// Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
	// flavor is mysql or mariadb
	cfg := replication.BinlogSyncerConfig{
		ServerID: 6166,
		Flavor:   "mysql",
		Host:     mts.Host,
		Port:     uint16(mts.Port),
		User:     mts.UserName,
		Password: mts.Password,
	}
	log.Infof("create a slave for get start gtid timestamp...")
	syncer := replication.NewBinlogSyncer(cfg)

	// Start sync with specified binlog file and position
	// streamer, _ := syncer.StartSync(mysql.Position{binlogFile, binlogPos})
	var err error
	var gs mysql.GTIDSet
	if gs, err = mysql.ParseGTIDSet("mysql", gtid); err != nil {
		log.Fatal(err)
	}
	streamer, _ := syncer.StartSyncGTID(gs)

	var gtidTimestamp uint32

	for {
		ev, _ := streamer.GetEvent(context.Background())
		// Dump event
		// ev.Dump(os.Stdout)
		if ev.Header.EventType == replication.GTID_EVENT {
			gtidTimestamp = ev.Header.Timestamp
			break
		}
	}
	syncer.Close()
	log.Infof("get start gtid timestamp: %v", gtidTimestamp)
	return gtidTimestamp
}
