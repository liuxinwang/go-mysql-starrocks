package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/juju/errors"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/position"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/registry"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/rule"
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
	"strings"
	"sync"
	"time"
)

type MysqlTablesMeta struct {
	tables map[string]*Table
}

type MysqlTables struct {
	sync.RWMutex
	*config.MysqlConfig
	tablesLock sync.RWMutex
	*MysqlTablesMeta
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

const MysqlName = "mysql"

func init() {
	registry.RegisterPlugin(registry.InputSchemaPlugin, MysqlName, &MysqlTables{})
}

func (mts *MysqlTables) Configure(pipelineName string, configInput map[string]interface{}) error {
	return nil
}

func (mts *MysqlTables) NewSchemaTables(conf *config.BaseConfig, pluginConfig map[string]interface{}, startPos string, rulesMap map[string]interface{}) {
	mts.MysqlTablesMeta = &MysqlTablesMeta{tables: make(map[string]*Table)}
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
		for k := range rulesMap {
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

	// init tables data
	mts.loadTablesMeta(tablesMeta)

	// handle increment ddl
	if updatedAt != "" {
		log.Infof("load last table meta for time: %s", updatedAt)
		incrementDdlSql := fmt.Sprintf("select db, table_name, table_ddl, ddl_pos, serial_number "+
			"from `%s`.table_increment_ddl where pos_id = '%d' and updated_at >= '%s' "+
			"and updated_at < '%s' order by updated_at, serial_number", position.DbName, posId, updatedAt, gtidTime)
		// TODO 增量ddl只处理同步表的
		idr, err := mts.ExecuteSQLForMetaDB(incrementDdlSql)
		if err != nil {
			log.Fatal("get increment ddl failed. err: ", err.Error())
		}
		for i := 0; i < idr.RowNumber(); i++ {
			db, _ := idr.GetString(i, 0)
			tableName, _ := idr.GetString(i, 1)
			ddl, _ := idr.GetString(i, 2)
			ddlPos, _ := idr.GetString(i, 3)
			serialNumber, _ := idr.GetInt(i, 4)
			log.Debugf("handle increment ddl: %v", ddl)
			err = mts.UpdateTable(db, tableName, ddl, ddlPos, int(serialNumber))
			if err != nil {
				log.Warnf("handle increment ddl failed, ddl: %v, err: %v", ddl, err.Error())
			}
		}
		log.Infof("replay increment ddl done, exec ddl events: %d", idr.RowNumber())
	}

	mts.StartTimerSaveMeta()
}

func (mts *MysqlTables) AddTableForMsg(msg *msg.Msg) error {
	return nil
}

func (mts *MysqlTables) AddTable(db string, table string) (*Table, error) {
	ddl, err := mts.GetTableCreateDDL(db, table)
	newTable, err := NewTable(ddl)
	if err != nil {
		return nil, err
	}
	newTable.Schema = db
	mts.tables[rule.RuleKeyFormat(db, table)] = newTable
	return newTable, nil
}

func (mts *MysqlTables) DelTable(db string, table string) (err error) {
	delete(mts.tables, rule.RuleKeyFormat(db, table))
	return nil
}

func (mts *MysqlTables) GetTableCreateDDL(db string, table string) (string, error) {
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

func (mts *MysqlTables) UpdateTable(db string, table string, ddl interface{}, pos string, index int) (err error) {
	t, ok := mts.tables[rule.RuleKeyFormat(db, table)]
	ddlStatements := make([]*DdlStatement, 0)

	ddlStatements, err = DdlToDdlStatements(fmt.Sprintf("%v", ddl), db)
	if err != nil {
		return err
	}

	if ddlStatements[0].IsLikeCreateTable { // create like ddl
		t2, ok2 := mts.tables[rule.RuleKeyFormat(ddlStatements[0].ReferTable.Schema, ddlStatements[0].ReferTable.Name)]
		if !ok2 {
			return errors.Errorf("table: %s.%s meta not found", db, table)
		}
		mts.tables[rule.RuleKeyFormat(db, table)] = &Table{
			Schema:            db,
			Name:              table,
			Comment:           t2.Comment,
			Columns:           t2.Columns,
			PrimaryKeyColumns: t2.PrimaryKeyColumns,
		}
	} else if ddlStatements[0].IsSelectCreateTable { // create select ddl
		return errors.Errorf("not support CREATE TABLE ... SELECT Statement for ddl: %v", ddl)
	} else if ddlStatements[0].IsDropTable { // drop table
		err = mts.DelTable(db, table)
		if err != nil {
			return err
		}
	} else if ddlStatements[0].IsCreateTable { // create ddl
		t = &Table{}
		err = TableDdlHandle(t, fmt.Sprintf("%v", ddl))
		if err != nil {
			return err
		}
		mts.tables[rule.RuleKeyFormat(db, table)] = t
	} else if ddlStatements[0].IsRenameTable {
		if !ok {
			return errors.Errorf("table: %s.%s meta not found", db, table)
		}
		err = TableDdlHandle(t, fmt.Sprintf("%v", ddl))

		// copy rename table
		_, ok2 := mts.tables[rule.RuleKeyFormat(t.Schema, t.Name)]
		if ok2 {
			return errors.Errorf("rename to table: %s.%s already exists", t.Schema, t.Name)
		}
		mts.tables[rule.RuleKeyFormat(t.Schema, t.Name)] = &Table{
			Schema:            t.Schema,
			Name:              t.Name,
			Comment:           t.Comment,
			Columns:           t.Columns,
			PrimaryKeyColumns: t.PrimaryKeyColumns,
		}
		// delete source table
		err = mts.DelTable(db, table)
		if err != nil {
			return err
		}
	} else { // alter table
		if !ok {
			return errors.Errorf("table: %s.%s meta not found", db, table)
		}
		err = TableDdlHandle(t, fmt.Sprintf("%v", ddl))

		if err != nil {
			return err
		}
	}

	// only test
	log.Debugf(t.ToString())
	insSql := fmt.Sprintf("insert ignore "+
		"into `%s`.table_increment_ddl(`pos_id`, `db`, `table_name`, `table_ddl`, `ddl_pos`, serial_number)values(?,?,?,?,?,?)", position.DbName)
	_, err = mts.ExecuteSQLForMetaDB(insSql, mts.posId, db, table, fmt.Sprintf("%v", ddl), pos, index)
	if err != nil {
		return err
	}
	return nil
}

func (mts *MysqlTables) GetTable(db string, table string) (*Table, error) {
	ta, ok := mts.tables[rule.RuleKeyFormat(db, table)]
	if !ok {
		return nil, fmt.Errorf("table: %s.%s meta not found", db, table)
	}
	return ta, nil
}

func (mts *MysqlTables) RefreshTable(db string, table string) {

}

func (mts *MysqlTables) Close() {
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

func (mts *MysqlTables) ExecuteSQLForMetaDB(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
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

func (mts *MysqlTables) LoadMetaFromDB(rulesMap map[string]interface{}) map[string]interface{} {
	// load meta from db
	log.Debugf("start load tables meta from db, waiting...")
	createDDLMap := make(map[string]interface{})
	var tables []string
	for k := range rulesMap {
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

func (mts *MysqlTables) LoadSyncTableMetaFromDB() map[string]interface{} {
	// load meta from db
	log.Debugf("start load sync tables meta from db, waiting...")
	createDDLMap := make(map[string]interface{})
	var tables []string
	for k := range mts.tables {
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

func (mts *MysqlTables) loadTablesMeta(tablesMeta string) {
	tablesMetaMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(tablesMeta), &tablesMetaMap)
	if err != nil {
		log.Fatalf("load tables meta failed, err: %v", err.Error())
	}
	for k, v := range tablesMetaMap {
		dbName := strings.Split(k, ".")[0]
		tableName := strings.Split(k, ".")[1]
		createDDL := strings.Replace(fmt.Sprintf("%v", v), "CREATE TABLE ", fmt.Sprintf("CREATE TABLE `%s`.", dbName), 1)
		tab, err := NewTable(createDDL)
		if err != nil {
			log.Fatalf("load tables meta failed, create ddl: %s, err: %v", createDDL, err.Error())
		}
		// only test
		if tab != nil {
			log.Debugf(tab.ToString())
		} else {
			log.Fatalf("load tables meta failed, create ddl: %s, err: NewTable func return nil", createDDL)
		}
		mts.tables[rule.RuleKeyFormat(dbName, tableName)] = tab
		log.Debugf("load tables meta to memDB for create table: %s.%s", dbName, tableName)
	}
}

func (mts *MysqlTables) SaveMeta(tablesMeta string) error {
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

func (mts *MysqlTables) StartTimerSaveMeta() {
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
					log.Fatalf("save tables meta failed. err: %s", err.Error())
				}
				tablesMeta := string(marshal)

				// save meta
				err = mts.SaveMeta(tablesMeta)
				if err != nil {
					log.Fatalf("save tables meta failed. err: %s", err.Error())
				}
				log.Infof("timer save meta to db successfully")
			case <-mts.ctx.Done():
				return
			}
		}
	}()
}

func (mts *MysqlTables) getTimestampForGtid(gtid string) uint32 {
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
