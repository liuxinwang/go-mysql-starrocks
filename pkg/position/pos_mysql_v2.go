package position

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/juju/errors"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
	"sync"
	"time"
)

type MysqlBasePositionV2 struct {
	BinlogName string `toml:"binlog-name" json:"binlog-name"`
	BinlogPos  uint32 `toml:"binlog-pos" json:"binlog-pos"`
	BinlogGTID string `toml:"binlog-gtid" json:"binlog-gtid"`
}
type MysqlPositionV2 struct {
	sync.RWMutex
	*MysqlBasePositionV2
	FilePath     string
	Name         string
	lastSaveTime time.Time
	connLock     sync.Mutex
	conn         *client.Conn
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

func (pos *MysqlPositionV2) LoadPosition(conf *config.BaseConfig) string {
	var err error
	pos.ctx, pos.cancel = context.WithCancel(context.Background())
	// load pos info from db
	// init db

	mc := &config.MysqlConfig{}
	if err = mapstructure.Decode(conf.InputConfig.Config["source"], mc); err != nil {
		log.Fatal("input config parsing failed. err: ", err.Error())
	}

	pos.Name = conf.Name

	// init conn
	pos.conn, err = client.Connect(fmt.Sprintf("%s:%d", mc.Host, mc.Port),
		mc.UserName, mc.Password, "", func(c *client.Conn) { c.SetCharset("utf8") })
	if err != nil {
		log.Fatal("input config conn failed. err: ", err.Error())
	}

	// init database
	pos.initDb()

	basePos := &MysqlBasePositionV2{}
	queryPosSql := fmt.Sprintf("select `position` from `%s`.`positions` where `name` = '%s'", DbName, conf.Name)
	r, err := pos.executeSQL(queryPosSql)
	if err != nil {
		log.Fatal("query `position` table failed. err: ", err.Error())
	}
	position, err := r.GetString(0, 0)
	if err != nil {
		log.Fatalf("`position` data get failed. err: %v", err.Error())
	}
	err = json.Unmarshal([]byte(position), basePos)
	if err != nil {
		log.Fatalf("`position` data parsing failed. err: %v", err.Error())
	}

	pos.MysqlBasePositionV2 = basePos

	if pos.BinlogGTID != "" {
		return pos.BinlogGTID
	}

	// from local pos.info load
	positionFilePath := GetPositionFilePath(conf)
	initFilePositionData := "binlog-name = \"\"\nbinlog-pos = 0\nbinlog-gtid = \"\""
	FindPositionFileNotCreate(positionFilePath, initFilePositionData)
	if _, err = toml.DecodeFile(positionFilePath, basePos); err != nil {
		log.Fatal(err)
	}
	if basePos.BinlogGTID != "" {
		// update db position from local pos.info
		marshal, err := json.Marshal(basePos)
		if err != nil {
			log.Fatal("init position data failed. err: ", err.Error())
		}
		updPosSql := fmt.Sprintf("update `%s`.`positions` "+
			"set `position` = '%s' where `name` = '%s'", DbName, string(marshal), conf.Name)
		_, err = pos.executeSQL(updPosSql)
		if err != nil {
			log.Fatal("update `position` table failed. err: ", err.Error())
		}
		pos.MysqlBasePositionV2 = basePos
		pos.FilePath = positionFilePath
		return pos.BinlogGTID
	}

	// if binlogGTID is "", load config start-position
	if conf.InputConfig.StartPosition != "" {
		pos.BinlogGTID = conf.InputConfig.StartPosition
	}
	return pos.BinlogGTID
}

func (pos *MysqlPositionV2) initDb() {
	r, err := pos.executeSQL("select"+
		" 1 from information_schema.SCHEMATA where SCHEMA_NAME = ?", DbName)
	if err != nil {
		log.Fatalf("init position db `_go_mysql_sr` failed. err: %v", err.Error())
	}

	if r.RowNumber() == 0 {
		createSql := fmt.Sprintf(
			"CREATE "+
				"DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET utf8mb4", DbName)
		_, err = pos.executeSQL(createSql)
		if err != nil {
			log.Fatalf("init position db `_go_mysql_sr` failed. err: %v", err.Error())
		}
	}

	r, err = pos.executeSQL("select 1 from "+
		"information_schema.tables where table_schema = ? and table_name = ?", DbName, "positions")
	if err != nil {
		log.Fatalf("query table failed. err: %v", err.Error())
	}

	if r.RowNumber() == 0 {
		posTaSql := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS "+
				"`%s`.`positions` ("+
				"`id` int(11) NOT NULL AUTO_INCREMENT,"+
				"`name` varchar(255) NOT NULL,"+
				"`position` text,"+
				"`created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,"+
				"`updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"+
				"PRIMARY KEY (`id`),"+
				"UNIQUE KEY `name` (`name`)"+
				")", DbName)
		_, err = pos.executeSQL(posTaSql)
		if err != nil {
			log.Fatalf("query table failed. err: %v", err.Error())
		}
	}

	// init table table_checkpoints table_increment_ddl
	r, err = pos.executeSQL("select 1 from "+
		"information_schema.tables where table_schema = ? and table_name = ?", DbName, "table_checkpoints")
	if err != nil {
		log.Fatalf("query table failed. err: %v", err.Error())
	}

	if r.RowNumber() == 0 {
		tcTaSql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS "+
			"`%s`.`table_checkpoints` ("+
			"`id` int(11) NOT NULL AUTO_INCREMENT,"+
			"`pos_id` int(11) NOT NULL,"+
			"`tables_meta` mediumtext,"+
			"`created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,"+
			"`updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"+
			"PRIMARY KEY (`id`))", DbName)
		_, err = pos.executeSQL(tcTaSql)
		if err != nil {
			log.Fatal("init `table_checkpoints` table failed. err: ", err.Error())
		}
	}

	r, err = pos.executeSQL("select 1 from "+
		"information_schema.tables where table_schema = ? and table_name = ?", DbName, "table_increment_ddl")
	if err != nil {
		log.Fatalf("init position db `_go_mysql_sr` failed. err: %v", err.Error())
	}

	if r.RowNumber() == 0 {
		tidTaSql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS "+
			"`%s`.`table_increment_ddl` ("+
			"`id` int(11) NOT NULL AUTO_INCREMENT,"+
			"`pos_id` int(11) NOT NULL,"+
			"`db` varchar(50) NOT NULL,"+
			"`table_ddl` text,"+
			"`ddl_pos` varchar(500),"+
			"`created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,"+
			"`updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"+
			"PRIMARY KEY (`id`),"+
			"UNIQUE KEY `uniq_posid_ddlpos` (`pos_id`,`ddl_pos`))", DbName)
		_, err = pos.executeSQL(tidTaSql)
		if err != nil {
			log.Fatal("init `table_increment_ddl` table failed. err: ", err.Error())
		}
	}

	initPositionData := MysqlBasePositionV2{BinlogName: "", BinlogPos: 0, BinlogGTID: ""}
	marshal, err := json.Marshal(initPositionData)
	if err != nil {
		log.Fatal("init position data failed. err: ", err.Error())
	}
	posDataSql := fmt.Sprintf(
		"insert "+
			"ignore into `%s`.`positions`"+
			"(name, position)values('%s', '%v')", DbName, pos.Name, string(marshal))
	_, err = pos.executeSQL(posDataSql)
	if err != nil {
		log.Fatal("init `position` table failed. err: ", err.Error())
	}
}

func (pos *MysqlPositionV2) SavePosition() error {
	pos.Lock()
	defer pos.Unlock()

	n := time.Now()
	if n.Sub(pos.lastSaveTime) < time.Second {
		return nil
	}
	pos.lastSaveTime = n

	// save pos to db
	marshal, err := json.Marshal(pos.MysqlBasePositionV2)
	if err != nil {
		log.Fatalf("`position` data parsing failed. err: %v", err.Error())
	}
	saveSql := fmt.Sprintf("update `%s`.`positions` "+
		"set `position` = '%s' where `name` = '%s'", DbName, string(marshal), pos.Name)
	_, err = pos.executeSQL(saveSql)
	if err != nil {
		log.Errorf("canal save position to db %s.%s err %v", DbName, pos.Name, err)
	}
	log.Debugf("save canal sync position gtid: %s", pos.BinlogGTID)

	return errors.Trace(err)
}

func (pos *MysqlPositionV2) ModifyPosition(v string) error {
	pos.Lock()
	defer pos.Unlock()
	if v == "" {
		return errors.Errorf("empty value")
	}
	pos.BinlogGTID = v
	return nil
}

func (pos *MysqlPositionV2) StartPosition() {
	if pos.BinlogGTID == "" {
		log.Fatal("start position failed: empty value binlog gtid value")
	}

	pos.wg.Add(1)
	go func() {
		defer pos.wg.Done()
		ticker := time.NewTicker(time.Second * 3)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := pos.SavePosition(); err != nil {
					log.Fatalf("position save failed: %v", errors.ErrorStack(err))
				}
			case <-pos.ctx.Done():
				if err := pos.SavePosition(); err != nil {
					log.Fatalf("last position save failed: %v", errors.ErrorStack(err))
				}
				log.Infof("last position save successfully. position: %v", pos.BinlogGTID)
				return
			}
		}
	}()
}

func (pos *MysqlPositionV2) Close() {
	pos.cancel()
	pos.wg.Wait()
	if pos.conn != nil {
		err := pos.conn.Close()
		if err != nil {
			log.Warnf("close mysql save position conn failed. err: %v", err.Error())
		}
	}
	log.Infof("close mysql save position ticker goroutine.")
}

func (pos *MysqlPositionV2) executeSQL(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
	pos.connLock.Lock()
	defer pos.connLock.Unlock()
	rr, err = pos.conn.Execute(cmd, args...)
	if err != nil && !mysql.ErrorEqual(err, mysql.ErrBadConn) {
		return
	} else if mysql.ErrorEqual(err, mysql.ErrBadConn) {
		err = pos.conn.Close()
		if err != nil {
			return nil, err
		}
		pos.conn = nil
	} else {
		return
	}
	return
}
