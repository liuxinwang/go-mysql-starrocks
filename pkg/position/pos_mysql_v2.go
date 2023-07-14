package position

import (
	"bytes"
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
	"github.com/siddontang/go/ioutil2"
	"sync"
	"time"
)

type MysqlBasePositionV2 struct {
	BinlogName string `toml:"binlog-name"`
	BinlogPos  uint32 `toml:"binlog-pos"`
	BinlogGTID string `toml:"binlog-gtid"`
}
type MysqlPositionV2 struct {
	sync.RWMutex
	*MysqlBasePositionV2
	FilePath     string
	lastSaveTime time.Time
	connLock     sync.Mutex
	conn         *client.Conn
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

func (pos *MysqlPositionV2) LoadPosition(conf *config.BaseConfig) {
	var err error
	pos.ctx, pos.cancel = context.WithCancel(context.Background())
	// load pos info from db
	// init db

	mc := &config.MysqlConfig{}
	if err = mapstructure.Decode(conf.InputConfig, mc); err != nil {
		log.Fatal("input config parsing failed. err: ", err.Error())
	}

	// init conn
	pos.conn, err = client.Connect(fmt.Sprintf("%s:%d", mc.Host, mc.Port),
		mc.UserName, mc.Password, "", func(c *client.Conn) { c.SetCharset("utf8mb4") })
	if err != nil {
		log.Fatal("input config conn failed. err: ", err.Error())
	}

	// init database
	dbName := "_go_mysql_sr"
	createSql := fmt.Sprintf(
		"CREATE "+
			"DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET utf8mb4", dbName)
	_, err = pos.executeSQL(createSql)
	if err != nil {
		log.Fatal("init position db `_go_mysql_sr` failed. err: ", err.Error())
	}
	posTaSql := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS "+
			"`%s`.`positions` ("+
			"`id` int(11) NOT NULL,"+
			"`name` varchar(255) NOT NULL,"+
			"`position` text,"+
			"`created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,"+
			"`updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"+
			"PRIMARY KEY (`id`),"+
			"UNIQUE KEY `name` (`name`)"+
			")", dbName)
	_, err = pos.executeSQL(posTaSql)
	if err != nil {
		log.Fatal("init `position` table failed. err: ", err.Error())
	}
	initPositionData := MysqlBasePositionV2{BinlogName: "", BinlogPos: 0, BinlogGTID: ""}
	marshal, err := json.Marshal(initPositionData)
	if err != nil {
		log.Fatal("init position data failed. err: ", err.Error())
	}
	posDataSql := fmt.Sprintf(
		"insert "+
			"ignore into `%s`.`positions`"+
			"(name, position)values('%s', '%v')", dbName, conf.Name, marshal)
	_, err = pos.executeSQL(posDataSql)
	if err != nil {
		log.Fatal("init `position` table failed. err: ", err.Error())
	}

	basePos := &MysqlBasePositionV2{}
	queryPosSql := fmt.Sprintf("select `position` from `%s`.`positions` where `name` = '%s'", dbName, conf.Name)
	r, err := pos.executeSQL(queryPosSql)
	if err != nil {
		log.Fatal("query `position` table failed. err: ", err.Error())
	}
	for i := 0; i < r.RowNumber(); i++ {
		name, _ := r.GetString(i, 0)
		println(name)
	}

	pos.MysqlBasePositionV2 = basePos

	if pos.BinlogGTID != "" {
		return
	}

	// if binlogGTID is "", load config start-position
	if conf.InputConfig.StartPosition != "" {
		pos.BinlogGTID = conf.InputConfig.StartPosition
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
	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	if err := e.Encode(pos); err != nil {
		return err
	}
	var err error
	if err = ioutil2.WriteFileAtomic(pos.FilePath, buf.Bytes(), 0644); err != nil {
		log.Errorf("canal save position to file %s err %v", pos.FilePath, err)
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
