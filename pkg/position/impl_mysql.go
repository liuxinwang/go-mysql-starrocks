package position

import (
	"bytes"
	"context"
	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go/ioutil2"
	"sync"
	"time"
)

type mysqlBasePosition struct {
	BinlogName string `toml:"binlog-name"`
	BinlogPos  uint32 `toml:"binlog-pos"`
	BinlogGTID string `toml:"binlog-gtid"`
}
type MysqlPosition struct {
	sync.RWMutex
	*mysqlBasePosition
	FilePath     string
	lastSaveTime time.Time
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

func (pos *MysqlPosition) LoadPosition(config *config.BaseConfig) {
	var err error
	pos.ctx, pos.cancel = context.WithCancel(context.Background())
	// load pos.info file position
	positionFilePath := GetPositionFilePath(config)
	initPositionData := "binlog-name = \"\"\nbinlog-pos = 0\nbinlog-gtid = \"\""
	FindPositionFileNotCreate(positionFilePath, initPositionData)
	if _, err = toml.DecodeFile(positionFilePath, &pos); err != nil {
		log.Fatal(err)
	}
	pos.FilePath = positionFilePath

	if pos.BinlogGTID != "" {
		return
	}

	// if binlogGTID is "", load config start-position
	if config.InputConfig.StartPosition != "" {
		pos.BinlogGTID = config.InputConfig.StartPosition
	}
}

func (pos *MysqlPosition) SavePosition() error {
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

func (pos *MysqlPosition) ModifyPosition(v string) error {
	pos.Lock()
	defer pos.Unlock()
	if v == "" {
		return errors.Errorf("empty value")
	}
	pos.BinlogGTID = v
	return nil
}

func (pos *MysqlPosition) StartPosition() {
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

func (pos *MysqlPosition) Close() {
	pos.cancel()
	pos.wg.Wait()
	log.Infof("close mysql save position ticker goroutine.")
}
