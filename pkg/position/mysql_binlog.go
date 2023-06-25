package position

import (
	"bytes"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/juju/errors"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go/ioutil2"
	"os"
	"strings"
	"sync"
	"time"
)

type MyPosition struct {
	sync.RWMutex
	BinlogName   string `toml:"binlog-name"`
	BinlogPos    string `toml:"binlog-pos"`
	BinlogGTID   string `toml:"binlog-gtid"`
	filePath     string
	lastSaveTime time.Time
}

func (pos *MyPosition) GetFilePath() string {
	return pos.filePath
}

func findFilePath(filePath string) {
	_, err := os.Stat(filePath)
	if err == nil {
		return
	}
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	} else {
		_, err = f.Write([]byte("binlog-name = \"\"\nbinlog-pos = \"\"\nbinlog-gtid = \"\""))
		if err != nil {
			log.Fatal(err)
		}
	}
}

func getPositionFilePath(conf *config.MysqlSrConfig) string {
	splits := strings.SplitAfter(conf.ConfigFile, "/")
	lastIndex := len(splits) - 1
	splits[lastIndex] = "_" + conf.Name + "-pos.info"
	positionFileName := strings.Join(splits, "")
	return positionFileName
}

func LoadPosition(conf *config.MysqlSrConfig) (*MyPosition, error) {
	var pos MyPosition
	var err error
	positionFilePath := getPositionFilePath(conf)
	findFilePath(positionFilePath)
	if _, err = toml.DecodeFile(positionFilePath, &pos); err != nil {
		return nil, errors.Trace(err)
	}
	pos.filePath = positionFilePath
	return &pos, err
}

func (pos *MyPosition) Save(gtid mysql.GTIDSet) error {
	pos.Lock()
	defer pos.Unlock()

	pos.BinlogGTID = fmt.Sprintf("%s", gtid.String())
	n := time.Now()
	if n.Sub(pos.lastSaveTime) < time.Second {
		return nil
	}
	pos.lastSaveTime = n
	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	e.Encode(pos)
	var err error
	if err = ioutil2.WriteFileAtomic(pos.filePath, buf.Bytes(), 0644); err != nil {
		log.Errorf("canal save position to file %s err %v", pos.filePath, err)
	}
	log.Debugf("save canal sync position gtid: %s", pos.BinlogGTID)
	return errors.Trace(err)
}
