package position

import (
	"bytes"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go/ioutil2"
	"os"
	"sync"
	"time"
)

var filePath = ".gtid_executed.toml"

type Position struct {
	sync.RWMutex
	BinlogName   string `toml:"binlog-name"`
	BinlogPos    string `toml:"binlog-pos"`
	BinlogGTID   string `toml:"binlog-gtid"`
	filePath     string
	lastSaveTime time.Time
}

func findFilePath() {
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

func LoadPosition() (*Position, error) {
	var pos Position
	var err error
	findFilePath()
	if _, err = toml.DecodeFile(filePath, &pos); err != nil {
		return nil, errors.Trace(err)
	}
	return &pos, err
}

func (pos *Position) Save(gtid mysql.GTIDSet) error {
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
	if err = ioutil2.WriteFileAtomic(filePath, buf.Bytes(), 0644); err != nil {
		log.Errorf("canal save position to file %s err %v", filePath, err)
	}
	log.Debugf("save canal sync position gtid: %s", pos.BinlogGTID)
	return errors.Trace(err)
}
