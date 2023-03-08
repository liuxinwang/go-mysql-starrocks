package position

import (
	"bytes"
	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go/ioutil2"
	"go-mysql-starrocks/pkg/config"
	"go-mysql-starrocks/pkg/msg"
	"os"
	"strings"
	"sync"
	"time"
)

type MongoPosition struct {
	sync.RWMutex
	ResumeTokens *msg.WatchId `bson:"_id"`
	filePath     string
	lastSaveTime time.Time
}

func (pos *Position) GetMongoFilePath() string {
	return pos.filePath
}

func findMongoFilePath(filePath string) {
	_, err := os.Stat(filePath)
	if err == nil {
		return
	}
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	} else {
		_, err = f.Write([]byte("[ResumeTokens]\n  Data = \"\""))
		if err != nil {
			log.Fatal(err)
		}
	}
}

func getMongoPositionFilePath(conf *config.MongoSrConfig) string {
	splits := strings.SplitAfter(conf.ConfigFile, "/")
	lastIndex := len(splits) - 1
	splits[lastIndex] = "_" + conf.Name + "-pos.info"
	positionFileName := strings.Join(splits, "")
	return positionFileName
}

func LoadMongoPosition(conf *config.MongoSrConfig) (*MongoPosition, error) {
	var pos MongoPosition
	var err error
	positionFilePath := getMongoPositionFilePath(conf)
	findMongoFilePath(positionFilePath)
	if _, err = toml.DecodeFile(positionFilePath, &pos); err != nil {
		return nil, errors.Trace(err)
	}
	pos.filePath = positionFilePath
	return &pos, err
}

func (pos *MongoPosition) MongoSave(resumeToken *msg.WatchId) error {
	if resumeToken == nil {
		return nil
	}
	pos.Lock()
	defer pos.Unlock()

	pos.ResumeTokens = resumeToken
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
	log.Debugf("save canal sync position resumeToken: %s", pos.ResumeTokens)
	return errors.Trace(err)
}
