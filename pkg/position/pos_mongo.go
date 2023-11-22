package position

import (
	"bytes"
	"context"
	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/registry"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go/ioutil2"
	"strconv"
	"sync"
	"time"
)

type mongoBasePosition struct {
	ResumeTokens *msg.WatchId `bson:"_id"`
}
type MongoPosition struct {
	sync.RWMutex
	*mongoBasePosition
	FilePath          string
	lastSaveTime      time.Time
	InitStartPosition time.Time
	wg                sync.WaitGroup
	ctx               context.Context
	cancel            context.CancelFunc
}

const MongoPosName = "mongo"

func init() {
	registry.RegisterPlugin(registry.InputPositionPlugin, MongoPosName, &MongoPosition{})
}

func (pos *MongoPosition) Configure(pipelineName string, configInput map[string]interface{}) error {
	return nil
}

func (pos *MongoPosition) LoadPosition(config *config.BaseConfig) string {
	var err error
	pos.ctx, pos.cancel = context.WithCancel(context.Background())
	// load pos.info file position
	positionFilePath := GetPositionFilePath(config)
	initPositionData := "[ResumeTokens]\n  Data = \"\""
	FindPositionFileNotCreate(positionFilePath, initPositionData)
	basePos := &mongoBasePosition{}
	if _, err = toml.DecodeFile(positionFilePath, basePos); err != nil {
		log.Fatal(err)
	}
	pos.mongoBasePosition = basePos
	pos.FilePath = positionFilePath

	if pos.ResumeTokens.Data != "" {
		tm := time.Unix(int64(pos.resumeTokenTimestamp()), 0)
		return tm.Format("2006-01-02 15:04:05")
	}

	// if ResumeTokens data is "", load config start-position
	if config.InputConfig.StartPosition != "" {
		var startPosition time.Time
		startPosition, err = time.ParseInLocation("2006-01-02 15:04:05", config.InputConfig.StartPosition, time.Local)
		if err != nil {
			log.Fatal(err)
		}
		pos.InitStartPosition = startPosition
	}
	return pos.InitStartPosition.Format("2006-01-02 15:04:05")
}

func (pos *MongoPosition) SavePosition() error {
	if pos.ResumeTokens == nil {
		return nil
	}
	pos.Lock()
	defer pos.Unlock()

	n := time.Now()
	if n.Sub(pos.lastSaveTime) < time.Second {
		return nil
	}
	pos.lastSaveTime = n
	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	var err error
	err = e.Encode(pos.mongoBasePosition)
	if err != nil {
		log.Errorf("save change stream sync position to file %s err %v", pos.FilePath, err)
	}
	if err = ioutil2.WriteFileAtomic(pos.FilePath, buf.Bytes(), 0644); err != nil {
		log.Errorf("save change stream sync position to file %s err %v", pos.FilePath, err)
	}
	log.Infof("save change stream sync position resumeToken timestamp: %d", pos.resumeTokenTimestamp())
	return errors.Trace(err)
}

func (pos *MongoPosition) ModifyPosition(v string) error {
	pos.Lock()
	defer pos.Unlock()
	if v == "" {
		return errors.Errorf("empty value")
	}
	pos.ResumeTokens.Data = v
	return nil
}

func (pos *MongoPosition) StartPosition() {
	if pos.ResumeTokens.Data == "" {
		log.Fatal("start position failed: empty value resumeTokens value")
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
				log.Infof("last position save successfully. position: %v", pos.resumeTokenTimestamp())
				return
			}
		}
	}()
}

func (pos *MongoPosition) Close() {
	pos.cancel()
	pos.wg.Wait()
	log.Infof("close mongo save position ticker goroutine.")
}

func (pos *MongoPosition) resumeTokenTimestamp() uint64 {
	i, err := strconv.ParseUint(pos.ResumeTokens.Data[2:18], 16, 64)
	if err != nil {
		log.Errorf("resumeToken parsing timestamp err %v", err)
	}
	return i >> 32

}
