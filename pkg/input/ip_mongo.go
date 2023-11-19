package input

import (
	"context"
	"fmt"
	"github.com/juju/errors"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/channel"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/core"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/metrics"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/position"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/rule"
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"regexp"
	"sync"
	"sync/atomic"
	"time"
)

type MongoInputPlugin struct {
	*config.MongoConfig
	Client                *mongo.Client
	ChangeStream          *mongo.ChangeStream
	syncChan              *channel.SyncChannel
	position              core.Position
	includeTableRegexLock sync.RWMutex
	includeTableRegex     []*regexp.Regexp
	delay                 *uint32
	wg                    sync.WaitGroup
	ctx                   context.Context
	cancel                context.CancelFunc
}

type mongoInputContext struct {
	Id *msg.WatchId `bson:"_id"`
}

type streamObject struct {
	Id                *msg.WatchId `bson:"_id"`
	OperationType     msg.ActionType
	FullDocument      map[string]interface{}
	Ns                NS
	UpdateDescription map[string]interface{}
	DocumentKey       map[string]interface{}
	ClusterTime       primitive.Timestamp
}

type NS struct {
	Database   string `bson:"db"`
	Collection string `bson:"coll"`
}

func (mi *MongoInputPlugin) NewInput(inputConfig interface{}, ruleRegex []string, inSchema core.Schema) {
	mi.MongoConfig = &config.MongoConfig{}
	err := mapstructure.Decode(inputConfig, mi.MongoConfig)
	if err != nil {
		log.Fatal("input config parsing failed. err: ", err.Error())
	}
	mi.ctx, mi.cancel = context.WithCancel(context.Background())

	uri := fmt.Sprintf("mongodb://%s:%s@%s", mi.UserName, mi.Password, mi.Uri)
	// client初始化
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("init mongo client")
	mi.Client = client
	mi.includeTableRegex = mi.checkTableRegex(ruleRegex)
	mi.delay = new(uint32)
}

func (mi *MongoInputPlugin) StartInput(pos core.Position, syncChan *channel.SyncChannel) core.Position {
	var mongoPos = &position.MongoPosition{}
	if err := mapstructure.Decode(pos, mongoPos); err != nil {
		log.Fatalf("mongo position parsing failed. err: %s", err.Error())
	}

	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if mongoPos.ResumeTokens.Data != "" {
		// 指定token启动change stream
		opts.SetResumeAfter(mongoPos.ResumeTokens)
	} else if !mongoPos.InitStartPosition.IsZero() {
		// 指定时间戳启动change stream
		t := &primitive.Timestamp{T: uint32(mongoPos.InitStartPosition.Unix()), I: 1}
		opts.SetStartAtOperationTime(t)
	}

	log.Infof("start change stream")

	changeStream, err := mi.Client.Watch(context.TODO(), mongo.Pipeline{}, opts)
	if err != nil {
		log.Fatal(err)
	}

	// assign value
	mi.syncChan = syncChan
	mi.ChangeStream = changeStream

	log.Infof("start change stream successfully")
	log.Infof("iterate over the cursor to handle the change-stream events")

	if mongoPos.ResumeTokens.Data == "" {
		log.Infof("iterate first cursor get init resumeToken value for save position")
		firstResumeToken := mi.getFirstResumeToken()
		if err := mongoPos.ModifyPosition(firstResumeToken); err != nil {
			log.Fatalf("first position save failed: %v", err.Error())
		}
	}

	// iterate over the cursor to print the change-stream events
	go func() {
		for mi.ChangeStream.Next(context.TODO()) {
			mi.msgHandle()
		}
	}()

	if err := mi.ChangeStream.Err(); err != nil {
		if err := mi.ChangeStream.Close(context.TODO()); err != nil {
			log.Fatal(err)
		}
		log.Fatal(err)
	}

	mi.position = mongoPos

	// Start metrics
	mi.StartMetrics()

	return mongoPos
}

func (mi *MongoInputPlugin) StartMetrics() {
	mi.promTimingMetrics()
}

func (mi *MongoInputPlugin) Close() {
	if err := mi.ChangeStream.Close(context.TODO()); err != nil {
		log.Fatal(err)
	}
	log.Infof("close mongo change stream.")
	mi.cancel()
	mi.wg.Wait()
	log.Infof("close mongo input metrics.")
}

func (mi *MongoInputPlugin) SetIncludeTableRegex(config map[string]interface{}) (*regexp.Regexp, error) {
	mi.includeTableRegexLock.Lock()
	defer mi.includeTableRegexLock.Unlock()
	sourceSchema := fmt.Sprintf("%v", config["source-schema"])
	sourceTable := fmt.Sprintf("%v", config["source-table"])
	reg, err := regexp.Compile(rule.SchemaTableToStrRegex(sourceSchema, sourceTable))
	if err != nil {
		return reg, err
	}
	// if exists, return
	for _, regex := range mi.includeTableRegex {
		if regex.String() == reg.String() {
			return reg, errors.New("table rule already exists.")
		}
	}
	mi.includeTableRegex = append(mi.includeTableRegex, reg)
	return reg, nil
}

func (mi *MongoInputPlugin) RemoveIncludeTableRegex(config map[string]interface{}) (*regexp.Regexp, error) {
	mi.includeTableRegexLock.Lock()
	defer mi.includeTableRegexLock.Unlock()
	sourceSchema := fmt.Sprintf("%v", config["source-schema"])
	sourceTable := fmt.Sprintf("%v", config["source-table"])
	reg, err := regexp.Compile(rule.SchemaTableToStrRegex(sourceSchema, sourceTable))
	if err != nil {
		return reg, err
	}
	// if exists remove
	for i, regex := range mi.includeTableRegex {
		if regex.String() == reg.String() {
			mi.includeTableRegex = append(mi.includeTableRegex[:i], mi.includeTableRegex[i+1:]...)
			return reg, nil
		}
	}
	return reg, errors.New("table rule not exists.")
}

func (mi *MongoInputPlugin) msgHandle() {
	var event = &streamObject{}
	if err := mi.ChangeStream.Decode(event); err != nil {
		log.Fatal(err)
	}

	mi.setDelay(event)

	// 默认过滤drop事件
	if event.OperationType == "drop" {
		mi.onRowCtl(event)
		return
	}

	// rule table match
	if mi.checkTableMatch(event) {
		mi.onRowCtl(event)
		return
	}

	mi.onRow(event)
	mi.onRowCtl(event) // for commit msg callback, modify position
}

func (mi *MongoInputPlugin) onRow(e *streamObject) {
	m := mi.eventPreProcessing(e)
	mi.syncChan.SyncChan <- m
}

func (mi *MongoInputPlugin) onRowCtl(e *streamObject) {
	ctlMsg := &msg.Msg{
		Type:                msg.MsgCtl,
		PluginName:          msg.MongoPlugin,
		InputContext:        &mongoInputContext{Id: e.Id},
		AfterCommitCallback: mi.AfterMsgCommit,
	}
	mi.syncChan.SyncChan <- ctlMsg
}

func (mi *MongoInputPlugin) eventPreProcessing(e *streamObject) *msg.Msg {
	var dataMsg = &msg.Msg{
		Database:    e.Ns.Database,
		Table:       e.Ns.Collection,
		Type:        msg.MsgDML,
		DmlMsg:      &msg.DMLMsg{},
		ResumeToken: e.Id,
		Timestamp:   time.Unix(int64(e.ClusterTime.T), int64(0)),
		PluginName:  msg.MongoPlugin,
	}

	switch e.OperationType {
	case msg.InsertAction:
		dataMsg.DmlMsg.Action = msg.InsertAction
		dataMsg.DmlMsg.Data = e.FullDocument
	case msg.UpdateAction:
		dataMsg.DmlMsg.Action = msg.UpdateAction
		if e.FullDocument == nil {
			dataMsg.DmlMsg.Data = e.DocumentKey
			for key := range e.UpdateDescription {
				if key == "updatedFields" {
					updatedFields := e.UpdateDescription["updatedFields"].(map[string]interface{})
					for updKey := range updatedFields {
						dataMsg.DmlMsg.Data[updKey] = updatedFields[updKey]
					}
					break
				}
			}
		} else {
			dataMsg.DmlMsg.Data = e.FullDocument
		}
	case msg.DeleteAction:
		dataMsg.DmlMsg.Action = msg.DeleteAction
		dataMsg.DmlMsg.Data = e.DocumentKey
	case msg.ReplaceAction:
		dataMsg.DmlMsg.Action = msg.ReplaceAction
		dataMsg.DmlMsg.Data = e.FullDocument
	default:
		log.Fatalf("unhandled message type: %s", e)
	}
	log.Debugf("msg event: %s %s.%s %v", e.OperationType, e.Ns.Database, e.Ns.Collection, dataMsg.DmlMsg.Data)
	return dataMsg
}

func (mi *MongoInputPlugin) AfterMsgCommit(msg *msg.Msg) error {
	ctx := msg.InputContext.(*mongoInputContext)
	if ctx.Id.Data != "" {
		if err := mi.position.ModifyPosition(ctx.Id.Data); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (mi *MongoInputPlugin) promTimingMetrics() {
	mi.wg.Add(1)
	go func() {
		defer mi.wg.Done()
		ticker := time.NewTicker(time.Second * 3)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// prom sync delay set
				metrics.DelayReadTime.Set(float64(mi.GetDelay()))
			case <-mi.ctx.Done():
				return
			}
		}
	}()
}

func (mi *MongoInputPlugin) setDelay(e *streamObject) {
	var newDelay uint32
	now := uint32(time.Now().Unix())
	if now >= e.ClusterTime.T {
		newDelay = now - e.ClusterTime.T
	}
	atomic.StoreUint32(mi.delay, newDelay)
}

func (mi *MongoInputPlugin) GetDelay() uint32 {
	return atomic.LoadUint32(mi.delay)
}

func (mi *MongoInputPlugin) checkTableRegex(ruleRegex []string) []*regexp.Regexp {
	if n := len(ruleRegex); n > 0 {
		includeTableRegex := make([]*regexp.Regexp, n)
		for i, val := range ruleRegex {
			reg, err := regexp.Compile(val)
			if err != nil {
				log.Fatal(err)
			}
			includeTableRegex[i] = reg
		}
		return includeTableRegex
	}
	return nil
}

func (mi *MongoInputPlugin) checkTableMatch(e *streamObject) bool {
	matchFlag := true
	key := fmt.Sprintf("%s.%s", e.Ns.Database, e.Ns.Collection)
	// check include
	if mi.includeTableRegex != nil {
		for _, reg := range mi.includeTableRegex {
			if reg.MatchString(key) {
				matchFlag = false
				break
			}
		}
	}
	return matchFlag
}

func (mi *MongoInputPlugin) getFirstResumeToken() string {
	for mi.ChangeStream.Next(context.TODO()) {
		mi.msgHandle()
		return mi.ChangeStream.ResumeToken().Index(0).Value().StringValue()
	}
	return ""
}
