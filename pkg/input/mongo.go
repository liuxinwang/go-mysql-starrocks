package input

import (
	"context"
	"fmt"
	"github.com/iancoleman/strcase"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/filter"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/output"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/position"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/rule"
	"github.com/siddontang/go-log/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
	"sync"
	"time"
)

type Mongo struct {
	*config.Mongo
	syncCh            chan interface{}
	syncChResumeToken *msg.WatchId `bson:"_id"` // sync chan中last ResumeToken
	ackResumeToken    *msg.WatchId `bson:"_id"` // sync data ack的 ResumeToken
	starrocks         *output.Starrocks
	rulesMap          map[string]*rule.MongoToSrRule
	ctx               context.Context
	cancel            context.CancelFunc
	position          *position.MongoPosition
	Client            *mongo.Client
	ChangeStream      *mongo.ChangeStream
	matcher           filter.ChangeStreamFilterMatcher
	collLock          sync.RWMutex
	colls             map[string]*msg.Coll
	startPosition     time.Time
	OutputType        string
	Config            *config.MongoSrConfig
}

type StreamObject struct {
	Id                *msg.WatchId `bson:"_id"`
	OperationType     string
	FullDocument      map[string]interface{}
	Ns                msg.NS
	UpdateDescription map[string]interface{}
	DocumentKey       map[string]interface{}
	ClusterTime       primitive.Timestamp
}

func (m *Mongo) Ctx() context.Context {
	return m.ctx
}

func (m *Mongo) Cancel() context.CancelFunc {
	return m.cancel
}

func NewMongo(conf *config.MongoSrConfig) *Mongo {
	m := &Mongo{}
	m.Config = conf
	m.Mongo = conf.Mongo
	m.starrocks = &output.Starrocks{Starrocks: conf.Starrocks}
	m.rulesMap = map[string]*rule.MongoToSrRule{}
	for _, r := range conf.Rules {
		m.rulesMap[r.SourceSchema+"."+r.SourceTable] = r
	}
	m.syncCh = make(chan interface{}, m.Config.SyncParam.ChannelSize)
	uri := fmt.Sprintf("mongodb://%s:%s@%s", m.UserName, m.Password, m.Uri)
	// 配置初始化
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("init mongo client")

	m.Client = client

	includeTableRegex := rule.NewMongoToSrRule(conf.Rules)
	ruleFilter := filter.NewRuleFilter(includeTableRegex)
	m.matcher = append(m.matcher, ruleFilter)

	m.ctx, m.cancel = context.WithCancel(context.Background())

	pos, err := position.LoadMongoPosition(conf)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("load mongo check position")
	m.position = pos

	m.colls = make(map[string]*msg.Coll)

	m.startPosition = conf.Input.StartPosition

	// 启动chanLoop
	go m.chanLoop()

	return m

}

func (m *Mongo) StartChangeStream() {
	defer func() {
		if err := m.ChangeStream.Close(context.TODO()); err != nil {
			log.Fatal(err)
		}
	}()

	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	if m.position.ResumeTokens.Data != "" {
		// 指定token启动change stream
		opts.SetResumeAfter(m.position.ResumeTokens)
	} else if !m.startPosition.IsZero() {
		// 指定时间戳启动change stream
		t := &primitive.Timestamp{T: uint32(m.startPosition.Unix()), I: 1}
		opts.SetStartAtOperationTime(t)
	}

	log.Infof("start change stream")

	changeStream, err := m.Client.Watch(context.TODO(), mongo.Pipeline{}, opts)

	m.ChangeStream = changeStream
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("start change stream successfully")
	log.Infof("iterate over the cursor to handle the change-stream events")

	// iterate over the cursor to print the change-stream events
	for changeStream.Next(context.TODO()) {
		var event StreamObject
		if err := changeStream.Decode(&event); err != nil {
			log.Fatal(err)
		}

		// 默认过滤drop事件
		if event.OperationType == "drop" {
			m.syncCh <- event.Id
			continue
		}

		if m.Config.Input.ConvertSnakeCase {
			// 转换document Field 从 camelCase 到 snake_case
			m.convertSnakeCase(&event)
		}

		dataMsg := m.eventPreProcessing(&event)
		if !m.matcher.IterateFilter(dataMsg) {
			if m.OutputType == "output" {
				log.Infof(dataMsg.String())
			} else {
				m.syncCh <- dataMsg
			}
		}
		m.syncCh <- event.Id
	}

	if err := changeStream.Err(); err != nil {
		log.Fatal(err)
	}
}

func (m *Mongo) convertSnakeCase(e *StreamObject) {
	for v := range e.FullDocument {
		snakeName := strcase.ToSnake(v)
		if snakeName != v {
			e.FullDocument[snakeName] = e.FullDocument[v]
			delete(e.FullDocument, v)
		}

		if v == "_id" {
			e.FullDocument["id"] = e.FullDocument[v]
			delete(e.FullDocument, v)
		}
	}

	if e.OperationType == msg.MongoDeleteAction {
		e.DocumentKey["id"] = e.DocumentKey["_id"]
		delete(e.DocumentKey, "_id")
	}

}

func (m *Mongo) eventPreProcessing(e *StreamObject) *msg.MongoMsg {
	var dataMsg = &msg.MongoMsg{}
	dataMsg.Ts = e.ClusterTime
	dataMsg.ResumeToken = e.Id
	dataMsg.OperationType = e.OperationType
	dataMsg.Ns = e.Ns
	dataMsg.DocumentKey = e.DocumentKey
	if e.OperationType == msg.MongoDeleteAction {
		dataMsg.Data = e.DocumentKey
	} else {
		dataMsg.Data = e.FullDocument
	}

	return dataMsg
}

func (m *Mongo) chanLoop() {
	ticker := time.NewTicker(time.Second * time.Duration(m.Config.SyncParam.FlushDelaySecond))
	defer ticker.Stop()

	eventsLen := 0
	schemaTableEvents := make(map[string][]*msg.MongoMsg)
	for {
		needFlush := false
		select {
		case v := <-m.syncCh:
			switch data := v.(type) {
			case *msg.MongoMsg:
				schemaTable := data.Ns.NsToString()
				rowsData, ok := schemaTableEvents[schemaTable]
				if !ok {
					schemaTableEvents[schemaTable] = make([]*msg.MongoMsg, 0, m.Config.SyncParam.ChannelSize)
				}
				m.collsCacheHandle(data)
				schemaTableEvents[schemaTable] = append(rowsData, data)
				eventsLen += 1

				if eventsLen >= m.Config.SyncParam.ChannelSize {
					needFlush = true
				}
			case *msg.WatchId:
				m.syncChResumeToken = data
			}
		case <-ticker.C:
			needFlush = true
		case <-m.ctx.Done():
			// 被取消或者超时就结束协程
			log.Infof("chanLoop output goroutine finished")
			return
		}

		if needFlush {
			for schemaTable := range schemaTableEvents {
				err := m.starrocks.MongoExecute(schemaTableEvents[schemaTable], m.rulesMap[schemaTable], m.colls[schemaTable])
				if err != nil {
					log.Errorf("do starrocks bulk err %v, close sync", err)
					m.cancel()
					return
				}
				delete(schemaTableEvents, schemaTable)
			}
			if err := m.position.MongoSave(m.syncChResumeToken); err != nil {
				m.cancel()
				return
			}
			m.ackResumeToken = m.syncChResumeToken
			eventsLen = 0
			ticker.Reset(time.Second * time.Duration(m.Config.SyncParam.FlushDelaySecond))
		}

	}
}

func (m *Mongo) collsCacheHandle(dataMsg *msg.MongoMsg) {
	key := dataMsg.Ns.NsToString()
	m.collLock.RLock()
	c, ok := m.colls[key]
	m.collLock.RUnlock()

	if ok {
		coll := &msg.Coll{Schema: dataMsg.Ns.Database, Name: dataMsg.Ns.Collection}
		m.getColumns(coll, dataMsg.Data)
		if reflect.DeepEqual(c.Columns, coll.Columns) {
			return
		}
		m.unionColumns(c, coll.Columns)
		return
	}

	// add coll to colls
	coll := &msg.Coll{Schema: dataMsg.Ns.Database, Name: dataMsg.Ns.Collection}
	m.getColumns(coll, dataMsg.Data)
	m.collLock.RLock()
	m.colls[key] = coll
	m.collLock.RUnlock()
}

func (m *Mongo) getColumns(coll *msg.Coll, data map[string]interface{}) {
	for k, v := range data {
		coll.AddColumn(k, v)
	}
}

func (m *Mongo) unionColumns(cacheColl *msg.Coll, msgColumns []msg.CollColumn) {
	columnsMap := make(map[string]bool)
	for _, c := range cacheColl.Columns {
		columnsMap[c.Name] = true
	}
	for _, c := range msgColumns {
		if _, ok := columnsMap[c.Name]; !ok {
			columnsMap[c.Name] = true
			cacheColl.Columns = append(cacheColl.Columns, c)
		}
	}
}
