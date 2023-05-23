package msg

import (
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/siddontang/go-log/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"reflect"
	"time"
)

const (
	MongoUpdateAction  = "update"
	MongoInsertAction  = "insert"
	MongoDeleteAction  = "delete"
	MongoReplaceAction = "replace"
)

const (
	MONGO_TYPE_DOUBLE     = iota + 1 // double
	MONGO_TYPE_STRING                // string
	MONGO_TYPE_OBJECT                // object
	MONGO_TYPE_ARRAY                 // array
	MONGO_TYPE_BINARY                // binData
	MONGO_TYPE_UNDEFINED             // undefined
	MONGO_TYPE_OBJECTID              // objectId
	MONGO_TYPE_BOOLEAN               // bool
	MONGO_TYPE_DATE                  // date
	MONGO_TYPE_NULL                  // null
	MONGO_TYPE_REGEX                 // regex
	MONGO_TYPE_DBPOINTER             // dbPointer
	MONGO_TYPE_JAVASCRIPT            // javascript
	MONGO_TYPE_SYMBOL                // symbol
	MONGO_TYPE_INT                   // int
	MONGO_TYPE_TIMESTAMP             // timestamp
	MONGO_TYPE_LONG                  // long
	MONGO_TYPE_DECIMAL               // decimal
)

type Msg struct {
	Table         *schema.Table
	Action        string
	IgnoreColumns []string
	Data          map[string]interface{}
	Old           map[string]interface{}
}

type MongoMsg struct {
	Ts            primitive.Timestamp
	ResumeToken   *WatchId `bson:"_id"`
	Ns            NS
	OperationType string
	DocumentKey   map[string]interface{}
	Data          map[string]interface{}
	Old           map[string]interface{}
}

type WatchId struct {
	Data string `bson:"_data"`
}

type NS struct {
	Database   string `bson:"db"`
	Collection string `bson:"coll"`
}

func (ns *NS) NsToString() string {
	return ns.Database + "." + ns.Collection
}

type Coll struct {
	Schema    string
	Name      string
	Columns   []CollColumn
	PKColumns []int
}

type CollColumn struct {
	Name    string
	Type    int
	RawType string
}

func (c *Coll) AddColumn(name string, value interface{}) {
	index := len(c.Columns)
	c.Columns = append(c.Columns, CollColumn{Name: name})

	switch t := value.(type) {
	case float32:
		c.Columns[index].Type = MONGO_TYPE_DOUBLE
	case float64:
		c.Columns[index].Type = MONGO_TYPE_DOUBLE
	case string:
		c.Columns[index].Type = MONGO_TYPE_STRING
	case map[string]interface{}:
		c.Columns[index].Type = MONGO_TYPE_OBJECT
	case primitive.A:
		c.Columns[index].Type = MONGO_TYPE_ARRAY
	case primitive.Binary:
		c.Columns[index].Type = MONGO_TYPE_BINARY
	case primitive.Undefined:
		c.Columns[index].Type = MONGO_TYPE_UNDEFINED
	case primitive.ObjectID:
		c.Columns[index].Type = MONGO_TYPE_OBJECTID
	case bool:
		c.Columns[index].Type = MONGO_TYPE_BOOLEAN
	case primitive.DateTime:
		c.Columns[index].Type = MONGO_TYPE_DATE
	case primitive.Null:
		c.Columns[index].Type = MONGO_TYPE_NULL
	case primitive.Regex:
		c.Columns[index].Type = MONGO_TYPE_REGEX
	case primitive.DBPointer:
		c.Columns[index].Type = MONGO_TYPE_DBPOINTER
	case primitive.JavaScript:
		c.Columns[index].Type = MONGO_TYPE_JAVASCRIPT
	case primitive.Symbol:
		c.Columns[index].Type = MONGO_TYPE_SYMBOL
	case int:
		c.Columns[index].Type = MONGO_TYPE_INT
	case int32:
		c.Columns[index].Type = MONGO_TYPE_INT
	case primitive.Timestamp:
		c.Columns[index].Type = MONGO_TYPE_TIMESTAMP
	case int64:
		c.Columns[index].Type = MONGO_TYPE_LONG
	case primitive.Decimal128:
		c.Columns[index].Type = MONGO_TYPE_DECIMAL
	case nil:
		return
	default:
		log.Warnf("column [%s] type could not be found, value type is %v", name, t)
		return
	}
	c.Columns[index].RawType = reflect.TypeOf(value).String()
}

func (mm *MongoMsg) String() string {
	b, _ := json.Marshal(mm.Data)
	return fmt.Sprintf(`{"ts": "%d", "time": "%v", "ns": "%v", "type": "%v", "data": "%v"}`,
		mm.Ts.T, time.Unix(int64(mm.Ts.T), 0).Format("2006-01-02 15:04:05"), mm.Ns.NsToString(), mm.OperationType, string(b))
}

func (msg *Msg) String() string {
	b, _ := json.Marshal(msg.Data)
	return fmt.Sprintf(`{"table": "%v", "action": "%v", "data": "%v"}`,
		msg.Table.String(), msg.Action, string(b))
}
