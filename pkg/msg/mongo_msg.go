package msg

import (
	"encoding/json"
	"fmt"
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
	MongoTypeDouble     = iota + 1 // double
	MongoTypeString                // string
	MongoTypeObject                // object
	MongoTypeArray                 // array
	MongoTypeBinary                // bindata
	MongoTypeUndefined             // undefined
	MongoTypeObjectId              // objectid
	MongoTypeBoolean               // bool
	MongoTypeDate                  // date
	MongoTypeNull                  // null
	MongoTypeRegex                 // regex
	MongoTypeDbPointer             // dbpointer
	MongoTypeJavascript            // javascript
	MongoTypeSymbol                // symbol
	MongoTypeInt                   // int
	MongoTypeTimestamp             // timestamp
	MongoTypeLong                  // long
	MongoTypeDecimal               // decimal
)

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

func (mm *MongoMsg) NewMsg() interface{} {
	return &MongoMsg{}
}

func (mm *MongoMsg) String() string {
	b, _ := json.Marshal(mm.Data)
	return fmt.Sprintf(`{"ts": "%d", "time": "%v", "ns": "%v", "type": "%v", "data": "%v"}`,
		mm.Ts.T, time.Unix(int64(mm.Ts.T), 0).Format("2006-01-02 15:04:05"), mm.Ns.NsToString(), mm.OperationType, string(b))
}

func (c *Coll) AddColumn(name string, value interface{}) {
	index := len(c.Columns)
	c.Columns = append(c.Columns, CollColumn{Name: name})

	switch t := value.(type) {
	case float32:
		c.Columns[index].Type = MongoTypeDouble
	case float64:
		c.Columns[index].Type = MongoTypeDouble
	case string:
		c.Columns[index].Type = MongoTypeString
	case map[string]interface{}:
		c.Columns[index].Type = MongoTypeObject
	case primitive.A:
		c.Columns[index].Type = MongoTypeArray
	case primitive.Binary:
		c.Columns[index].Type = MongoTypeBinary
	case primitive.Undefined:
		c.Columns[index].Type = MongoTypeUndefined
	case primitive.ObjectID:
		c.Columns[index].Type = MongoTypeObjectId
	case bool:
		c.Columns[index].Type = MongoTypeBoolean
	case primitive.DateTime:
		c.Columns[index].Type = MongoTypeDate
	case primitive.Null:
		c.Columns[index].Type = MongoTypeNull
	case primitive.Regex:
		c.Columns[index].Type = MongoTypeRegex
	case primitive.DBPointer:
		c.Columns[index].Type = MongoTypeDbPointer
	case primitive.JavaScript:
		c.Columns[index].Type = MongoTypeJavascript
	case primitive.Symbol:
		c.Columns[index].Type = MongoTypeSymbol
	case int:
		c.Columns[index].Type = MongoTypeInt
	case int32:
		c.Columns[index].Type = MongoTypeInt
	case primitive.Timestamp:
		c.Columns[index].Type = MongoTypeTimestamp
	case int64:
		c.Columns[index].Type = MongoTypeLong
	case primitive.Decimal128:
		c.Columns[index].Type = MongoTypeDecimal
	case nil:
		return
	default:
		log.Warnf("column [%s] type could not be found, value type is %v", name, t)
		return
	}
	c.Columns[index].RawType = reflect.TypeOf(value).String()
}
