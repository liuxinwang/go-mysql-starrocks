package filter

import (
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
)

type Filter interface {
	NewFilter(config map[string]interface{}) error
	Filter(msg *msg.Msg) bool
}
