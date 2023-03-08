package filter

import (
	"github.com/siddontang/go-log/log"
	"go-mysql-starrocks/pkg/msg"
)

type BinlogFilter interface {
	Filter(msg *msg.Msg) bool
}

type BinlogFilterMatcher []BinlogFilter

func (matcher BinlogFilterMatcher) IterateFilter(msg *msg.Msg) bool {
	for _, filter := range matcher {
		if filter.Filter(msg) {
			log.Debugf("filter binlog event %v", msg.Data)
			return true
		}
	}
	return false
}
