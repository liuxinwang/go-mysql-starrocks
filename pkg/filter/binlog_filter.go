package filter

import (
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/siddontang/go-log/log"
)

type BinlogFilter interface {
	Filter(msg *msg.MysqlMsg) bool
}

type BinlogFilterMatcher []BinlogFilter

func (matcher BinlogFilterMatcher) IterateFilter(msg *msg.MysqlMsg) bool {
	for _, filter := range matcher {
		if filter.Filter(msg) {
			log.Debugf("filter binlog event %v", msg.Data)
			return true
		}
	}
	return false
}
