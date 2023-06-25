package filter

import (
	"github.com/liuxinwang/go-mysql-starrocks/pkg/channel"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/siddontang/go-log/log"
)

type MatcherFilter []Filter

func NewMatcherFilter(filterConfigs []*config.FilterConfig) MatcherFilter {
	var matcher MatcherFilter
	for _, fc := range filterConfigs {
		if fc.Type == "delete-dml-column" {
			ddcf := &DeleteDmlColumnFilter{}
			if err := ddcf.NewFilter(fc.Config); err != nil {
				log.Fatal(err)
			}
			matcher = append(matcher, ddcf)
		}
		if fc.Type == "convert-dml-column" {
			cdcf := &ConvertDmlColumnFilter{}
			if err := cdcf.NewFilter(fc.Config); err != nil {
				log.Fatal(err)
			}
			matcher = append(matcher, cdcf)
		}
	}
	return matcher
}

func (matcher MatcherFilter) IterateFilter(msg *msg.Msg) bool {
	for _, filter := range matcher {
		if filter.Filter(msg) {
			log.Debugf("filter msg %v", msg.DmlMsg.Data)
			return true
		}
	}
	return false
}

func (matcher MatcherFilter) StartFilter(syncChan *channel.SyncChannel, outputChan *channel.OutputChannel) {
	// 消费syncChan
	for {
		select {
		case v := <-syncChan.SyncChan:
			switch data := v.(type) {
			case *msg.Msg:
				// 过滤syncChan
				if !matcher.IterateFilter(data) {
					// 写入outputChan
					outputChan.SyncChan <- data
				}
			}
		}
	}
}
