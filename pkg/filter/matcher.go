package filter

import (
	"github.com/liuxinwang/go-mysql-starrocks/pkg/channel"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/metrics"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/schema"
	"github.com/siddontang/go-log/log"
)

type MatcherFilter []Filter

func NewMatcherFilter(filterConfigs []*config.FilterConfig) MatcherFilter {
	var matcher MatcherFilter
	for _, fc := range filterConfigs {
		switch typ := fc.Type; typ {
		case DeleteDMLColumnFilterName:
			ddcf := &DeleteDmlColumnFilter{}
			if err := ddcf.NewFilter(fc.Config); err != nil {
				log.Fatal(err)
			}
			matcher = append(matcher, ddcf)
		case ConvertDmlColumnFilterName:
			cdcf := &ConvertDmlColumnFilter{}
			if err := cdcf.NewFilter(fc.Config); err != nil {
				log.Fatal(err)
			}
			matcher = append(matcher, cdcf)
		case ConvertSnakeCaseColumnFilterName:
			cscf := &ConvertSnakeCaseColumnFilter{}
			if err := cscf.NewFilter(fc.Config); err != nil {
				log.Fatal(err)
			}
			matcher = append(matcher, cscf)
		case RenameDmlColumnFilterName:
			rdcf := &RenameDmlColumnFilter{}
			if err := rdcf.NewFilter(fc.Config); err != nil {
				log.Fatal(err)
			}
			matcher = append(matcher, rdcf)
		case JsDmlColumnFilterName:
			jdcf := &JsDmlColumnFilter{}
			if err := jdcf.NewFilter(fc.Config); err != nil {
				log.Fatal(err)
			}
			matcher = append(matcher, jdcf)
		default:
			log.Warnf("filter: %s unhandled will not take effect.", typ)
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

func (matcher MatcherFilter) StartFilter(syncChan *channel.SyncChannel, outputChan *channel.OutputChannel, inSchema schema.Schema) {
	// 消费syncChan
	go func() {
		for {
			select {
			case v := <-syncChan.SyncChan:
				switch data := v.(type) {
				case *msg.Msg:
					// 过滤syncChan
					if !matcher.IterateFilter(data) {
						// 写入outputChan
						outputChan.SyncChan <- data

						if data.Type == msg.MsgDML {
							// prom read event number counter
							metrics.OpsReadProcessed.Inc()
							if data.PluginName == msg.MongoPlugin {
								// add table cache for mongo
								err := inSchema.AddTableForMsg(data)
								if err != nil {
									log.Fatalf("add table meta for msg missing: %v", data)
								}
							}
						}
					}
				}
			case <-syncChan.Done:
				log.Infof("close syncChan filter goroutine.")
				log.Infof("close input sync chan.")
				return
			}
		}
	}()
}
