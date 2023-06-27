package channel

import (
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
)

type SyncChannel struct {
	SyncChan               chan interface{}
	FLushCHanMaxWaitSecond int
	Done                   chan struct{}
}

func (sc *SyncChannel) NewChannel(config *config.SyncParamConfig) {
	sc.SyncChan = make(chan interface{}, config.ChannelSize)
	sc.FLushCHanMaxWaitSecond = config.FlushDelaySecond
	sc.Done = make(chan struct{})
}

func (sc *SyncChannel) GetChannel() interface{} {
	return sc
}

func (sc *SyncChannel) Close() {
	close(sc.Done)
}
