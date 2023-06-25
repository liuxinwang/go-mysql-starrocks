package channel

import "github.com/liuxinwang/go-mysql-starrocks/pkg/config"

type SyncChannel struct {
	SyncChan               chan interface{}
	FLushCHanMaxWaitSecond int
}

func (sc *SyncChannel) NewChannel(config *config.SyncParamConfig) {
	sc.SyncChan = make(chan interface{}, config.ChannelSize)
	sc.FLushCHanMaxWaitSecond = config.FlushDelaySecond
}

func (sc *SyncChannel) GetChannel() interface{} {
	return sc
}
