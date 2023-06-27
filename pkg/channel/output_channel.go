package channel

import (
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
)

type OutputChannel struct {
	SyncChan               chan interface{}
	ChannelSize            int
	FLushCHanMaxWaitSecond int
	Done                   chan struct{}
}

func (oc *OutputChannel) NewChannel(config *config.SyncParamConfig) {
	oc.SyncChan = make(chan interface{}, config.ChannelSize)
	oc.ChannelSize = config.ChannelSize
	oc.FLushCHanMaxWaitSecond = config.FlushDelaySecond
	oc.Done = make(chan struct{})
}

func (oc *OutputChannel) GetChannel() interface{} {
	return oc
}

func (oc *OutputChannel) Close() {
	close(oc.Done)
}
