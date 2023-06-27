package channel

import "github.com/liuxinwang/go-mysql-starrocks/pkg/config"

type Channel interface {
	NewChannel(config *config.SyncParamConfig)
	GetChannel() interface{}
	Close()
}
