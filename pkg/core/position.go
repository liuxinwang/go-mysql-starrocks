package core

import "github.com/liuxinwang/go-mysql-starrocks/pkg/config"

type Position interface {
	LoadPosition(config *config.BaseConfig) string
	SavePosition() error
	ModifyPosition(v string) error
	StartPosition()
	Close()
}
