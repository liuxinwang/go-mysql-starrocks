package config

import (
	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"path/filepath"
)

type BaseConfig struct {
	Name            string
	InputConfig     *InputConfig     `toml:"input"`
	OutputConfig    *OutputConfig    `toml:"output"`
	SyncParamConfig *SyncParamConfig `toml:"sync-param"`
	FilterConfig    []*FilterConfig  `toml:"filter"`
	FileName        *string
}

type MysqlConfig struct {
	Host     string
	Port     int
	UserName string
	Password string
}

type MongoConfig struct {
	Uri      string
	UserName string
	Password string
}

type StarrocksConfig struct {
	Host     string
	Port     int
	LoadPort int `mapstructure:"load-port"`
	UserName string
	Password string
}

type DorisConfig struct {
	Host     string
	Port     int
	LoadPort int `mapstructure:"load-port"`
	UserName string
	Password string
}

type SyncParamConfig struct {
	ChannelSize      int `toml:"channel-size"`
	FlushDelaySecond int `toml:"flush-delay-second"`
}

type FilterConfig struct {
	Type   string                 `toml:"type"`
	Config map[string]interface{} `toml:"config"`
}

type InputConfig struct {
	Type          string                 `toml:"type"`
	StartPosition string                 `toml:"start-position"`
	Config        map[string]interface{} `toml:"config"`
}

type OutputConfig struct {
	Type   string                 `toml:"type"`
	Config map[string]interface{} `toml:"config"`
}

func NewBaseConfig(fileName *string) *BaseConfig {
	var bc = &BaseConfig{}
	fileNamePath, err := filepath.Abs(*fileName)
	if err != nil {
		log.Fatal(err)
	}
	bc.FileName = &fileNamePath
	err = bc.ReadBaseConfig()
	if err != nil {
		log.Fatal(err)
	}
	return bc
}

func (bc *BaseConfig) ReadBaseConfig() error {
	var err error
	if _, err = toml.DecodeFile(*bc.FileName, bc); err != nil {
		return errors.Trace(err)
	}
	return err
}
