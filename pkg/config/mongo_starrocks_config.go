package config

import (
	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/rule"
	"github.com/siddontang/go-log/log"
	"os"
	"path/filepath"
	"time"
)

type MongoSrConfig struct {
	Name       string
	Input      *Input
	Mongo      *MongoConfig
	Starrocks  *StarrocksConfig
	Filter     []*FilterConfig
	Rules      []*rule.MongoToSrRule `toml:"rule"`
	Logger     *log.Logger
	ConfigFile string
	OutputType string
	SyncParam  *SyncParamConfig `toml:"sync-param"`
}

type Input struct {
	StartPosition    time.Time `toml:"start-position"`
	ConvertSnakeCase bool      `toml:"convert-snake-case"`
}

func (config *MongoSrConfig) ReadMongoSrConf(filename string) (*MongoSrConfig, error) {
	var err error
	if _, err = toml.DecodeFile(filename, &config); err != nil {
		return nil, errors.Trace(err)
	}
	return config, err
}

func NewMongoSrConfig(configFile *string) *MongoSrConfig {
	c := &MongoSrConfig{}
	fileName, err := filepath.Abs(*configFile)
	if err != nil {
		log.Fatal(err)
	}
	c, err = c.ReadMongoSrConf(fileName)
	if err != nil {
		log.Fatal(err)
	}
	if c.Name == "" {
		log.Errorf("The configuration file \"name\" variable cannot be empty")
		os.Exit(0)
	}
	if c.SyncParam == nil {
		log.Errorf("The configuration file \"[sync-param]\" variable cannot be empty")
		os.Exit(0)
	}
	if c.SyncParam.ChannelSize < 100 {
		log.Warnf("The [sync-param] configuration parameter \"channel-size\" should not be less than 100, and reset configured channel-size = 100")
		c.SyncParam.ChannelSize = 100
	}
	if c.SyncParam.FlushDelaySecond < 1 {
		log.Warnf("The [sync-param] configuration parameter \"flush-delay-second\" should not be less than 1, and reset configured flush-delay-second = 1")
		c.SyncParam.FlushDelaySecond = 1
	}
	c.ConfigFile = fileName
	if err != nil {
		log.Fatal(err)
	}
	return c
}
