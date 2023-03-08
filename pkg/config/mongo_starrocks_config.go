package config

import (
	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"go-mysql-starrocks/pkg/rule"
	"os"
	"path/filepath"
)

type MongoSrConfig struct {
	Name       string
	Mongo      *Mongo
	Starrocks  *Starrocks
	Filter     []*Filter
	Rules      []*rule.MongoToSrRule `toml:"rule"`
	Logger     *log.Logger
	ConfigFile string
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
		log.Infof("The configuration file \"name\" variable cannot be empty")
		os.Exit(0)
	}
	c.ConfigFile = fileName
	if err != nil {
		log.Fatal(err)
	}
	return c
}
