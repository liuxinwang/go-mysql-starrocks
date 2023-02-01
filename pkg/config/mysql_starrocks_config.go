package config

import (
	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"go-mysql-starrocks/pkg/rule"
	"os"
	"path/filepath"
)

type Mysql struct {
	Host     string
	Port     int
	UserName string
	Password string
}

type Starrocks struct {
	Host     string
	Port     int
	UserName string
	Password string
}

type MysqlSrConfig struct {
	Name       string
	Mysql      *Mysql
	Starrocks  *Starrocks
	Rules      []*rule.MysqlToSrRule `toml:"rule"`
	Logger     *log.Logger
	ConfigFile string
}

func (config *MysqlSrConfig) ReadMysqlSrConf(filename string) (*MysqlSrConfig, error) {
	var err error
	if _, err = toml.DecodeFile(filename, &config); err != nil {
		return nil, errors.Trace(err)
	}
	return config, err
}

func NewMysqlSrConfig(configFile *string) *MysqlSrConfig {
	c := &MysqlSrConfig{}
	fileName, err := filepath.Abs(*configFile)
	if err != nil {
		log.Fatal(err)
	}
	c, err = c.ReadMysqlSrConf(fileName)
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
