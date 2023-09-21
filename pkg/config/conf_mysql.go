package config

import (
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
)

func (mc *MysqlConfig) NewInputSourceConfig(config map[string]interface{}) {
	var source = config["source"]
	err := mapstructure.Decode(source, mc)
	if err != nil {
		log.Fatal("input.source config parsing failed. err: %v", err.Error())
	}
}

func (mc *MysqlConfig) GetInputSourceConfig() interface{} {
	return mc
}

func (mc *MysqlConfig) NewOutputTargetConfig(config map[string]interface{}) {
	var target = config["target"]
	err := mapstructure.Decode(target, mc)
	if err != nil {
		log.Fatal("output.target config parsing failed. err: %v", err.Error())
	}
}

func (mc *MysqlConfig) GetOutputTargetConfig() interface{} {
	return mc
}
