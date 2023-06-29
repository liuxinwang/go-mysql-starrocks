package config

import (
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
)

func (sc *StarrocksConfig) NewOutputTargetConfig(config map[string]interface{}) {
	var target = config["target"]
	err := mapstructure.Decode(target, sc)
	if err != nil {
		log.Fatal("output.target config parsing failed. err: %v", err.Error())
	}
}

func (sc *StarrocksConfig) GetOutputTargetConfig() interface{} {
	return sc
}
