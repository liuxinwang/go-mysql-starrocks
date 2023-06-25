package config

import (
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
)

func (dc *DorisConfig) NewOutputTargetConfig(config map[string]interface{}) {
	var target = config["target"]
	err := mapstructure.Decode(target, dc)
	if err != nil {
		log.Fatal("output.target config parsing failed. err: %v", err.Error())
	}
}

func (dc *DorisConfig) GetOutputTargetConfig() interface{} {
	return dc
}
