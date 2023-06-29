package config

import (
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
)

func (mc *MongoConfig) NewInputSourceConfig(config map[string]interface{}) {
	var source = config["source"]
	err := mapstructure.Decode(source, mc)
	if err != nil {
		log.Fatal("input.source config parsing failed. err: %v", err.Error())
	}
}

func (mc *MongoConfig) GetInputSourceConfig() interface{} {
	return mc
}
