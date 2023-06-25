package config

type InputConfigInter interface {
	NewInputSourceConfig(config map[string]interface{})
	GetInputSourceConfig() interface{}
}

type OutputConfigInter interface {
	NewOutputTargetConfig(config map[string]interface{})
	GetOutputTargetConfig() interface{}
}
