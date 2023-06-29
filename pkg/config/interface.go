package config

type InputSourceConfig interface {
	NewInputSourceConfig(config map[string]interface{})
	GetInputSourceConfig() interface{}
}

type OutputTargetConfig interface {
	NewOutputTargetConfig(config map[string]interface{})
	GetOutputTargetConfig() interface{}
}
