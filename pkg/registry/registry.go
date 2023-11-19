package registry

import (
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"sync"
)

type PluginType string

const (
	InputPlugin         PluginType = "input"
	InputPositionPlugin PluginType = "inputPosition"
	InputSchemaPlugin   PluginType = "inputSchema"
	OutputPlugin        PluginType = "output"
	OutputRulePlugin    PluginType = "outputRule"
)

type Plugin interface {
	Configure(pipelineName string, data map[string]interface{}) error
}

var registry map[PluginType]Plugin
var mutex sync.Mutex

func init() {
	registry = make(map[PluginType]Plugin)
}

func RegisterPlugin(pluginType PluginType, name string, v Plugin) {
	mutex.Lock()
	defer mutex.Unlock()

	log.Debugf("[RegisterPlugin] type: %v, name: %v", pluginType, name)

	_, ok := registry[pluginType]
	if !ok {
		registry[pluginType] = v
	}
}

func GetPlugin(pluginType PluginType, name string) (Plugin, error) {
	mutex.Lock()
	defer mutex.Unlock()

	if registry == nil {
		return nil, errors.Errorf("empty registry")
	}

	plugin, ok := registry[pluginType]
	if !ok {
		return nil, errors.Errorf("empty plugin type: %v, name: %v", pluginType, name)
	}
	return plugin, nil
}
