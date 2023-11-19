package app

import (
	"github.com/juju/errors"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/channel"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/core"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/filter"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/registry"
	"sync"
)

type Server struct {
	Input         core.Input
	InputPosition core.Position
	InputSchema   core.Schema
	Output        core.Output
	OutPutRule    core.Rule
	SyncChan      *channel.SyncChannel
	outputChan    *channel.OutputChannel
	matcherFilter filter.MatcherFilter
	sync.Mutex
}

func NewServer(config *config.BaseConfig) (*Server, error) {
	server := Server{}

	// output
	plugin, err := registry.GetPlugin(registry.OutputPlugin, config.OutputConfig.Type)
	if err != nil {
		return nil, errors.Trace(err)
	}
	output, ok := plugin.(core.Output)
	if !ok {
		return nil, errors.Errorf("not a valid output plugin: %v", config.OutputConfig.Type)
	}
	server.Output = output
	err = plugin.Configure(config.OutputConfig.Type, config.OutputConfig.Config)
	if err != nil {
		return nil, err
	}

	// output rule
	plugin, err = registry.GetPlugin(registry.OutputRulePlugin, config.OutputConfig.Type)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rule, ok := plugin.(core.Rule)
	if !ok {
		return nil, errors.Errorf("not a valid output rule plugin: %v", config.OutputConfig.Type)
	}
	server.OutPutRule = rule
	err = plugin.Configure(config.OutputConfig.Type, config.OutputConfig.Config)
	if err != nil {
		return nil, err
	}

	// input
	plugin, err = registry.GetPlugin(registry.InputPlugin, config.InputConfig.Type)
	if err != nil {
		return nil, errors.Trace(err)
	}
	input, ok := plugin.(core.Input)
	if !ok {
		return nil, errors.Errorf("not a valid input type")
	}
	server.Input = input
	err = plugin.Configure(config.InputConfig.Type, config.InputConfig.Config)
	if err != nil {
		return nil, err
	}

	// input position
	plugin, err = registry.GetPlugin(registry.InputPositionPlugin, config.OutputConfig.Type)
	if err != nil {
		return nil, errors.Trace(err)
	}
	position, ok := plugin.(core.Position)
	if !ok {
		return nil, errors.Errorf("not a valid input position plugin: %v", config.OutputConfig.Type)
	}
	server.InputPosition = position

	// input schema
	plugin, err = registry.GetPlugin(registry.InputSchemaPlugin, config.OutputConfig.Type)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schema, ok := plugin.(core.Schema)
	if !ok {
		return nil, errors.Errorf("not a valid input schema plugin: %v", config.OutputConfig.Type)
	}
	server.InputSchema = schema

	// 加载position
	positionData := position.LoadPosition(config)
	// 初始化schema
	schema.NewSchemaTables(config, config.InputConfig.Config, positionData, server.OutPutRule.GetRuleToMap())
	// 初始化output
	server.Output.NewOutput(nil, server.OutPutRule.GetRuleToMap(), server.InputSchema)
	// 初始化input
	server.Input.NewInput(nil, server.OutPutRule.GetRuleToRegex(), server.InputSchema)
	// 初始化channel
	server.SyncChan = &channel.SyncChannel{}
	server.SyncChan.NewChannel(config.SyncParamConfig)
	server.outputChan = &channel.OutputChannel{}
	server.outputChan.NewChannel(config.SyncParamConfig)
	// 初始化filter配置
	server.matcherFilter = filter.NewMatcherFilter(config.FilterConfig)

	return &server, nil
}

func (s *Server) Start() error {
	s.Lock()
	defer s.Unlock()

	// 启动input插件
	s.InputPosition = s.Input.StartInput(s.InputPosition, s.SyncChan)
	// 启动position
	s.InputPosition.StartPosition()
	// 启动filter
	s.matcherFilter.StartFilter(s.SyncChan, s.outputChan, s.InputSchema)
	// 启动output插件
	go s.Output.StartOutput(s.outputChan)

	return nil
}
