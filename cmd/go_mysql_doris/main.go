package main

import (
	"fmt"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/channel"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/filter"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/input"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/metrics"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/output"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/position"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/rule"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/utils"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sevlyar/go-daemon"
	"github.com/siddontang/go-log/log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// 输入参数处理
	help := utils.HelpInit()
	// 日志初始化
	// _ = utils.LogInit(help)
	// 目前使用daemon的 log
	log.SetLevelByName(*help.LogLevel)
	// daemon模式启动
	if *help.Daemon {
		cntxt := &daemon.Context{
			PidFileName: utils.GetExecPath() + "/go_mysql_sr.pid",
			PidFilePerm: 0644,
			LogFileName: *help.LogFile,
			LogFilePerm: 0640,
			WorkDir:     "./",
			Umask:       027,
		}
		d, err := cntxt.Reborn()
		if err != nil {
			log.Fatal("Unable to run: ", err)
		}

		if d != nil {
			return
		}
		defer func(cntxt *daemon.Context) {
			err := cntxt.Release()
			if err != nil {
				log.Fatal("daemon release error: ", err)
			}
		}(cntxt)
	}

	// 进程信号处理
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	// Start prometheus http monitor
	go func() {
		metrics.OpsStartTime.Set(float64(time.Now().Unix()))
		log.Infof("starting http monitor on port %d.", *help.HttpPort)
		http.Handle("/metrics", promhttp.Handler())
		//TODO
		// http.HandleFunc("/api/addRule", api.AddRuleHandle(h))
		// http.HandleFunc("/api/delRule", api.DelRuleHandle(h))
		// http.HandleFunc("/api/getRule", api.GetRuleHandle(h))
		httpPortAddr := fmt.Sprintf(":%d", *help.HttpPort)
		err := http.ListenAndServe(httpPortAddr, nil)
		if err != nil {
			log.Fatalf("starting http monitor error: %v", err)
		}
	}()

	// 初始化配置
	baseConfig := config.NewBaseConfig(help.ConfigFile)

	var ci config.InputConfigInter
	var co config.OutputConfigInter
	var ii input.PluginInput
	var rr rule.Rule
	var pos position.Position
	var syncChan *channel.SyncChannel
	var outputChan *channel.OutputChannel
	var matcherFilter filter.MatcherFilter
	var oo output.PluginOutput

	// 初始化channel
	syncChan = &channel.SyncChannel{}
	syncChan.NewChannel(baseConfig.SyncParamConfig)
	outputChan = &channel.OutputChannel{}
	outputChan.NewChannel(baseConfig.SyncParamConfig)

	// 初始化output插件配置
	switch baseConfig.OutputConfig.Type {
	case "doris":
		co = &config.DorisConfig{}
		co.NewOutputTargetConfig(baseConfig.OutputConfig.Config)
		// 初始化rule配置
		rr = &rule.DorisRules{}
		rr.NewRule(baseConfig.OutputConfig.Config)
		// 初始化output插件实例
		oo = &output.Doris{}
		oo.NewOutput(co)
	}

	// 初始化input插件配置
	switch baseConfig.InputConfig.Type {
	case "mysql":
		ci = &config.MysqlConfig{}
		ci.NewInputSourceConfig(baseConfig.InputConfig.Config)
		// 初始化input插件实例
		ii = &input.MysqlInputPlugin{}
		ii.NewInput(ci, rr.GetRuleToRegex())
		pos = &position.MysqlPosition{}
		pos.LoadPosition(baseConfig)
	}

	// 初始化filter配置
	matcherFilter = filter.NewMatcherFilter(baseConfig.FilterConfig)

	// 启动input插件
	pos = ii.StartInput(pos, syncChan)
	// 启动position
	pos.StartPosition()
	// 启动filter
	matcherFilter.StartFilter(syncChan, outputChan)
	// 启动output插件
	go oo.StartOutput(outputChan, rr.GetRuleToMap())

	select {
	case n := <-sc:
		log.Infof("receive signal %v, closing", n)
		// 关闭input插件
		ii.Close()
		// 关闭filter
		syncChan.Close()
		// 关闭output插件
		oo.Close()
		// flush last position
		pos.Close()
		log.Infof("[Main] is stopped.")
	}
}
