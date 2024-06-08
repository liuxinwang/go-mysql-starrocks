package main

import (
	"fmt"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/api"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/app"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/metrics"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/schema"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/utils"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sevlyar/go-daemon"
	"github.com/siddontang/go-log/log"
	"net/http"
	_ "net/http/pprof"
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

	if help.MetaDbPort != nil {
		schema.MemDbPort = *help.MetaDbPort
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
		log.Infof("starting http on port %d.", *help.HttpPort)
		http.Handle("/metrics", promhttp.Handler())
		httpPortAddr := fmt.Sprintf(":%d", *help.HttpPort)
		err := http.ListenAndServe(httpPortAddr, nil)
		if err != nil {
			log.Fatalf("starting http monitor error: %v", err)
		}
	}()

	// 初始化配置
	baseConfig := config.NewBaseConfig(help.ConfigFile)

	s, err := app.NewServer(baseConfig)
	if err != nil {
		log.Fatalf("%v", err.Error())
	}

	err = s.Start()
	if err != nil {
		log.Fatalf("%v", err.Error())
	}

	// api handle
	http.HandleFunc("/api/addRule", api.AddRuleHandle(s.Input, s.Output, s.InputSchema))
	http.HandleFunc("/api/delRule", api.DelRuleHandle(s.Input, s.Output, s.InputSchema))
	http.HandleFunc("/api/getRule", api.GetRuleHandle(s.Output))
	http.HandleFunc("/api/pause", api.PauseHandle(s.Output))
	http.HandleFunc("/api/resume", api.ResumeHandle(s.Output))

	select {
	case n := <-sc:
		log.Infof("receive signal %v, closing", n)
		// 关闭input插件
		s.Input.Close()
		// 关闭filter
		s.SyncChan.Close()
		// 关闭output插件
		s.Output.Close()
		// flush last position
		s.InputPosition.Close()
		// close schema conn
		s.InputSchema.Close()
		log.Infof("[Main] is stopped.")
	}
}
