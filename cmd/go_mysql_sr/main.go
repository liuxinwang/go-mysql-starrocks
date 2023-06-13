package main

import (
	"fmt"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/input"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/metrics"
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
	// daemon模式启动
	if *help.Daemon {
		cntxt := &daemon.Context{
			PidFileName: utils.GetExecPath() + "/go_mysql_sr.pid",
			PidFilePerm: 0644,
			LogFileName: utils.GetExecPath() + "/go_mysql_sr.log",
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

	// 日志初始化
	l := utils.LogInit(help)

	// 进程信号处理
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	// 初始化配置
	conf := config.NewMysqlSrConfig(help.ConfigFile)
	conf.Logger = l

	// 初始化mysql canal
	h := input.NewMysql(conf)
	c := h.C()

	// Start prometheus http monitor
	go func() {
		metrics.OpsStartTime.Set(float64(time.Now().Unix()))
		log.Infof("starting http monitor on port %d.", *help.HttpPort)
		http.Handle("/metrics", promhttp.Handler())
		httpPortAddr := fmt.Sprintf(":%d", *help.HttpPort)
		err := http.ListenAndServe(httpPortAddr, nil)
		if err != nil {
			log.Fatalf("starting http monitor error: %v", err)
		}
	}()

	// Start canal
	go func() {
		err := c.StartFromGTID(h.AckGTIDSet())
		if err != nil {
			log.Fatal(err)
		}
	}()

	select {
	case <-c.Ctx().Done():
		log.Infof("context is done with %v, closing", c.Ctx().Err())
	case <-h.Ctx().Done():
		log.Infof("context is done with %v, closing", h.Ctx().Err())
	case n := <-sc:
		log.Infof("receive signal %v, closing", n)
	}
	h.Cancel()
	c.Close()
}
