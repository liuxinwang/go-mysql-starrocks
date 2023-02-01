package main

import (
	"github.com/siddontang/go-log/log"
	"go-mysql-starrocks/pkg/config"
	"go-mysql-starrocks/pkg/input"
	"go-mysql-starrocks/pkg/utils"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// 输入参数处理
	help := utils.HelpInit()
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

	// Start canal
	go c.StartFromGTID(h.AckGTIDSet())

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
