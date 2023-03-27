package main

import (
	"context"
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
	conf := config.NewMongoSrConfig(help.ConfigFile)
	conf.Logger = l

	// 初始化mongo client
	m := input.NewMongo(conf)

	m.OutputType = *help.OutputType

	defer func() {
		if err := m.Client.Disconnect(context.TODO()); err != nil {
			log.Fatal(err)
		}
	}()

	go m.StartChangeStream()

	select {
	case <-m.Ctx().Done():
		log.Infof("context is done with %v, closing", m.Ctx().Err())
	case <-m.Ctx().Done():
		log.Infof("context is done with %v, closing", m.Ctx().Err())
	case n := <-sc:
		log.Infof("receive signal %v, closing", n)
	}
	m.Cancel()
}
