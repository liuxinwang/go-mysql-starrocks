package utils

import (
	"flag"
	"github.com/go-demo/version"
	"github.com/siddontang/go-log/log"
	"os"
)

type Help struct {
	printVersion bool
	ConfigFile   *string
	LogLevel     *string
	LogFile      *string
	OutputType   *string
}

func HelpInit() *Help {
	var help Help
	help.ConfigFile = flag.String("config", "", "go-mysql-starrocks config file")
	help.LogLevel = flag.String("level", "info", "log level")
	help.LogFile = flag.String("log-file", "", "log file path")
	help.OutputType = flag.String("type", "starrocks", "output type: starrocks, output")

	flag.BoolVar(&help.printVersion, "version", false, "print program build version")
	flag.Parse()
	// 这个需要放在第一个判断
	if help.printVersion {
		version.PrintVersion()
		os.Exit(0)
	}
	if *help.ConfigFile == "" {
		log.Infof("-config param does not exist")
		os.Exit(0)
	}
	return &help
}
