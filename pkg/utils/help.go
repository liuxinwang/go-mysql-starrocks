package utils

import (
	"flag"
	"github.com/go-demo/version"
	"github.com/siddontang/go-log/log"
	"os"
	"path/filepath"
)

type Help struct {
	printVersion bool
	ConfigFile   *string
	LogLevel     *string
	LogFile      *string
	OutputType   *string
	Daemon       *bool
	HttpPort     *int
}

func HelpInit() *Help {
	var help Help
	help.ConfigFile = flag.String("config", "", "go-mysql-starrocks config file")
	help.LogLevel = flag.String("level", "info", "log level")
	help.LogFile = flag.String("log-file", "go_mysql_sr.log", "log file path")
	help.OutputType = flag.String("type", "starrocks", "output type: starrocks, output")
	help.Daemon = flag.Bool("daemon", false, "daemon run, must include param 'log-file'")
	help.HttpPort = flag.Int("http-port", 6166, "http monitor port, curl http://localhost:6166/metrics")
	flag.BoolVar(&help.printVersion, "version", false, "print program build version")
	flag.Parse()
	// 这个需要放在第一个判断
	if help.printVersion {
		version.PrintVersion()
		os.Exit(0)
	}
	if *help.ConfigFile == "" {
		log.Infof("-config param does not exist!")
		os.Exit(0)
	} else {
		abs, err := filepath.Abs(*help.ConfigFile)
		if err != nil {
			log.Fatal("-config abs error: ", err.Error())
		}
		*help.ConfigFile = abs
	}
	if *help.OutputType != "starrocks" && *help.OutputType != "output" {
		log.Infof("-type param value is wrong, see help!")
		os.Exit(0)
	}
	if *help.Daemon {
		if *help.LogFile == "" {
			log.Infof("daemon mode, must include -log-file param!")
			os.Exit(0)
		}
	}
	return &help
}
