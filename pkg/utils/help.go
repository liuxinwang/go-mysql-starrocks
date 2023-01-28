package utils

import (
	"flag"
	"github.com/go-demo/version"
	"os"
)

type Help struct {
	printVersion bool
	ConfigFile   *string
	LogLevel     *string
	OutputType   *string
}

func HelpInit() Help {
	var help Help
	help.ConfigFile = flag.String("config", "starrocks.toml", "go-mysql-starrocks config file")
	help.LogLevel = flag.String("level", "info", "log level")
	help.OutputType = flag.String("type", "starrocks", "output type: starrocks, output")

	flag.BoolVar(&help.printVersion, "version", false, "print program build version")
	flag.Parse()
	if help.printVersion {
		version.PrintVersion()
		os.Exit(0)
	}
	return help
}
