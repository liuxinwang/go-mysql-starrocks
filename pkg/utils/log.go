package utils

import (
	"github.com/siddontang/go-log/log"
	"os"
)

func LogInit(help *Help) *log.Logger {
	var l *log.Logger
	if *help.LogFile != "" {
		// 写入文件
		logH, _ := log.NewFileHandler(*help.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND)
		l = log.NewDefault(logH)
	} else {
		// 输出到控制台
		logH, _ := log.NewStreamHandler(os.Stdout)
		l = log.NewDefault(logH)
	}
	log.SetDefaultLogger(l)
	log.SetLevelByName(*help.LogLevel)
	return l
}
