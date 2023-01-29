package utils

import (
	"github.com/siddontang/go-log/log"
	"os"
)

func LogInit(logLevel string) *log.Logger {
	logH, _ := log.NewFileHandler("error.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND)
	l := log.NewDefault(logH)
	log.SetDefaultLogger(l)
	log.SetLevelByName(logLevel)
	return l
}
