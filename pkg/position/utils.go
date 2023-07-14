package position

import (
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/siddontang/go-log/log"
	"os"
	"strings"
)

var DbName = "_go_mysql_sr"

func GetPositionFilePath(conf *config.BaseConfig) string {
	splits := strings.SplitAfter(*conf.FileName, "/")
	lastIndex := len(splits) - 1
	splits[lastIndex] = "_" + conf.Name + "-pos.info"
	positionFileName := strings.Join(splits, "")
	return positionFileName
}

func FindPositionFileNotCreate(filePath string, initPositionData string) {
	if _, err := os.Stat(filePath); err == nil {
		return
	}
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	defer func(f *os.File) {
		if f == nil {
			return
		}
		if err := f.Close(); err != nil {
			log.Fatalf("file close failed. err: ", err.Error())
		}
	}(f)
	if err != nil {
		log.Fatal(err)
	} else {
		_, err = f.Write([]byte(initPositionData))
		if err != nil {
			log.Fatal(err)
		}
	}
}
