package schema

import (
	"github.com/liuxinwang/go-mysql-starrocks/pkg/config"
	"github.com/siddontang/go-log/log"
	"os"
	"strings"
)

func GetMetaFilePath(conf *config.BaseConfig) string {
	splits := strings.SplitAfter(*conf.FileName, "/")
	lastIndex := len(splits) - 1
	splits[lastIndex] = "_" + conf.Name + "-meta.info"
	metaFileName := strings.Join(splits, "")
	return metaFileName
}

func FindMetaFileNotCreate(filePath string, initData string) {
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
		_, err = f.Write([]byte(initData))
		if err != nil {
			log.Fatal(err)
		}
	}
}
