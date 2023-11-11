#!/bin/bash

version="v0.4.3"
currentDir=$(cd $(dirname "$0") || exit; pwd)

path="github.com/go-demo/version"
buildTime=$(date +"%Y-%m-%d %H:%M:%S")
buildTimeFormat=$(date +"%Y%m%d%H%M%S")
newDir="../../bin/go-all-starrocks-$version"
flagsMac="-X $path.Version=$version -X '$path.GoVersion=$(go version)' -X '$path.BuildTime=$buildTime' -X $path.GitCommit=$(git rev-parse HEAD)"
flagsLinux="-X $path.Version=$version -X '$path.GoVersion=$(go version)' -X '$path.BuildTime=$buildTime' -X $path.GitCommit=$(git rev-parse HEAD)"

mkdir -p "$newDir"
for dbType in mysql
#也可以写成for element in ${array[*]}
do
    echo start buid $dbType
    cd "$currentDir"/cmd/go_"$dbType"_sr || exit
    # go build -ldflags "$flagsMac" -o "$newDir"/go-"$dbType"-starrocks-mac-"$buildTimeFormat"
    GOOS=linux GOARCH=amd64 go build -ldflags "$flagsLinux" -o "$newDir"/go-"$dbType"-sr-linux-"$buildTimeFormat"
    echo end buid $dbType
done
