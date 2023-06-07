## mysql 同步到 starrocks

![LICENSE](https://img.shields.io/badge/license-GPLv2%20-blue.svg)
![](https://img.shields.io/github/languages/top/liuxinwang/go-mysql-starrocks)
![](https://img.shields.io/badge/build-prerelease-brightgreen.svg)
[![Release](https://img.shields.io/github/release/liuxinwang/go-mysql-starrocks.svg?style=flat-square)](https://github.com/liuxinwang/go-mysql-starrocks/releases)


### 使用说明
#### 环境准备
```
1. GO构建版本 v1.18.10
2. MySQL 需要开启gtid
```
#### 1. 修改配置文件
mysql-to-starrocks.toml
```toml
# name 必填，多实例运行时需保证全局唯一
name = "mysql2starrocks"

[input]
# 指定初次监听开始的gtid点位，当_xxx-pos.info点位文件内容存在时，此选项不生效
# start-gtid = "3ba13781-44eb-2157-88a5-0dc879ec2221:1-123456"

[mysql] # mysql连接信息
host = "127.0.0.1"
port = 3306
username = "root"
password = ""

[starrocks] # starrocks连接信息
host = "127.0.0.1"
port = 8040
username = "root"
password = ""

[sync-param]
# 同步chan队列最大值，达到会进行flush，最小100
channel-size = 10240
# 同步延迟秒数，达到会进行flush，最小1
flush-delay-second = 10

#[[filter]]
#type = "delete-dml-column" # 过滤列
#[filter.config]
#match-schema = "mysql_test"
#match-table = "tb1"
#columns = ["phone"]

#[[filter]]
#type = "convert-dml-column" # 转换dml行字段类型为json，column varchar（mysql） -> column json（starrocks）
#[filter.config]
#match-schema = "test"
#match-table = "tb1"
#columns = ["varchar_json_column", "varchar_arrayjson_column"]
#cast-as = ["json", "arrayJson"] # json示例: {"id": 1, "name": 'zhangsan'}, arrayJson示例: [{"id": 1, "name": 'zhangsan'}, {"id": 1, "name": 'lisi'}]

[[rule]] # 库表同步映射1
source-schema = "mysql_test"
source-table = "tb1"
target-schema = "starrocks_test"
target-table = "tb1"

[[rule]] # 库表同步映射2
source-schema = "mysql_test"
source-table = "tb2"
target-schema = "starrocks_test"
target-table = "tb2"
```
#### 2. 启动
```shell
[sr@ ~]$ ./go-mysql-starrocks-linux-xxxxxx -config mysql-to-starrocks.toml
```
#### 3. 查看日志
默认输出到控制台

指定log-file参数运行
```shell
[sr@ ~]$ ./go-mysql-starrocks-linux-xxxxxx -config mysql-to-starrocks.toml -log-file mysql2starrocks.log
[sr@ ~]$ tail -f mysql2starrocks.log
```

#### 4. 查看帮助
```shell
[sr@ ~]$ ./go-mysql-starrocks-linux-xxxxxx -h
```
#### 5. 后台运行
```shell
[sr@ ~]$ ./go-mysql-starrocks-linux-xxxxxx -config mysql-to-starrocks.toml -log-file mysql2starrocks.log -level info -daemon
```
#### 6. 监控
集成prometheus，开放6166端口，当前支持 
```shell
curl localhost:6166/metrics
```

[使用docker部署](docs/docker%20run.md)

* * *
- - -
同时也支持mongo，详情参考[mongo sync配置](docs/mongo%20sync.md)