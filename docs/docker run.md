### 使用Dockerfile构建

#### 1. git clone
```shell
[sr@ ~]$ git clone https://github.com/liuxinwang/go-mysql-starrocks.git
```
#### 2. docker build
```shell
[sr@ ~]$ cd go-mysql-starrocks
[sr@ ~]$ docker build --no-cache --tag go-mysql-sr .
```
#### 3. docker中使用本地配置文件，编辑配置文件starrocks.toml
```shell
[sr@ ~]$ cd configs
[sr@ ~]$ mkdir go-mysql-sr
[sr@ ~]$ cp mysql-to-starrocks-sample.toml go-mysql-sr/starrocks.toml
[sr@ ~]$ vim go-mysql-sr/starrocks.toml
```
#### 4. docker run start，替换${path}为本地配置文件绝对路径
```shell
[sr@ ~]$ docker run -itd -p 6166:6166 --name go-mysql-sr -v ${path}/go-mysql-starrocks/configs/go-mysql-sr/:/etc/go-mysql-sr/ go-mysql-sr
```