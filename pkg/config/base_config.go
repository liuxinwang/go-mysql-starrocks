package config

type Mysql struct {
	Host     string
	Port     int
	UserName string
	Password string
}

type Mongo struct {
	Uri      string
	UserName string
	Password string
}

type Starrocks struct {
	Host     string
	Port     int
	UserName string
	Password string
}

type SyncParam struct {
	ChannelSize      int `toml:"channel-size"`
	FlushDelaySecond int `toml:"flush-delay-second"`
}
