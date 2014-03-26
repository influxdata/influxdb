package configuration

import (
	log "code.google.com/p/log4go"
	"common"
	"encoding/json"
	"fmt"
	"github.com/BurntSushi/toml"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"time"
)

type size struct {
	int
}

const (
	ONE_MEGABYTE = 1024 * 1024
	ONE_GIGABYTE = 1024 * ONE_MEGABYTE
)

func (d *size) UnmarshalText(text []byte) error {
	str := string(text)
	length := len(str)
	size, err := strconv.ParseInt(string(text[:length-1]), 10, 64)
	if err != nil {
		return err
	}
	switch suffix := text[len(text)-1]; suffix {
	case 'm':
		size *= ONE_MEGABYTE
	case 'g':
		size *= ONE_GIGABYTE
	default:
		return fmt.Errorf("Unknown size suffix %s", suffix)
	}
	d.int = int(size)
	return nil
}

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

type AdminConfig struct {
	Port   int
	Assets string
}

type ApiConfig struct {
	SslPort     int    `toml:"ssl-port"`
	SslCertPath string `toml:"ssl-cert"`
	Port        int
}

type GraphiteConfig struct {
	Enabled  bool
	Port     int
	Database string
}

type RaftConfig struct {
	Port    int
	Dir     string
	Timeout duration `toml:"election-timeout"`
}

type StorageConfig struct {
	Dir             string
	WriteBufferSize int `toml:"write-buffer-size"`
}

type ClusterConfig struct {
	SeedServers               []string `toml:"seed-servers"`
	ProtobufPort              int      `toml:"protobuf_port"`
	ProtobufTimeout           duration `toml:"protobuf_timeout"`
	ProtobufHeartbeatInterval duration `toml:"protobuf_heartbeat"`
	WriteBufferSize           int      `toml"write-buffer-size"`
	ConcurrentShardQueryLimit int      `toml:"concurrent-shard-query-limit"`
}

type LoggingConfig struct {
	File  string
	Level string
}

type LevelDbConfiguration struct {
	MaxOpenFiles   int  `toml:"max-open-files"`
	LruCacheSize   size `toml:"lru-cache-size"`
	MaxOpenShards  int  `toml:"max-open-shards"`
	PointBatchSize int  `toml:"point-batch-size"`
}

type ShardingDefinition struct {
	ReplicationFactor int                `toml:"replication-factor"`
	ShortTerm         ShardConfiguration `toml:"short-term"`
	LongTerm          ShardConfiguration `toml:"long-term"`
}

type ShardConfiguration struct {
	Duration         string
	parsedDuration   time.Duration
	Split            int
	SplitRandom      string `toml:"split-random"`
	splitRandomRegex *regexp.Regexp
	hasRandomSplit   bool
}

func (self *ShardConfiguration) ParseAndValidate(defaultShardDuration time.Duration) error {
	var err error
	if self.Split == 0 {
		self.Split = 1
	}
	if self.SplitRandom == "" {
		self.hasRandomSplit = false
	} else {
		self.hasRandomSplit = true
		self.splitRandomRegex, err = regexp.Compile(self.SplitRandom)
		if err != nil {
			return err
		}
	}
	if self.Duration == "" {
		self.parsedDuration = defaultShardDuration
		return nil
	}
	val, err := common.ParseTimeDuration(self.Duration)
	if err != nil {
		return err
	}
	self.parsedDuration = time.Duration(val)
	return nil
}

func (self *ShardConfiguration) ParsedDuration() *time.Duration {
	return &self.parsedDuration
}

func (self *ShardConfiguration) HasRandomSplit() bool {
	return self.hasRandomSplit
}

func (self *ShardConfiguration) SplitRegex() *regexp.Regexp {
	return self.splitRandomRegex
}

type WalConfig struct {
	Dir                   string `toml:"dir"`
	FlushAfterRequests    int    `toml:"flush-after"`
	BookmarkAfterRequests int    `toml:"bookmark-after"`
	IndexAfterRequests    int    `toml:"index-after"`
	RequestsPerLogFile    int    `toml:"requests-per-log-file"`
}

type InputPlugins struct {
	Graphite GraphiteConfig `toml:"graphite"`
}

type TomlConfiguration struct {
	Admin        AdminConfig
	HttpApi      ApiConfig    `toml:"api"`
	InputPlugins InputPlugins `toml:"input_plugins"`
	Raft         RaftConfig
	Storage      StorageConfig
	Cluster      ClusterConfig
	Logging      LoggingConfig
	LevelDb      LevelDbConfiguration
	Hostname     string
	BindAddress  string             `toml:"bind-address"`
	Sharding     ShardingDefinition `toml:"sharding"`
	WalConfig    WalConfig          `toml:"wal"`
}

type Configuration struct {
	AdminHttpPort             int
	AdminAssetsDir            string
	ApiHttpSslPort            int
	ApiHttpCertPath           string
	ApiHttpPort               int
	GraphiteEnabled           bool
	GraphitePort              int
	GraphiteDatabase          string
	RaftServerPort            int
	RaftTimeout               duration
	SeedServers               []string
	DataDir                   string
	RaftDir                   string
	ProtobufPort              int
	ProtobufTimeout           duration
	ProtobufHeartbeatInterval duration
	Hostname                  string
	LogFile                   string
	LogLevel                  string
	BindAddress               string
	LevelDbMaxOpenFiles       int
	LevelDbLruCacheSize       int
	LevelDbMaxOpenShards      int
	LevelDbPointBatchSize     int
	ShortTermShard            *ShardConfiguration
	LongTermShard             *ShardConfiguration
	ReplicationFactor         int
	WalDir                    string
	WalFlushAfterRequests     int
	WalBookmarkAfterRequests  int
	WalIndexAfterRequests     int
	WalRequestsPerLogFile     int
	LocalStoreWriteBufferSize int
	PerServerWriteBufferSize  int
	ConcurrentShardQueryLimit int
}

func LoadConfiguration(fileName string) *Configuration {
	config, err := parseTomlConfiguration(fileName)
	if err != nil {
		log.Error("Couldn't parse configuration file: " + fileName)
		panic(err)
	}
	return config
}

func parseTomlConfiguration(filename string) (*Configuration, error) {
	body, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	tomlConfiguration := &TomlConfiguration{}
	_, err = toml.Decode(string(body), tomlConfiguration)
	if err != nil {
		return nil, err
	}
	err = tomlConfiguration.Sharding.LongTerm.ParseAndValidate(time.Hour * 24 * 30)
	if err != nil {
		return nil, err
	}
	err = tomlConfiguration.Sharding.ShortTerm.ParseAndValidate(time.Hour * 24 * 7)
	if err != nil {
		return nil, err
	}

	if tomlConfiguration.WalConfig.IndexAfterRequests == 0 {
		tomlConfiguration.WalConfig.IndexAfterRequests = 1000
	}

	if tomlConfiguration.WalConfig.RequestsPerLogFile == 0 {
		tomlConfiguration.WalConfig.RequestsPerLogFile = 10 * tomlConfiguration.WalConfig.IndexAfterRequests
	}

	defaultConcurrentShardQueryLimit := 10
	if tomlConfiguration.Cluster.ConcurrentShardQueryLimit != 0 {
		defaultConcurrentShardQueryLimit = tomlConfiguration.Cluster.ConcurrentShardQueryLimit
	}

	if tomlConfiguration.Raft.Timeout.Duration == 0 {
		tomlConfiguration.Raft.Timeout = duration{time.Second}
	}

	config := &Configuration{
		AdminHttpPort:             tomlConfiguration.Admin.Port,
		AdminAssetsDir:            tomlConfiguration.Admin.Assets,
		ApiHttpPort:               tomlConfiguration.HttpApi.Port,
		ApiHttpCertPath:           tomlConfiguration.HttpApi.SslCertPath,
		ApiHttpSslPort:            tomlConfiguration.HttpApi.SslPort,
		GraphiteEnabled:           tomlConfiguration.InputPlugins.Graphite.Enabled,
		GraphitePort:              tomlConfiguration.InputPlugins.Graphite.Port,
		GraphiteDatabase:          tomlConfiguration.InputPlugins.Graphite.Database,
		RaftServerPort:            tomlConfiguration.Raft.Port,
		RaftTimeout:               tomlConfiguration.Raft.Timeout,
		RaftDir:                   tomlConfiguration.Raft.Dir,
		ProtobufPort:              tomlConfiguration.Cluster.ProtobufPort,
		ProtobufTimeout:           tomlConfiguration.Cluster.ProtobufTimeout,
		ProtobufHeartbeatInterval: tomlConfiguration.Cluster.ProtobufHeartbeatInterval,
		SeedServers:               tomlConfiguration.Cluster.SeedServers,
		DataDir:                   tomlConfiguration.Storage.Dir,
		LogFile:                   tomlConfiguration.Logging.File,
		LogLevel:                  tomlConfiguration.Logging.Level,
		Hostname:                  tomlConfiguration.Hostname,
		BindAddress:               tomlConfiguration.BindAddress,
		LevelDbMaxOpenFiles:       tomlConfiguration.LevelDb.MaxOpenFiles,
		LevelDbLruCacheSize:       tomlConfiguration.LevelDb.LruCacheSize.int,
		LevelDbMaxOpenShards:      tomlConfiguration.LevelDb.MaxOpenShards,
		LongTermShard:             &tomlConfiguration.Sharding.LongTerm,
		ShortTermShard:            &tomlConfiguration.Sharding.ShortTerm,
		ReplicationFactor:         tomlConfiguration.Sharding.ReplicationFactor,
		WalDir:                    tomlConfiguration.WalConfig.Dir,
		WalFlushAfterRequests:     tomlConfiguration.WalConfig.FlushAfterRequests,
		WalBookmarkAfterRequests:  tomlConfiguration.WalConfig.BookmarkAfterRequests,
		WalIndexAfterRequests:     tomlConfiguration.WalConfig.IndexAfterRequests,
		WalRequestsPerLogFile:     tomlConfiguration.WalConfig.RequestsPerLogFile,
		LocalStoreWriteBufferSize: tomlConfiguration.Storage.WriteBufferSize,
		PerServerWriteBufferSize:  tomlConfiguration.Cluster.WriteBufferSize,
		ConcurrentShardQueryLimit: defaultConcurrentShardQueryLimit,
	}

	if config.LocalStoreWriteBufferSize == 0 {
		config.LocalStoreWriteBufferSize = 1000
	}
	if config.PerServerWriteBufferSize == 0 {
		config.PerServerWriteBufferSize = 1000
	}

	// if it wasn't set, set it to 100
	if config.LevelDbMaxOpenFiles == 0 {
		config.LevelDbMaxOpenFiles = 100
	}

	// if it wasn't set, set it to 200 MB
	if config.LevelDbLruCacheSize == 0 {
		config.LevelDbLruCacheSize = 200 * ONE_MEGABYTE
	}

	// if it wasn't set, set it to 100
	if config.LevelDbPointBatchSize == 0 {
		config.LevelDbPointBatchSize = 100
	}

	return config, nil
}

func parseJsonConfiguration(fileName string) (*Configuration, error) {
	log.Info("Loading Config from " + fileName)
	config := &Configuration{}

	data, err := ioutil.ReadFile(fileName)
	if err == nil {
		err = json.Unmarshal(data, config)
		if err != nil {
			return nil, err
		}
	} else {
		log.Error("Couldn't load configuration file: " + fileName)
		panic(err)
	}

	return config, nil
}

func (self *Configuration) AdminHttpPortString() string {
	if self.AdminHttpPort <= 0 {
		return ""
	}

	return fmt.Sprintf("%s:%d", self.BindAddress, self.AdminHttpPort)
}

func (self *Configuration) ApiHttpPortString() string {
	if self.ApiHttpPort <= 0 {
		return ""
	}

	return fmt.Sprintf("%s:%d", self.BindAddress, self.ApiHttpPort)
}

func (self *Configuration) ApiHttpSslPortString() string {
	return fmt.Sprintf("%s:%d", self.BindAddress, self.ApiHttpSslPort)
}

func (self *Configuration) GraphitePortString() string {
	if self.GraphitePort <= 0 {
		return ""
	}

	return fmt.Sprintf("%s:%d", self.BindAddress, self.GraphitePort)
}

func (self *Configuration) ProtobufPortString() string {
	return fmt.Sprintf("%s:%d", self.BindAddress, self.ProtobufPort)
}

func (self *Configuration) HostnameOrDetect() string {
	if self.Hostname != "" {
		return self.Hostname
	} else {
		n, err := os.Hostname()
		if err == nil {
			return n
		} else {
			return "localhost"
		}
	}
}

func (self *Configuration) ProtobufConnectionString() string {
	return fmt.Sprintf("%s:%d", self.HostnameOrDetect(), self.ProtobufPort)
}
