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
	"time"
)

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

type RaftConfig struct {
	Port int
	Dir  string
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
}

type LoggingConfig struct {
	File  string
	Level string
}

type LevelDbConfiguration struct {
	MaxOpenFiles int `toml:"max-open-files"`
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

type TomlConfiguration struct {
	Admin       AdminConfig
	Api         ApiConfig
	Raft        RaftConfig
	Storage     StorageConfig
	Cluster     ClusterConfig
	Logging     LoggingConfig
	LevelDb     LevelDbConfiguration
	Hostname    string
	BindAddress string             `toml:"bind-address"`
	Sharding    ShardingDefinition `toml:"sharding"`
	WalConfig   WalConfig          `toml:"wal"`
}

type Configuration struct {
	AdminHttpPort             int
	AdminAssetsDir            string
	ApiHttpSslPort            int
	ApiHttpCertPath           string
	ApiHttpPort               int
	RaftServerPort            int
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

	config := &Configuration{
		AdminHttpPort:             tomlConfiguration.Admin.Port,
		AdminAssetsDir:            tomlConfiguration.Admin.Assets,
		ApiHttpPort:               tomlConfiguration.Api.Port,
		ApiHttpCertPath:           tomlConfiguration.Api.SslCertPath,
		ApiHttpSslPort:            tomlConfiguration.Api.SslPort,
		RaftServerPort:            tomlConfiguration.Raft.Port,
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
