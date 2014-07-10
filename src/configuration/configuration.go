package configuration

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	log "code.google.com/p/log4go"
	"github.com/BurntSushi/toml"
)

type Size int

const (
	ONE_MEGABYTE int64 = 1024 * 1024
	ONE_GIGABYTE       = 1024 * ONE_MEGABYTE
	// Maximum integer representable by a word (32bit or 64bit depending
	// on the architecture)
	MAX_INT = int64(^uint(0) >> 1)
)

func (d *Size) UnmarshalText(text []byte) error {
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
		return fmt.Errorf("Unknown size suffix %c", suffix)
	}
	if size > MAX_INT {
		return fmt.Errorf("Size %d cannot be represented by an int", size)
	}
	*d = Size(size)
	return nil
}

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		return nil
	}
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
	ReadTimeout duration `toml:"read-timeout"`
}

type GraphiteConfig struct {
	Enabled    bool
	Port       int
	Database   string
	UdpEnabled bool `toml:"udp_enabled"`
}

type UdpInputConfig struct {
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
	Dir                  string
	DefaultEngine        string `toml:"default-engine"`
	WriteBufferSize      int    `toml:"write-buffer-size"`
	MaxOpenShards        int    `toml:"max-open-shards"`
	PointBatchSize       int    `toml:"point-batch-size"`
	WriteBatchSize       int    `toml:"write-batch-size"`
	Engines              map[string]toml.Primitive
	RetentionSweepPeriod duration `toml:"retention-sweep-period"`
}

type ClusterConfig struct {
	SeedServers               []string `toml:"seed-servers"`
	ProtobufPort              int      `toml:"protobuf_port"`
	ProtobufTimeout           duration `toml:"protobuf_timeout"`
	ProtobufHeartbeatInterval duration `toml:"protobuf_heartbeat"`
	MinBackoff                duration `toml:"protobuf_min_backoff"`
	MaxBackoff                duration `toml:"protobuf_max_backoff"`
	WriteBufferSize           int      `toml:"write-buffer-size"`
	ConcurrentShardQueryLimit int      `toml:"concurrent-shard-query-limit"`
	MaxResponseBufferSize     int      `toml:"max-response-buffer-size"`
}

type LevelDbConfiguration struct {
	MaxOpenFiles int  `toml:"max-open-files"`
	LruCacheSize Size `toml:"lru-cache-size"`

	// global configuration, use these values if the values in
	// StorageConfig aren't set
	MaxOpenShards  int `toml:"max-open-shards"`
	PointBatchSize int `toml:"point-batch-size"`
	WriteBatchSize int `toml:"write-batch-size"`
}

type LoggingConfig struct {
	File  string
	Level string
}

type WalConfig struct {
	Dir                   string `toml:"dir"`
	FlushAfterRequests    int    `toml:"flush-after"`
	BookmarkAfterRequests int    `toml:"bookmark-after"`
	IndexAfterRequests    int    `toml:"index-after"`
	RequestsPerLogFile    int    `toml:"requests-per-log-file"`
}

type InputPlugins struct {
	Graphite        GraphiteConfig   `toml:"graphite"`
	UdpInput        UdpInputConfig   `toml:"udp"`
	UdpServersInput []UdpInputConfig `toml:"udp_servers"`
}

type TomlConfiguration struct {
	Admin             AdminConfig
	HttpApi           ApiConfig    `toml:"api"`
	InputPlugins      InputPlugins `toml:"input_plugins"`
	Raft              RaftConfig
	Storage           StorageConfig
	Cluster           ClusterConfig
	Logging           LoggingConfig
	Hostname          string
	BindAddress       string    `toml:"bind-address"`
	ReportingDisabled bool      `toml:"reporting-disabled"`
	WalConfig         WalConfig `toml:"wal"`
	LevelDb           LevelDbConfiguration
}

type Configuration struct {
	AdminHttpPort   int
	AdminAssetsDir  string
	ApiHttpSslPort  int
	ApiHttpCertPath string
	ApiHttpPort     int
	ApiReadTimeout  time.Duration

	GraphiteEnabled    bool
	GraphitePort       int
	GraphiteDatabase   string
	GraphiteUdpEnabled bool

	UdpServers []UdpInputConfig

	StorageDefaultEngine        string
	StorageMaxOpenShards        int
	StoragePointBatchSize       int
	StorageWriteBatchSize       int
	StorageEngineConfigs        map[string]toml.Primitive
	StorageRetentionSweepPeriod duration

	// TODO: this is for backward compatability only
	LevelDbMaxOpenFiles int
	LevelDbLruCacheSize int

	RaftServerPort               int
	RaftTimeout                  duration
	SeedServers                  []string
	DataDir                      string
	RaftDir                      string
	ProtobufPort                 int
	ProtobufTimeout              duration
	ProtobufHeartbeatInterval    duration
	ProtobufMinBackoff           duration
	ProtobufMaxBackoff           duration
	Hostname                     string
	LogFile                      string
	LogLevel                     string
	BindAddress                  string
	WalDir                       string
	WalFlushAfterRequests        int
	WalBookmarkAfterRequests     int
	WalIndexAfterRequests        int
	WalRequestsPerLogFile        int
	LocalStoreWriteBufferSize    int
	PerServerWriteBufferSize     int
	ClusterMaxResponseBufferSize int
	ConcurrentShardQueryLimit    int
	ReportingDisabled            bool
	Version                      string
	InfluxDBVersion              string
}

func LoadConfiguration(fileName string) *Configuration {
	log.Info("Loading configuration file %s", fileName)
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

	// if it wasn't set, set it to 100
	if tomlConfiguration.Storage.PointBatchSize == 0 {
		if s := tomlConfiguration.LevelDb.PointBatchSize; s != 0 {
			tomlConfiguration.Storage.PointBatchSize = s
		} else {
			tomlConfiguration.Storage.PointBatchSize = 100
		}
	}

	// if it wasn't set, set it to 10M
	if tomlConfiguration.Storage.WriteBatchSize == 0 {
		if s := tomlConfiguration.LevelDb.WriteBatchSize; s != 0 {
			tomlConfiguration.Storage.WriteBatchSize = s
		} else {
			tomlConfiguration.Storage.WriteBatchSize = 10 * 1024 * 1024
		}
	}

	if tomlConfiguration.Storage.MaxOpenShards == 0 {
		tomlConfiguration.Storage.MaxOpenShards = tomlConfiguration.LevelDb.MaxOpenShards
	}

	if tomlConfiguration.Storage.DefaultEngine == "" {
		tomlConfiguration.Storage.DefaultEngine = "leveldb"
	}

	if tomlConfiguration.Storage.RetentionSweepPeriod.Duration == 0 {
		tomlConfiguration.Storage.RetentionSweepPeriod = duration{10 * time.Minute}
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

	apiReadTimeout := tomlConfiguration.HttpApi.ReadTimeout.Duration
	if apiReadTimeout == 0 {
		apiReadTimeout = 5 * time.Second
	}

	if tomlConfiguration.Cluster.MinBackoff.Duration == 0 {
		tomlConfiguration.Cluster.MinBackoff = duration{time.Second}
	}

	if tomlConfiguration.Cluster.MaxBackoff.Duration == 0 {
		tomlConfiguration.Cluster.MaxBackoff = duration{10 * time.Second}
	}

	if tomlConfiguration.Cluster.ProtobufHeartbeatInterval.Duration == 0 {
		tomlConfiguration.Cluster.ProtobufHeartbeatInterval = duration{10 * time.Millisecond}
	}

	config := &Configuration{
		AdminHttpPort:   tomlConfiguration.Admin.Port,
		AdminAssetsDir:  tomlConfiguration.Admin.Assets,
		ApiHttpPort:     tomlConfiguration.HttpApi.Port,
		ApiHttpCertPath: tomlConfiguration.HttpApi.SslCertPath,
		ApiHttpSslPort:  tomlConfiguration.HttpApi.SslPort,
		ApiReadTimeout:  apiReadTimeout,

		GraphiteEnabled:    tomlConfiguration.InputPlugins.Graphite.Enabled,
		GraphitePort:       tomlConfiguration.InputPlugins.Graphite.Port,
		GraphiteDatabase:   tomlConfiguration.InputPlugins.Graphite.Database,
		GraphiteUdpEnabled: tomlConfiguration.InputPlugins.Graphite.UdpEnabled,

		UdpServers: tomlConfiguration.InputPlugins.UdpServersInput,

		// storage configuration
		StorageDefaultEngine:        tomlConfiguration.Storage.DefaultEngine,
		StorageMaxOpenShards:        tomlConfiguration.Storage.MaxOpenShards,
		StoragePointBatchSize:       tomlConfiguration.Storage.PointBatchSize,
		StorageWriteBatchSize:       tomlConfiguration.Storage.WriteBatchSize,
		DataDir:                     tomlConfiguration.Storage.Dir,
		LocalStoreWriteBufferSize:   tomlConfiguration.Storage.WriteBufferSize,
		StorageEngineConfigs:        tomlConfiguration.Storage.Engines,
		StorageRetentionSweepPeriod: tomlConfiguration.Storage.RetentionSweepPeriod,

		LevelDbMaxOpenFiles: tomlConfiguration.LevelDb.MaxOpenFiles,
		LevelDbLruCacheSize: int(tomlConfiguration.LevelDb.LruCacheSize),

		RaftServerPort:               tomlConfiguration.Raft.Port,
		RaftTimeout:                  tomlConfiguration.Raft.Timeout,
		RaftDir:                      tomlConfiguration.Raft.Dir,
		ProtobufPort:                 tomlConfiguration.Cluster.ProtobufPort,
		ProtobufTimeout:              tomlConfiguration.Cluster.ProtobufTimeout,
		ProtobufHeartbeatInterval:    tomlConfiguration.Cluster.ProtobufHeartbeatInterval,
		ProtobufMinBackoff:           tomlConfiguration.Cluster.MinBackoff,
		ProtobufMaxBackoff:           tomlConfiguration.Cluster.MaxBackoff,
		SeedServers:                  tomlConfiguration.Cluster.SeedServers,
		LogFile:                      tomlConfiguration.Logging.File,
		LogLevel:                     tomlConfiguration.Logging.Level,
		Hostname:                     tomlConfiguration.Hostname,
		BindAddress:                  tomlConfiguration.BindAddress,
		ReportingDisabled:            tomlConfiguration.ReportingDisabled,
		WalDir:                       tomlConfiguration.WalConfig.Dir,
		WalFlushAfterRequests:        tomlConfiguration.WalConfig.FlushAfterRequests,
		WalBookmarkAfterRequests:     tomlConfiguration.WalConfig.BookmarkAfterRequests,
		WalIndexAfterRequests:        tomlConfiguration.WalConfig.IndexAfterRequests,
		WalRequestsPerLogFile:        tomlConfiguration.WalConfig.RequestsPerLogFile,
		PerServerWriteBufferSize:     tomlConfiguration.Cluster.WriteBufferSize,
		ClusterMaxResponseBufferSize: tomlConfiguration.Cluster.MaxResponseBufferSize,
		ConcurrentShardQueryLimit:    defaultConcurrentShardQueryLimit,
	}

	config.UdpServers = append(config.UdpServers, UdpInputConfig{
		Enabled:  tomlConfiguration.InputPlugins.UdpInput.Enabled,
		Database: tomlConfiguration.InputPlugins.UdpInput.Database,
		Port:     tomlConfiguration.InputPlugins.UdpInput.Port,
	})

	if config.LocalStoreWriteBufferSize == 0 {
		config.LocalStoreWriteBufferSize = 1000
	}
	if config.PerServerWriteBufferSize == 0 {
		config.PerServerWriteBufferSize = 1000
	}

	if config.ClusterMaxResponseBufferSize == 0 {
		config.ClusterMaxResponseBufferSize = 100
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

func (self *Configuration) UdpInputPortString(port int) string {
	if port <= 0 {
		return ""
	}

	return fmt.Sprintf("%s:%d", self.BindAddress, port)
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

func (self *Configuration) RaftConnectionString() string {
	return fmt.Sprintf("http://%s:%d", self.HostnameOrDetect(), self.RaftServerPort)
}

func (self *Configuration) ProtobufListenString() string {
	return fmt.Sprintf("%s:%d", self.BindAddress, self.ProtobufPort)
}

func (self *Configuration) RaftListenString() string {
	return fmt.Sprintf("%s:%d", self.BindAddress, self.RaftServerPort)
}
