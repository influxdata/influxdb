package configuration

import (
	log "code.google.com/p/log4go"
	"encoding/json"
	"fmt"
	"github.com/BurntSushi/toml"
	"io/ioutil"
	"os"
)

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
	Dir string
}

type ClusterConfig struct {
	SeedServers  []string `toml:"seed-servers"`
	ProtobufPort int      `toml:"protobuf_port"`
}

type LoggingConfig struct {
	File  string
	Level string
}

type LevelDbConfiguration struct {
	MaxOpenFiles int `toml:"max-open-files"`
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
	BindAddress string `toml:"bind-address"`
}

type Configuration struct {
	AdminHttpPort       int
	AdminAssetsDir      string
	ApiHttpSslPort      int
	ApiHttpCertPath     string
	ApiHttpPort         int
	RaftServerPort      int
	SeedServers         []string
	DataDir             string
	RaftDir             string
	ProtobufPort        int
	Hostname            string
	LogFile             string
	LogLevel            string
	BindAddress         string
	LevelDbMaxOpenFiles int
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

	config := &Configuration{
		AdminHttpPort:       tomlConfiguration.Admin.Port,
		AdminAssetsDir:      tomlConfiguration.Admin.Assets,
		ApiHttpPort:         tomlConfiguration.Api.Port,
		ApiHttpCertPath:     tomlConfiguration.Api.SslCertPath,
		ApiHttpSslPort:      tomlConfiguration.Api.SslPort,
		RaftServerPort:      tomlConfiguration.Raft.Port,
		RaftDir:             tomlConfiguration.Raft.Dir,
		ProtobufPort:        tomlConfiguration.Cluster.ProtobufPort,
		SeedServers:         tomlConfiguration.Cluster.SeedServers,
		DataDir:             tomlConfiguration.Storage.Dir,
		LogFile:             tomlConfiguration.Logging.File,
		LogLevel:            tomlConfiguration.Logging.Level,
		Hostname:            tomlConfiguration.Hostname,
		BindAddress:         tomlConfiguration.BindAddress,
		LevelDbMaxOpenFiles: tomlConfiguration.LevelDb.MaxOpenFiles,
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
