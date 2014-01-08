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
	Port int
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

type TomlConfiguration struct {
	Admin    AdminConfig
	Api      ApiConfig
	Raft     RaftConfig
	Storage  StorageConfig
	Cluster  ClusterConfig
	Logging  LoggingConfig
	Hostname string
}

type Configuration struct {
	AdminHttpPort  int
	AdminAssetsDir string
	ApiHttpPort    int
	RaftServerPort int
	SeedServers    []string
	DataDir        string
	RaftDir        string
	ProtobufPort   int
	Hostname       string
	LogFile        string
	LogLevel       string
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
		AdminHttpPort:  tomlConfiguration.Admin.Port,
		AdminAssetsDir: tomlConfiguration.Admin.Assets,
		ApiHttpPort:    tomlConfiguration.Api.Port,
		RaftServerPort: tomlConfiguration.Raft.Port,
		RaftDir:        tomlConfiguration.Raft.Dir,
		ProtobufPort:   tomlConfiguration.Cluster.ProtobufPort,
		SeedServers:    tomlConfiguration.Cluster.SeedServers,
		DataDir:        tomlConfiguration.Storage.Dir,
		LogFile:        tomlConfiguration.Logging.File,
		LogLevel:       tomlConfiguration.Logging.Level,
		Hostname:       tomlConfiguration.Hostname,
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
	return fmt.Sprintf(":%d", self.AdminHttpPort)
}

func (self *Configuration) ApiHttpPortString() string {
	return fmt.Sprintf(":%d", self.ApiHttpPort)
}

func (self *Configuration) ProtobufPortString() string {
	return fmt.Sprintf(":%d", self.ProtobufPort)
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
