package configuration

import (
	log "code.google.com/p/log4go"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/stvp/go-toml-config"
	"io/ioutil"
	"os"
	"strings"
)

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
	configSet := config.NewConfigSet("influxdb", flag.ContinueOnError)
	adminPort := configSet.Int("admin.port", 8083)
	adminAssetsDir := configSet.String("admin.assets", "./admin")
	apiHttpPort := configSet.Int("api.port", 8086)
	raftPort := configSet.Int("raft.port", 8090)
	raftDir := configSet.String("raft.dir", "/tmp/influxdb/development/raft")
	seedServers := configSet.String("cluster.seed-servers", "")
	dataDir := configSet.String("storage.dir", "/tmp/influxdb/development/db")
	protobufPort := configSet.Int("cluster.protobuf_port", 8099)
	logFile := configSet.String("logging.file", "influxdb.log")
	logLevel := configSet.String("logging.level", "info")
	hostname := configSet.String("hostname", "")

	if err := configSet.Parse(filename); err != nil {
		return nil, err
	}

	config := &Configuration{
		AdminHttpPort:  *adminPort,
		AdminAssetsDir: *adminAssetsDir,
		ApiHttpPort:    *apiHttpPort,
		RaftServerPort: *raftPort,
		RaftDir:        *raftDir,
		ProtobufPort:   *protobufPort,
		DataDir:        *dataDir,
		LogFile:        *logFile,
		LogLevel:       *logLevel,
		Hostname:       *hostname,
	}

	servers := strings.Split(*seedServers, ",")
	for _, server := range servers {
		server = strings.TrimSpace(server)
		if server == "" {
			continue
		}
		if !strings.HasPrefix(server, "http://") {
			server = "http://" + server
		}
		config.SeedServers = append(config.SeedServers, server)
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
