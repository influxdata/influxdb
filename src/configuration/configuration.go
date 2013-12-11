package configuration

import (
	log "code.google.com/p/log4go"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
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
}

func LoadConfiguration(fileName string) *Configuration {
	log.Info("Loading Config from " + fileName)
	config := &Configuration{}

	data, err := ioutil.ReadFile(fileName)
	if err == nil {
		err = json.Unmarshal(data, config)
		if err != nil {
			log.Error("Couldn't parse configuration file: " + fileName)
			panic(err)
		}
	} else {
		log.Error("Couldn't load configuration file: " + fileName)
		panic(err)
	}

	return config
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
