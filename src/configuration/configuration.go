package configuration

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

type Configuration struct {
	AdminHttpPort  string
	AdminAssetsDir string
	ApiHttpPort    string
	RaftServerPort int
	SeedServers    []string
	DataDir        string
	RaftDir        string
}

func LoadConfiguration(fileName string) *Configuration {
	log.Println("Loading Config from " + fileName)
	config := &Configuration{}

	data, err := ioutil.ReadFile(fileName)
	if err == nil {
		err = json.Unmarshal(data, config)
		if err != nil {
			log.Println("Couldn't parse configuration file: " + fileName)
			panic(err)
		}
	} else {
		log.Println("Couldn't load configuration file: " + fileName)
		panic(err)
	}

	return config
}
