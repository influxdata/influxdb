package main

import (
	"admin"
	"api/http"
	"configuration"
	"coordinator"
	"datastore"
	"engine"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
)

var fileName = flag.String("config", "config.json.sample", "Config file")
var wantsVersion = flag.Bool("version", false, "Get version number")

var version = "dev"

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	if wantsVersion != nil && *wantsVersion {
		fmt.Printf("InfluxDB v%s\n", version)
		return
	}
	config := configuration.LoadConfiguration(*fileName)

	log.Println("Starting Influx Server...")
	ringSize := int64(1000)
	clusterConfig := coordinator.NewClusterConfiguration(ringSize)
	os.MkdirAll(config.RaftDir, 0744)

	raftServer := coordinator.NewRaftServer(config.RaftDir, "localhost", config.RaftServerPort, clusterConfig)
	go func() {
		raftServer.ListenAndServe(config.SeedServers, false)
	}()
	os.MkdirAll(config.DataDir, 0744)
	log.Println("Opening database at ", config.DataDir)
	db, err := datastore.NewLevelDbDatastore(config.DataDir)
	if err != nil {
		panic(err)
	}
	coord := coordinator.NewCoordinatorImpl(db, raftServer, clusterConfig)
	eng, err := engine.NewQueryEngine(coord)
	if err != nil {
		panic(err)
	}
	log.Println()
	adminServer := admin.NewHttpServer(config.AdminAssetsDir, config.AdminHttpPortString())
	log.Println("Starting admin interface on port", config.AdminHttpPort)
	go func() {
		adminServer.ListenAndServe()
	}()
	log.Println("Starting Http Api server on port", config.ApiHttpPort)
	server := http.NewHttpServer(config.ApiHttpPortString(), eng, coord, coord)
	server.ListenAndServe()
}
