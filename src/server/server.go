package main

import (
	"admin"
	"api/http"
	"configuration"
	"coordinator"
	"datastore"
	"engine"
	"flag"
	"log"
	"os"
	"runtime"
)

var fileName = flag.String("config", "config.json.sample", "Config file")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

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
	adminServer := admin.NewHttpServer(config.AdminAssetsDir, config.AdminHttpPort)
	log.Println("Starting admin interface on port ", config.AdminHttpPort)
	go func() {
		adminServer.ListenAndServe()
	}()
	log.Println("Starting Http Api server on port ", config.ApiHttpPort)
	server := http.NewHttpServer(config.ApiHttpPort, eng, coord)
	server.ListenAndServe()
}
