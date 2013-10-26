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
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
)

var fileName = flag.String("config", "config.json.sample", "Config file")
var wantsVersion = flag.Bool("version", false, "Get version number")
var pidFile = flag.String("pidfile", "/opt/influxdb/shared/influxdb.pid", "the pid file")

var version = "dev"
var gitSha = ""

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	if wantsVersion != nil && *wantsVersion {
		fmt.Printf("InfluxDB v%s (git: %s)\n", version, gitSha)
		return
	}
	config := configuration.LoadConfiguration(*fileName)

	if pidFile != nil && *pidFile != "" {
		pid := strconv.Itoa(os.Getpid())
		if err := ioutil.WriteFile(*pidFile, []byte(pid), 0644); err != nil {
			panic(err)
		}
	}

	log.Println("Starting Influx Server...")
	clusterConfig := coordinator.NewClusterConfiguration()
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
