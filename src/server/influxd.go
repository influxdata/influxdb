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
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"
)

const (
	version = "dev"
	gitSha  = "HEAD"
)

func waitForSignals(stopped <-chan bool) {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	for {
		sig := <-ch
		fmt.Printf("Received signal: %s\n", sig.String())
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			runtime.SetCPUProfileRate(0)
			<-stopped
			os.Exit(0)
		}
	}
}

func startProfiler(filename *string) error {
	if filename == nil || *filename == "" {
		return nil
	}

	cpuProfileFile, err := os.Create(*filename)
	if err != nil {
		return err
	}
	runtime.SetCPUProfileRate(500)
	stopped := make(chan bool)

	go waitForSignals(stopped)

	go func() {
		for {
			select {
			default:
				data := runtime.CPUProfile()
				if data == nil {
					cpuProfileFile.Close()
					stopped <- true
					break
				}
				cpuProfileFile.Write(data)
			}
		}
	}()
	return nil
}

func main() {
	fileName := flag.String("config", "config.json.sample", "Config file")
	wantsVersion := flag.Bool("v", false, "Get version number")
	resetRootPassword := flag.Bool("reset-root", false, "Reset root password")
	pidFile := flag.String("pidfile", "", "the pid file")
	cpuProfiler := flag.String("cpuprofile", "", "filename where cpu profile data will be written")

	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	startProfiler(cpuProfiler)

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

	if *resetRootPassword {
		time.Sleep(2 * time.Second) // wait for the raft server to join the cluster

		fmt.Printf("Resetting root's password to %s", coordinator.DEFAULT_ROOT_PWD)
		if err := raftServer.CreateRootUser(); err != nil {
			panic(err)
		}
	}
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
