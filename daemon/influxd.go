package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/coordinator"
	"github.com/influxdb/influxdb/server"
	"github.com/jmhodges/levigo"
)

func setupLogging(loggingLevel, logFile string) {
	level := log.DEBUG
	switch loggingLevel {
	case "info":
		level = log.INFO
	case "warn":
		level = log.WARNING
	case "error":
		level = log.ERROR
	}

	log.Global = make(map[string]*log.Filter)

	if logFile == "stdout" {
		flw := log.NewConsoleLogWriter()
		log.AddFilter("stdout", level, flw)

	} else {
		logFileDir := filepath.Dir(logFile)
		os.MkdirAll(logFileDir, 0744)

		flw := log.NewFileLogWriter(logFile, false)
		log.AddFilter("file", level, flw)

		flw.SetFormat("[%D %T] [%L] (%S) %M")
		flw.SetRotate(true)
		flw.SetRotateSize(0)
		flw.SetRotateLines(0)
		flw.SetRotateDaily(true)
	}

	log.Info("Redirectoring logging to %s", logFile)
}

func main() {
	fileName := flag.String("config", "config.sample.toml", "Config file")
	wantsVersion := flag.Bool("v", false, "Get version number")
	resetRootPassword := flag.Bool("reset-root", false, "Reset root password")
	hostname := flag.String("hostname", "", "Override the hostname, the `hostname` config option will be overridden")
	raftPort := flag.Int("raft-port", 0, "Override the raft port, the `raft.port` config option will be overridden")
	protobufPort := flag.Int("protobuf-port", 0, "Override the protobuf port, the `protobuf_port` config option will be overridden")
	pidFile := flag.String("pidfile", "", "the pid file")
	repairLeveldb := flag.Bool("repair-ldb", false, "set to true to repair the leveldb files")
	loadDatabaseConfig := flag.String("load-database-config", "", "Will create databases with the given shard spaces from a file.")
	loadServer := flag.String("load-server", "localhost:8086", "If loading a database config, connects to this host/port")
	loadUser := flag.String("load-user", "root", "If loading a database config, uses this user to auth")
	loadPassword := flag.String("load-password", "root", "If loading a database config, use this password to auth")
	stdout := flag.Bool("stdout", false, "Log to stdout overriding the configuration")

	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	v := fmt.Sprintf("InfluxDB v%s (git: %s) (leveldb: %d.%d)", version, gitSha, levigo.GetLevelDBMajorVersion(), levigo.GetLevelDBMinorVersion())
	if wantsVersion != nil && *wantsVersion {
		fmt.Println(v)
		return
	}
	if *loadDatabaseConfig != "" {
		err := LoadDatabaseConfig(*loadDatabaseConfig, *loadServer, *loadUser, *loadPassword)
		if err != nil {
			panic(err)
		}
		return
	}
	config := configuration.LoadConfiguration(*fileName)

	// override the hostname if it was specified on the command line
	if hostname != nil && *hostname != "" {
		config.Hostname = *hostname
	}

	if raftPort != nil && *raftPort != 0 {
		config.RaftServerPort = *raftPort
	}

	if protobufPort != nil && *protobufPort != 0 {
		config.ProtobufPort = *protobufPort
	}

	config.Version = v
	config.InfluxDBVersion = version

	if *stdout {
		config.LogFile = "stdout"
	}
	setupLogging(config.LogLevel, config.LogFile)

	if *repairLeveldb {
		log.Info("Repairing leveldb")
		files, err := ioutil.ReadDir(config.DataDir)
		if err != nil {
			panic(err)
		}
		o := levigo.NewOptions()
		defer o.Close()
		for _, f := range files {
			p := path.Join(config.DataDir, f.Name())
			log.Info("Repairing %s", p)
			if err := levigo.RepairDatabase(p, o); err != nil {
				panic(err)
			}
		}
	}

	if pidFile != nil && *pidFile != "" {
		pid := strconv.Itoa(os.Getpid())
		if err := ioutil.WriteFile(*pidFile, []byte(pid), 0644); err != nil {
			panic(err)
		}
	}

	if config.BindAddress == "" {
		log.Info("Starting Influx Server %s...", version)
	} else {
		log.Info("Starting Influx Server %s bound to %s...", version, config.BindAddress)
	}
	fmt.Printf(`
+---------------------------------------------+
|  _____        __ _            _____  ____   |
| |_   _|      / _| |          |  __ \|  _ \  |
|   | |  _ __ | |_| |_   ___  _| |  | | |_) | |
|   | | | '_ \|  _| | | | \ \/ / |  | |  _ <  |
|  _| |_| | | | | | | |_| |>  <| |__| | |_) | |
| |_____|_| |_|_| |_|\__,_/_/\_\_____/|____/  |
+---------------------------------------------+

`)
	os.MkdirAll(config.RaftDir, 0744)
	os.MkdirAll(config.DataDir, 0744)
	server, err := server.NewServer(config)
	if err != nil {
		// sleep for the log to flush
		time.Sleep(time.Second)
		panic(err)
	}

	if err := startProfiler(server); err != nil {
		panic(err)
	}

	if *resetRootPassword {
		// TODO: make this not suck
		// This is ghetto as hell, but it'll work for now.
		go func() {
			time.Sleep(2 * time.Second) // wait for the raft server to join the cluster

			log.Warn("Resetting root's password to %s", coordinator.DEFAULT_ROOT_PWD)
			if err := server.RaftServer.CreateRootUser(); err != nil {
				panic(err)
			}
		}()
	}
	err = server.ListenAndServe()
	if err != nil {
		log.Error("ListenAndServe failed: ", err)
	}
}

type DatabaseConfig struct {
	Database string               `json:"database"`
	Spaces   []*client.ShardSpace `json:"spaces"`
}

func LoadDatabaseConfig(fileName, server, user, password string) error {
	fmt.Println("Loading config from ", fileName)
	c, err := client.NewClient(&client.ClientConfig{
		Host:     server,
		Username: user,
		Password: password,
	})
	if err != nil {
		return err
	}
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}
	configs := []*DatabaseConfig{}
	err = json.Unmarshal(content, &configs)
	if err != nil {
		return err
	}
	for _, config := range configs {
		err := c.CreateDatabase(config.Database)
		if err != nil {
			return err
		}
		for _, space := range config.Spaces {
			space.Database = config.Database
			err := c.CreateShardSpace(space)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
