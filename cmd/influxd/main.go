package main

import (
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
	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/coordinator"
	"github.com/jmhodges/levigo"
)

const logo = `
+---------------------------------------------+
|  _____        __ _            _____  ____   |
| |_   _|      / _| |          |  __ \|  _ \  |
|   | |  _ __ | |_| |_   ___  _| |  | | |_) | |
|   | | | '_ \|  _| | | | \ \/ / |  | |  _ <  |
|  _| |_| | | | | | | |_| |>  <| |__| | |_) | |
| |_____|_| |_|_| |_|\__,_/_/\_\_____/|____/  |
+---------------------------------------------+
`

func main() {
	if err := start(); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func start() error {
	var (
		fileName          = flag.String("config", "config.sample.toml", "Config file")
		showVersion       = flag.Bool("v", false, "Get version number")
		resetRootPassword = flag.Bool("reset-root", false, "Reset root password")
		hostname          = flag.String("hostname", "", "Override the hostname, the `hostname` config option will be overridden")
		raftPort          = flag.Int("raft-port", 0, "Override the raft port, the `raft.port` config option will be overridden")
		protobufPort      = flag.Int("protobuf-port", 0, "Override the protobuf port, the `protobuf_port` config option will be overridden")
		pidFile           = flag.String("pidfile", "", "the pid file")
		repairLeveldb     = flag.Bool("repair-ldb", false, "set to true to repair the leveldb files")
		stdout            = flag.Bool("stdout", false, "Log to stdout overriding the configuration")
		syslog            = flag.String("syslog", "", "Log to syslog facility overriding the configuration")
	)
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	c := fmt.Sprintf("InfluxDB v%s (git: %s) (leveldb: %d.%d)", version, gitSha, levigo.GetLevelDBMajorVersion(), levigo.GetLevelDBMinorVersion())
	if *showVersion {
		fmt.Println(v)
		return nil
	}

	// Parse configuration.
	config, err := ParseConfigFile(*fileName)
	if err != nil {
		return err
	}
	config.Version = v
	config.InfluxDBVersion = version

	// Override config properties.
	if *hostname != "" {
		config.Hostname = *hostname
	}
	if *raftPort != 0 {
		config.RaftServerPort = *raftPort
	}
	if *protobufPort != 0 {
		config.ProtobufPort = *protobufPort
	}
	if *syslog != "" {
		config.Logging.File = *syslog
	} else if *stdout {
		config.Logging.File = "stdout"
	}
	setupLogging(config.Logging.Level, config.Logging.File)

	// Write pid file.
	if *pidFile != "" {
		pid := strconv.Itoa(os.Getpid())
		if err := ioutil.WriteFile(*pidFile, []byte(pid), 0644); err != nil {
			panic(err)
		}
	}

	// Initialize directories.
	if err := os.MkdirAll(config.Raft.Dir, 0744); err != nil {
		panic(err)
	}
	if err := os.MkdirAll(config.Storage.Dir, 0744); err != nil {
		panic(err)
	}

	// TODO(benbjohnson): Start admin server.

	if config.BindAddress == "" {
		log.Info("Starting Influx Server %s...", version)
	} else {
		log.Info("Starting Influx Server %s bound to %s...", version, config.BindAddress)
	}
	fmt.Printf(logo)

	// Start server.
	s, err := influxdb.NewServer(config)
	if err != nil {
		// sleep for the log to flush
		time.Sleep(time.Second)
		panic(err)
	}
	if err := startProfiler(s); err != nil {
		panic(err)
	}

	if *resetRootPassword {
		// TODO: make this not suck
		// This is ghetto as hell, but it'll work for now.
		go func() {
			time.Sleep(2 * time.Second) // wait for the raft server to join the cluster
			log.Warn("Resetting root's password to %s", influxdb.DefaultRootPassword)
			if err := server.RaftServer.CreateRootUser(); err != nil {
				panic(err)
			}
		}()
	}
	if err := server.ListenAndServe(); err != nil {
		log.Error("ListenAndServe failed: ", err)
	}
	return err
}

func setupLogging(loggingLevel, logFile string) {
	level := log.DEBUG
	switch loggingLevel {
	case "trace":
		level = log.TRACE
	case "fine":
		level = log.FINE
	case "info":
		level = log.INFO
	case "warn":
		level = log.WARNING
	case "error":
		level = log.ERROR
	default:
		log.Error("Unknown log level %s. Defaulting to DEBUG", loggingLevel)
	}

	log.Global = make(map[string]*log.Filter)

	facility, ok := GetSysLogFacility(logFile)
	if ok {
		flw, err := NewSysLogWriter(facility)
		if err != nil {
			fmt.Fprintf(os.Stderr, "NewSysLogWriter: %s\n", err.Error())
			return
		}
		log.AddFilter("syslog", level, flw)
	} else if logFile == "stdout" {
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

type Stopper interface {
	Stop()
}
