package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"

	"code.google.com/p/log4go"
	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/messaging"
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

// These variables are populated via the Go linker.
var (
	version string
	commit  string
)

func main() {
	if err := start(); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func start() error {
	var (
		fileName     = flag.String("config", "config.sample.toml", "Config file")
		showVersion  = flag.Bool("v", false, "Get version number")
		hostname     = flag.String("hostname", "", "Override the hostname, the `hostname` config option will be overridden")
		protobufPort = flag.Int("protobuf-port", 0, "Override the protobuf port, the `protobuf_port` config option will be overridden")
		pidFile      = flag.String("pidfile", "", "the pid file")
		stdout       = flag.Bool("stdout", false, "Log to stdout overriding the configuration")
		syslog       = flag.String("syslog", "", "Log to syslog facility overriding the configuration")
	)
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	v := fmt.Sprintf("InfluxDB v%s (git: %s)", version, commit)
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
	if *protobufPort != 0 {
		config.Cluster.ProtobufPort = *protobufPort
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
	if err := os.MkdirAll(config.Storage.Dir, 0744); err != nil {
		panic(err)
	}

	// TODO(benbjohnson): Start admin server.

	if config.BindAddress == "" {
		log4go.Info("Starting Influx Server %s...", version)
	} else {
		log4go.Info("Starting Influx Server %s bound to %s...", version, config.BindAddress)
	}
	fmt.Printf(logo)

	// Parse broker URLs from seed servers.
	var brokerURLs []*url.URL
	for _, s := range config.Cluster.SeedServers {
		u, err := url.Parse(s)
		if err != nil {
			panic(err)
		}
		brokerURLs = append(brokerURLs, u)
	}

	// Create messaging client for broker.
	client := messaging.NewClient("XXX-CHANGEME-XXX")
	if err := client.Open(brokerURLs); err != nil {
		log4go.Error("Error opening Messaging Client: %s", err.Error())
	}

	// Start server.
	s := influxdb.NewServer(client)

	// TODO: startProfiler()
	// TODO: -reset-root

	// Initialize HTTP handler.
	h := influxdb.NewHandler(s)

	// Start HTTP server.
	func() { log.Fatal(http.ListenAndServe(":8086", h)) }() // TODO: Change HTTP port.
	// TODO: Start HTTPS server.

	// Wait indefinitely.
	<-(chan struct{})(nil)
	return nil
}

func setupLogging(loggingLevel, logFile string) {
	level := log4go.DEBUG
	switch loggingLevel {
	case "trace":
		level = log4go.TRACE
	case "fine":
		level = log4go.FINE
	case "info":
		level = log4go.INFO
	case "warn":
		level = log4go.WARNING
	case "error":
		level = log4go.ERROR
	default:
		log4go.Error("Unknown log level %s. Defaulting to DEBUG", loggingLevel)
	}

	log4go.Global = make(map[string]*log4go.Filter)

	facility, ok := GetSysLogFacility(logFile)
	if ok {
		flw, err := NewSysLogWriter(facility)
		if err != nil {
			fmt.Fprintf(os.Stderr, "NewSysLogWriter: %s\n", err.Error())
			return
		}
		log4go.AddFilter("syslog", level, flw)
	} else if logFile == "stdout" {
		flw := log4go.NewConsoleLogWriter()
		log4go.AddFilter("stdout", level, flw)
	} else {
		logFileDir := filepath.Dir(logFile)
		os.MkdirAll(logFileDir, 0744)

		flw := log4go.NewFileLogWriter(logFile, false)
		log4go.AddFilter("file", level, flw)

		flw.SetFormat("[%D %T] [%L] (%S) %M")
		flw.SetRotate(true)
		flw.SetRotateSize(0)
		flw.SetRotateLines(0)
		flw.SetRotateDaily(true)
	}

	log4go.Info("Redirectoring logging to %s", logFile)
}

type Stopper interface {
	Stop()
}
