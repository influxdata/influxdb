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
	"strings"

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
		fmt.Println(err.Error())
		os.Exit(1)
	}
	os.Exit(0)
}

// getCommandIndex returns the index of the given command, if present
// in the command-line arguments. If not present, it returns -1.
func getCommandIndex(needle string) int {
	for i, n := range flag.Args() {
		if needle == n {
			return i
		}
	}
	return -1
}

// roleBrokerRequire returns true if the role requires creating a broker, false
// otherwise.
func roleBrokerRequired(role string) bool {
	if role == "combined" || role == "broker" {
		return true
	}
	return false
}

// roleDataRequire returns true if the role requires creating a data node, false
// otherwise.
func roleDataRequired(role string) bool {
	if role == "combined" || role == "data" {
		return true
	}
	return false
}

// usage displays the help message for the user.
func usage() {
	fmt.Fprintf(os.Stderr, "\nConfigure and start the InfluxDB server.\n\n")
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] [COMMAND] [<arg>]\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "Commands:\n")
	fmt.Fprintf(os.Stderr, "  run: Start with existing cluster configuration. If none, run in local mode.\n")
	fmt.Fprintf(os.Stderr, "  create-cluster: Create a cluster that other nodes can join\n")
	fmt.Fprintf(os.Stderr, "  join-cluster <seed servers>: Prepare to join an existing cluster using seed-servers\n")
	fmt.Fprintf(os.Stderr, "  version: Display server version\n")
	fmt.Fprintf(os.Stderr, "\n If no command is specified, 'run' is the default\n")
	fmt.Fprintf(os.Stderr, "\nOptions:\n")
	flag.PrintDefaults()
}

// validateCommand ensure the command is acceptable, and includes any required
// arguments. Return nil if the command is OK, a standard error otherwise.
func validateCommand() error {
	if flag.NArg() > 0 {
		found := false
		for _, c := range []string{"create-cluster", "join-cluster", "run", "version"} {
			if flag.Arg(0) == c {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("'%s' is not a valid command", flag.Arg(0))
		}

	}
	for _, c := range []string{"create-cluster", "run", "version"} {
		if getCommandIndex(c) > -1 && flag.NArg() != 1 {
			return fmt.Errorf("'%s' must not be specified with other commands", c)
		}
	}
	if getCommandIndex("join-cluster") > -1 && flag.NArg() != 2 {
		return fmt.Errorf("'join' must be specified with at least 1 seed-server")
	}

	return nil
}

func start() error {
	// Add command-line options, and set custom "usage" function.
	var (
		fileName = flag.String("config", "config.sample.toml", "Config file")
		role     = flag.String("role", "combined", "Role for this node. Applicable only to cluster deployments")
		hostname = flag.String("hostname", "", "Override the hostname, the `hostname` config option will be overridden")
		pidFile  = flag.String("pidfile", "", "the pid file")
	)
	flag.Usage = usage

	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	// Ensure that command passed in at the command-line is valid.
	if err := validateCommand(); err != nil {
		return err
	}
	if *role != "combined" && *role != "broker" && *role != "data" {
		return fmt.Errorf("Only the roles 'combined', 'broker', and 'data' are supported")
	}

	// Check for "version" command
	v := fmt.Sprintf("InfluxDB v%s (git: %s)", version, commit)
	if getCommandIndex("version") > -1 {
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

	// Check for create-cluster request.
	if getCommandIndex("create-cluster") > -1 {
		if !roleBrokerRequired(*role) {
			return fmt.Errorf("Cluster must be created as 'combined' or 'broker' role")
		}

		// Broker required -- and it must be initialized since we're creating a cluster.
		b := messaging.NewBroker()
		if err := b.Open(config.Raft.Dir); err != nil {
			return fmt.Errorf("Failed to create cluster", err.Error())
		}
		if err := b.Initialize(); err != nil {
			return fmt.Errorf("Failed to initialize cluster", err.Error())
		}

		if roleDataRequired(*role) {
			// Do any required data node stuff.
		}
		fmt.Println("Cluster node created as", *role, "in", config.Raft.Dir)
		return nil
	}

	// Check for join-cluster request.
	j := getCommandIndex("join-cluster")
	if j > -1 {
		if roleBrokerRequired(*role) {
			// Broker required -- but don't initialize it.
			b := messaging.NewBroker()
			if err := b.Open(config.Raft.Dir); err != nil {
				return fmt.Errorf("Failed to prepare to join cluster", err.Error())
				// Join the seed servers here, using flags.Args[j+1]
			}
		} else if roleDataRequired(*role) {
			// do any required data-node stuff.
		}
		return nil
	}

	// Override config properties.
	if *hostname != "" {
		config.Hostname = *hostname
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
	for _, s := range strings.Split(*seedServers, ",") {
		u, err := url.Parse(s)
		if err != nil {
			panic(err)
		}
		brokerURLs = append(brokerURLs, u)
	}

	var client influxdb.MessagingClient
	if len(brokerURLs) > 1 {
		// Create messaging client for broker.
		c := messaging.NewClient("XXX-CHANGEME-XXX")
		if err := c.Open(brokerURLs); err != nil {
			log4go.Error("Error opening Messaging Client: %s", err.Error())
		}
		defer c.Close()
		client = c
		log4go.Info("Cluster messaging client created")
	} else {
		client = messaging.NewLoopbackClient()
		log4go.Info("Local messaging client created")
	}

	// Start server.
	s := influxdb.NewServer(client)
	err = s.Open(config.Storage.Dir)
	if err != nil {
		panic(err)
	}

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
