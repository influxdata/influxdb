package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
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

const modeFilename = "mode.json"

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

// createCluster creates a new node, ready to be joined by other nodes. It is also
// responsible for displaying help for this command.
func createCluster(args []string, config *Config) error {
	var (
		clusterFlags = flag.NewFlagSet("cluster", flag.ExitOnError)
		role         = clusterFlags.String("role", "combined", "Role for this node, must be 'combined' or 'broker'.")
	)
	clusterFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "\nUsage: %s create-cluster [options]\n\n", os.Args[0])
		clusterFlags.PrintDefaults()
	}
	clusterFlags.Parse(args)

	if *role != "combined" && *role != "broker" {
		return fmt.Errorf("New cluster node must be created as 'combined' or 'broker' role")
	}

	// Broker required -- and it must be initialized since we're creating a cluster.
	b := messaging.NewBroker()
	if err := b.Open(config.Raft.Dir); err != nil {
		return fmt.Errorf("Failed to create cluster", err.Error())
	}
	if err := b.Initialize(); err != nil {
		return fmt.Errorf("Failed to initialize cluster", err.Error())
	}

	if *role == "combined" {
		// Do any required data node stuff.
	}
	fmt.Println("New cluster node created as", *role, "in", config.Raft.Dir)
	return nil
}

// joinCluster creates a new node, ready to be joined by other nodes. It is also
// responsible for displaying help for this command.
func joinCluster(args []string, config *Config) error {
	var (
		joinFlags = flag.NewFlagSet("join", flag.ExitOnError)
		role      = joinFlags.String("role", "combined", "Role for this node, must be 'combined', 'broker', or 'data'.")
	)
	joinFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "\nUsage: %s join-cluster <servers> [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, " servers: Comma-separated list of servers, for joining existing cluster, in form host:port\n")
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
		joinFlags.PrintDefaults()
	}
	joinFlags.Parse(args)

	if *role != "combined" && *role != "broker" && *role != "data" {
		return fmt.Errorf("Node must join as 'combined', 'broker', or 'data'")
	}

	if joinFlags.NArg() < 1 {
		return fmt.Errorf("'join-cluster' requires seed servers")
	}

	if *role == "combined" || *role == "broker" {
		// Broker required -- but don't initialize it. Joining a cluster will
		// do that.
		b := messaging.NewBroker()
		if err := b.Open(config.Raft.Dir); err != nil {
			return fmt.Errorf("Failed to prepare to join cluster", err.Error())
		}
	}

	if *role == "combined" || *role == "data" {
		// do any required data-node stuff.
	}
	fmt.Println("Joined cluster at", joinFlags.Arg(0))
	return nil
}

func run(args []string, config *Config) error {
	var (
		runFlags = flag.NewFlagSet("run", flag.ExitOnError)
		pidFile  = runFlags.String("pidfile", "", "the pid file")
	)

	setupLogging(config.Logging.Level, config.Logging.File)

	// Write pid file.
	if *pidFile != "" {
		pid := strconv.Itoa(os.Getpid())
		if err := ioutil.WriteFile(*pidFile, []byte(pid), 0644); err != nil {
			panic(err)
		}
	}

	// TODO(benbjohnson): Start admin server.

	if config.BindAddress == "" {
		log4go.Info("Starting Influx Server %s...", version)
	} else {
		log4go.Info("Starting Influx Server %s bound to %s...", version, config.BindAddress)
	}
	fmt.Printf(logo)

	// Bring up the node in the state as is on disk.
	var (
		brokerURLs []*url.URL
		client     influxdb.MessagingClient
		server     *influxdb.Server
	)

	state, err := createStateIfNotExists(filepath.Join(config.Cluster.Dir, modeFilename))
	if err != nil {
		return err
	}

	if state.Mode == "local" {
		client = messaging.NewLoopbackClient()
		log4go.Info("Local messaging client created")

		server = influxdb.NewServer(client)
		err := server.Open(config.Storage.Dir)
		if err != nil {
			return fmt.Errorf("failed to open local Server", err.Error())
		}
	} else {
		// If the Broker directory exists, open a Broker on this node.
		if _, err := os.Stat(config.Raft.Dir); err == nil {
			b := messaging.NewBroker()
			if err := b.Open(config.Raft.Dir); err != nil {
				return fmt.Errorf("failed to open Broker", err.Error())
			}
		} else {
			return fmt.Errorf("failed to check for Broker directory", err.Error())
		}

		// If a Data directory exists, open a Data node.
		if _, err := os.Stat(config.Storage.Dir); err == nil {
			// Create correct client here for connecting to Broker.
			c := messaging.NewClient("XXX-CHANGEME-XXX")
			if err := c.Open(brokerURLs); err != nil {
				log4go.Error("Error opening Messaging Client: %s", err.Error())
			}
			defer c.Close()
			client = c
			log4go.Info("Cluster messaging client created")

			server = influxdb.NewServer(client)
			err = server.Open(config.Storage.Dir)
			if err != nil {
				return fmt.Errorf("failed to open data Server", err.Error())
			}
		} else {
			return fmt.Errorf("failed to check for Broker directory", err.Error())
		}

		// TODO: startProfiler()
		// TODO: -reset-root

		// Start up HTTP server with correct endpoints. TODO
		h := influxdb.NewHandler(server)

		func() { log.Fatal(http.ListenAndServe(":8086", h)) }() // TODO: Change HTTP port.
		// TODO: Start HTTPS server.
	}

	// Wait indefinitely.
	<-(chan struct{})(nil)

	return nil
}

func start() error {
	generalFlags := flag.NewFlagSet("general", flag.ExitOnError)
	fileName := generalFlags.String("config", "config.sample.toml", "Configuration file")
	hostname := generalFlags.String("hostname", "", "Override the hostname, the `hostname` config option will be overridden")
	generalFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "\nConfigure and start the InfluxDB server.\n\n")
		fmt.Fprintf(os.Stderr, "Usage: %s [options] [[command] [arguments]]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  run:            Start with existing cluster configuration. If none, run in local mode.\n")
		fmt.Fprintf(os.Stderr, "  create-cluster: Create a new node that other nodes can join to form a new cluster.\n")
		fmt.Fprintf(os.Stderr, "  join-cluster:   Prepare a new node to join a cluster.\n")
		fmt.Fprintf(os.Stderr, "  version:        Display server version.\n")
		fmt.Fprintf(os.Stderr, "\n If no command is specified, 'run' is the default.\n")
		fmt.Fprintf(os.Stderr, "\n Use \"%s [command] -h\" for more information about a command.\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
		generalFlags.PrintDefaults()
	}

	generalFlags.Parse(os.Args[1:])

	config, err := ParseConfigFile(*fileName)
	if err != nil {
		return err
	}

	// Override config properties.
	if *hostname != "" {
		config.Hostname = *hostname
	}

	// Ensure always-required directories exist.
	os.MkdirAll(config.Cluster.Dir, 0744)

	var cmd string
	var args []string
	if generalFlags.NArg() == 0 {
		cmd = "run"
	} else {
		// There is an explicit command.
		cmd = generalFlags.Arg(0)
		if generalFlags.NArg() > 1 {
			// And it has arguments
			args = generalFlags.Args()[1:]
		}
	}

	switch cmd {
	case "run":
		return run(args, config)
	case "create-cluster":
		return createCluster(args, config)
	case "join-cluster":
		return joinCluster(args, config)
	case "version":
		v := fmt.Sprintf("InfluxDB v%s (git: %s)", version, commit)
		fmt.Println(v)
		return nil
	default:
		return fmt.Errorf("unknown command: %s. '%s -h' for usage", cmd, flag.Arg(0))
	}
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

// createStateIfNotExists returns the cluster state, from the file at path. If no file exists
// at path, the default state is created, and written to the path.
func createStateIfNotExists(path string) (State, error) {
	var s State
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
			s.Mode = "local"

			g, err := os.Create(path)
			if err != nil {
				return s, err
			}
			enc := json.NewEncoder(g)
			err = enc.Encode(&s)
			g.Close()
		}
		return s, err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	err = dec.Decode(&s)
	return s, err
}

type Stopper interface {
	Stop()
}

type State struct {
	Mode string `json:"mode"`
}
