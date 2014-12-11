package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/messaging"
)

// stateFilename represents the name of the file to store node state.
const stateFilename = "state"

// execRun runs the "run" command.
func execRun(args []string) {
	// Parse command flags.
	fs := flag.NewFlagSet("", flag.ExitOnError)
	var (
		configPath = fs.String("config", "config.sample.toml", "")
		pidPath    = fs.String("pidfile", "", "")
		hostname   = fs.String("hostname", "", "")
	)
	fs.Usage = printRunUsage
	fs.Parse(args)

	// Write pid file.
	if *pidPath != "" {
		pid := strconv.Itoa(os.Getpid())
		if err := ioutil.WriteFile(*pidPath, []byte(pid), 0644); err != nil {
			log.Fatal(err)
		}
	}

	// Parse configuration.
	config, err := ParseConfigFile(*configPath)
	if err != nil {
		log.Fatalf("config: %s", err)
	}

	// Override config properties.
	if *hostname != "" {
		config.Hostname = *hostname
	}

	// TODO(benbjohnson): Start admin server.

	log.Print(logo)
	if config.BindAddress == "" {
		log.Printf("Starting Influx Server %s...", version)
	} else {
		log.Printf("Starting Influx Server %s bound to %s...", version, config.BindAddress)
	}

	// Bring up the node in the state as is on disk.
	var brokerURLs []*url.URL

	// Read state from disk.
	state, err := createStateIfNotExists(filepath.Join(config.Cluster.Dir, stateFilename))
	if err != nil {
		log.Fatal(err)
	}

	// Open
	var client influxdb.MessagingClient
	var server *influxdb.Server
	if state.Mode == "local" {
		client = messaging.NewLoopbackClient()
		log.Printf("Local messaging client created")
		server = influxdb.NewServer(client)
		err := server.Open(config.Storage.Dir)
		if err != nil {
			log.Fatalf("failed to open local Server", err.Error())
		}
	} else {
		// If the Broker directory exists, open a Broker on this node.
		if _, err := os.Stat(config.Raft.Dir); err == nil {
			b := messaging.NewBroker()
			if err := b.Open(config.Raft.Dir); err != nil {
				log.Fatalf("failed to open Broker", err.Error())
			}
		} else {
			log.Fatalf("failed to check for Broker directory", err.Error())
		}
		// If a Data directory exists, open a Data node.
		if _, err := os.Stat(config.Storage.Dir); err == nil {
			// Create correct client here for connecting to Broker.
			c := messaging.NewClient("XXX-CHANGEME-XXX")
			if err := c.Open(brokerURLs); err != nil {
				log.Fatalf("Error opening Messaging Client: %s", err.Error())
			}
			defer c.Close()
			client = c
			log.Printf("Cluster messaging client created")

			server = influxdb.NewServer(client)
			err = server.Open(config.Storage.Dir)
			if err != nil {
				log.Fatalf("failed to open data Server", err.Error())
			}
		} else {
			log.Fatalf("failed to check for Broker directory", err.Error())
		}
	}

	// TODO: startProfiler()
	// TODO: -reset-root

	// Start up HTTP server with correct endpoints.
	h := influxdb.NewHandler(server)
	func() { log.Fatal(http.ListenAndServe(":8086", h)) }()

	// Wait indefinitely.
	<-(chan struct{})(nil)
}

func printRunUsage() {
	log.Println(`usage: run [flags]

run starts the node with any existing cluster configuration. If no cluster configuration is
found, then the node runs in "local" mode. "Local" mode 

        -config <path>
                                Set the path to the configuration file.

        -hostname <name>
                                Override the hostname, the 'hostname' configuration option will be overridden.

        -pidfile <path>
                                Write process ID to a file.
`)
}

// createStateIfNotExists returns the cluster state, from the file at path.
// If no file exists at path, the default state is created, and written to the path.
func createStateIfNotExists(path string) (*State, error) {
	// Read state from path.
	// If state doesn't exist then return a "local" state.
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return &State{Mode: "local"}, nil
	} else if err != nil {
		return nil, err
	}
	defer f.Close()

	// Decode state from file and return.
	s := &State{}
	if err := json.NewDecoder(f).Decode(&s); err != nil {
		return nil, err
	}

	return s, nil
}
