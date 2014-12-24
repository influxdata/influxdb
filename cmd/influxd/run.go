package main

import (
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/messaging"
)

// execRun runs the "run" command.
func execRun(args []string) {
	// Parse command flags.
	fs := flag.NewFlagSet("", flag.ExitOnError)
	var (
		configPath  = fs.String("config", configDefaultPath, "")
		pidPath     = fs.String("pidfile", "", "")
		hostname    = fs.String("hostname", "", "")
		seedServers = fs.String("seed-servers", "", "")
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

	// Start up the node.
	var brokerHandler *messaging.Handler
	var serverHandler *influxdb.Handler
	var brokerDirExists bool
	var storageDirExists bool

	if _, err := os.Stat(config.Raft.Dir); err == nil {
		brokerDirExists = true
	}
	if _, err := os.Stat(config.Storage.Dir); err == nil {
		storageDirExists = true
	}

	if !brokerDirExists && !storageDirExists {
		// Node is completely new, so create the minimum needed, which
		// is a storage directory.
		if err := os.MkdirAll(config.Storage.Dir, 0744); err != nil {
			log.Fatal(err)
		}
		storageDirExists = true
	}

	// If the Broker directory exists, open a Broker on this node.
	if brokerDirExists {
		b := messaging.NewBroker()
		if err := b.Open(config.Raft.Dir, config.RaftConnectionString()); err != nil {
			log.Fatalf("failed to open Broker", err.Error())
		}
		brokerHandler = messaging.NewHandler(b)
	}

	// If the storage directory exists, open a Data node.
	if storageDirExists {
		var client influxdb.MessagingClient
		var server *influxdb.Server

		clientFilePath := filepath.Join(config.Storage.Dir, messagingClientFile)

		if _, err := os.Stat(clientFilePath); err == nil {
			var brokerURLs []*url.URL
			for _, s := range strings.Split(*seedServers, ",") {
				u, err := url.Parse(s)
				if err != nil {
					log.Fatalf("seed server", err)
				}
				brokerURLs = append(brokerURLs, u)
			}

			c := messaging.NewClient(0) // TODO: Set replica id.
			if err := c.Open(clientFilePath, brokerURLs); err != nil {
				log.Fatalf("Error opening Messaging Client: %s", err.Error())
			}
			defer c.Close()
			client = c
		} else {
			client = messaging.NewLoopbackClient()
		}

		server = influxdb.NewServer(client)
		err = server.Open(config.Storage.Dir)
		if err != nil {
			log.Fatalf("failed to open data Server", err.Error())
		}
		serverHandler = influxdb.NewHandler(server)
	}

	// TODO: startProfiler()
	// TODO: -reset-root

	// Start up HTTP server(s)
	if config.ApiHTTPListenAddr() != config.RaftListenAddr() {
		if serverHandler != nil {
			go func() { log.Fatal(http.ListenAndServe(config.ApiHTTPListenAddr(), serverHandler)) }()
		}
		if brokerHandler != nil {
			go func() { log.Fatal(http.ListenAndServe(config.RaftListenAddr(), brokerHandler)) }()
		}
	} else {
		h := NewHandler(brokerHandler, serverHandler)
		go func() { log.Fatal(http.ListenAndServe(config.ApiHTTPListenAddr(), h)) }()
	}

	// Wait indefinitely.
	<-(chan struct{})(nil)
}

func printRunUsage() {
	log.Printf(`usage: run [flags]

run starts the node with any existing cluster configuration. If no cluster configuration is
found, then the node runs in "local" mode. "Local" mode is a single-node mode that does not
use Distributed Consensus, but is otherwise fully-functional.

        -config <path>
                                Set the path to the configuration file. Defaults to %s.

        -hostname <name>
                                Override the hostname, the 'hostname' configuration option will be overridden.

        -seed-servers <servers>
                                If joining a cluster, overrides any previously configured or discovered
                                Data node seed servers.

        -pidfile <path>
                                Write process ID to a file.
`, configDefaultPath)
}
