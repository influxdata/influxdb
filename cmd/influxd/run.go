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
		role        = fs.String("role", "combined", "")
		hostname    = fs.String("hostname", "", "")
		seedServers = fs.String("seed-servers", "", "")
	)
	fs.Usage = printRunUsage
	fs.Parse(args)

	// Validate CLI flags.
	if *role != "combined" && *role != "broker" && *role != "data" {
		log.Fatalf("role must be 'combined', 'broker', or 'data'")
	}

	// Parse broker urls from seed servers.
	brokerURLs := parseSeedServers(*seedServers)

	// Print sweet InfluxDB logo and write the process id to file.
	log.Print(logo)
	writePIDFile(*pidPath)

	// Parse the configuration and determine if a broker and/or server exist.
	config := parseConfig(*configPath, *hostname)
	hasBroker := fileExists(config.Broker.Dir)
	hasServer := fileExists(config.Data.Dir)
	initializing := !hasBroker && !hasServer

	// Open broker if it exists or if we're initializing for the first time.
	var b *messaging.Broker
	var h *Handler
	if hasBroker || (initializing && (*role == "combined" || *role == "broker")) {
		b = openBroker(config.Broker.Dir, config.BrokerConnectionString())

		// If this is the first time running then initialize a broker.
		// Update the seed server so the server can connect locally.
		if initializing {
			if err := b.Initialize(); err != nil {
				log.Fatalf("initialize: %s", err)
			}
		}

		// Start the broker handler.
		h = &Handler{brokerHandler: messaging.NewHandler(b)}
		go func() { log.Fatal(http.ListenAndServe(config.BrokerListenAddr(), h)) }()
		log.Printf("Broker running on %s", config.BrokerListenAddr())
	}

	// Open server if it exists or we're initializing for the first time.
	var s *influxdb.Server
	if hasServer || (initializing && (*role == "combined" || *role == "data")) {
		s = openServer(config.Data.Dir)

		// If the server is uninitialized then initialize it with the broker.
		// Otherwise simply create a messaging client with the server id.
		if s.ID() == 0 {
			initServer(s, b)
		} else {
			openServerClient(s, brokerURLs)
		}

		// Start the server handler.
		// If it uses the same port as the broker then simply attach it.
		sh := influxdb.NewHandler(s)
		sh.AuthenticationEnabled = config.Authentication.Enabled

		if config.BrokerListenAddr() == config.ApiHTTPListenAddr() {
			h.serverHandler = sh
		} else {
			go func() { log.Fatal(http.ListenAndServe(config.ApiHTTPListenAddr(), sh)) }()
		}
		log.Printf("DataNode#%d running on %s", s.ID(), config.ApiHTTPListenAddr())
	}

	// Wait indefinitely.
	<-(chan struct{})(nil)
}

// write the current process id to a file specified by path.
func writePIDFile(path string) {
	if path == "" {
		return
	}

	// Retrieve the PID and write it.
	pid := strconv.Itoa(os.Getpid())
	if err := ioutil.WriteFile(path, []byte(pid), 0644); err != nil {
		log.Fatal(err)
	}
}

// parses the configuration from a given path. Sets overrides as needed.
func parseConfig(path, hostname string) *Config {
	// Parse configuration.
	config, err := ParseConfigFile(path)
	if os.IsNotExist(err) {
		config = NewConfig()
	} else if err != nil {
		log.Fatalf("config: %s", err)
	}

	// Override config properties.
	if hostname != "" {
		config.Hostname = hostname
	}

	return config
}

// creates and initializes a broker at a given path.
func openBroker(path, addr string) *messaging.Broker {
	b := messaging.NewBroker()
	if err := b.Open(path, addr); err != nil {
		log.Fatalf("failed to open broker: %s", err)
	}
	return b
}

// creates and initializes a server at a given path.
func openServer(path string) *influxdb.Server {
	s := influxdb.NewServer()
	if err := s.Open(path); err != nil {
		log.Fatalf("failed to open data server", err.Error())
	}
	return s
}

// initializes a new server that does not yet have an ID.
func initServer(s *influxdb.Server, b *messaging.Broker) {
	// TODO: Change messaging client to not require a ReplicaID so we can create
	// a replica without already being a replica.

	// Create replica on broker.
	if err := b.CreateReplica(1); err != nil {
		log.Fatalf("replica creation error: %d", err)
	}

	// Initialize messaging client.
	c := messaging.NewClient(1)
	if err := c.Open(filepath.Join(s.Path(), messagingClientFile), []*url.URL{b.URL()}); err != nil {
		log.Fatalf("messaging client error: %s", err)
	}
	if err := s.SetClient(c); err != nil {
		log.Fatalf("set client error: %s", err)
	}

	// Initialize the server.
	if err := s.Initialize(b.URL()); err != nil {
		log.Fatalf("server initialization error: %s", err)
	}
}

// opens the messaging client and attaches it to the server.
func openServerClient(s *influxdb.Server, brokerURLs []*url.URL) {
	c := messaging.NewClient(s.ID())
	if err := c.Open(filepath.Join(s.Path(), messagingClientFile), brokerURLs); err != nil {
		log.Fatalf("messaging client error: %s", err)
	}
	if err := s.SetClient(c); err != nil {
		log.Fatalf("set client error: %s", err)
	}
}

// parses a comma-delimited list of URLs.
func parseSeedServers(s string) (a []*url.URL) {
	for _, s := range strings.Split(s, ",") {
		u, err := url.Parse(s)
		if err != nil {
			log.Fatalf("cannot parse seed servers: %s", err)
		}
		a = append(a, u)
	}
	return
}

// returns true if the file exists.
func fileExists(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}

func printRunUsage() {
	log.Printf(`usage: run [flags]

run starts the node with any existing cluster configuration. If no cluster configuration is
found, then the node runs in "local" mode. "Local" mode is a single-node mode that does not
use Distributed Consensus, but is otherwise fully-functional.

        -config <path>
                                Set the path to the configuration file. Defaults to %s.

        -role <role>
                                Set the role to be 'combined', 'broker' or 'data'. broker' means it will take
                                part in Raft Distributed Consensus. 'data' means it will store time-series data.
                                'combined' means it will do both. The default is 'combined'. In role other than
                                these three is invalid.

        -hostname <name>
                                Override the hostname, the 'hostname' configuration option will be overridden.

        -seed-servers <servers>
                                If joining a cluster, overrides any previously configured or discovered
                                Data node seed servers.

        -pidfile <path>
                                Write process ID to a file.
`, configDefaultPath)
}
