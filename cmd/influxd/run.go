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

	// Print sweet InfluxDB logo and write the process id to file.
	log.Print(logo)
	writePIDFile(*pidPath)

	// Parse the configuration and open the broker & server, if applicable.
	config := parseConfig(*configPath, *hostname)
	b := openBroker(config.Broker.Dir, config.BrokerConnectionString())
	s := openServer(config.Data.Dir, strings.Split(*seedServers, ","))

	// Start the HTTP service(s).
	listenAndServe(b, s, config.BrokerListenAddr(), config.ApiHTTPListenAddr())

	// TODO: Initialize, if necessary.

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
	if err != nil {
		log.Fatalf("config: %s", err)
	}

	// Override config properties.
	if hostname != "" {
		config.Hostname = hostname
	}

	return config
}

// creates and initializes a broker at a given path.
// Ignored if there is no broker directory.
func openBroker(path, addr string) *messaging.Broker {
	// Ignore if there's no broker directory.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}

	// If the Broker directory exists, open a Broker on this node.
	b := messaging.NewBroker()
	if err := b.Open(path, addr); err != nil {
		log.Fatalf("failed to open Broker", err.Error())
	}
	return b
}

// creates and initializes a server at a given path.
// Ignored if there is no data directory.
func openServer(path string, seedServers []string) *influxdb.Server {
	// Ignore if the data directory doesn't exists.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}

	// Create and open server
	s := influxdb.NewServer()
	if err := s.Open(path); err != nil {
		log.Fatalf("failed to open data server", err.Error())
	}

	// Open messaging client to communicate with the brokers.
	var brokerURLs []*url.URL
	for _, s := range seedServers {
		u, err := url.Parse(s)
		if err != nil {
			log.Fatalf("cannot parse seed server: %s", err)
		}
		brokerURLs = append(brokerURLs, u)
	}

	// Initialize the messaging client.
	c := messaging.NewClient(s.ID())
	if err := c.Open(filepath.Join(path, messagingClientFile), brokerURLs); err != nil {
		log.Fatalf("error opening messaging client: %s", err)
	}

	// Assign the client to the server.
	if err := s.SetClient(c); err != nil {
		log.Fatalf("set messaging client: %s", err)
	}

	return s
}

// starts handlers for the broker and server.
// If the broker and server are running on the same port then combine them.
func listenAndServe(b *messaging.Broker, s *influxdb.Server, brokerAddr, serverAddr string) {
	// Initialize handlers.
	var bh, sh http.Handler
	if b != nil {
		bh = messaging.NewHandler(b)
	}
	if s != nil {
		sh = influxdb.NewHandler(s)
	}

	// Combine handlers if they are using the same bind address.
	if brokerAddr == serverAddr {
		go func() { log.Fatal(http.ListenAndServe(brokerAddr, NewHandler(bh, sh))) }()
	} else {
		// Otherwise start the handlers on separate ports.
		if sh != nil {
			go func() { log.Fatal(http.ListenAndServe(serverAddr, sh)) }()
		}
		if bh != nil {
			go func() { log.Fatal(http.ListenAndServe(brokerAddr, bh)) }()
		}
	}

	// Log the handlers starting up.
	if serverAddr == "" {
		log.Printf("Starting Influx Server %s...", version)
	} else {
		log.Printf("Starting Influx Server %s bound to %s...", version, serverAddr)
	}

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
