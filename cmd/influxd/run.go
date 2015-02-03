package main

import (
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/collectd"
	"github.com/influxdb/influxdb/graphite"
	"github.com/influxdb/influxdb/httpd"
	"github.com/influxdb/influxdb/messaging"
)

// execRun runs the "run" command.
func execRun(args []string) {
	// Parse command flags.
	fs := flag.NewFlagSet("", flag.ExitOnError)
	var (
		configPath = fs.String("config", "", "")
		pidPath    = fs.String("pidfile", "", "")
		hostname   = fs.String("hostname", "", "")
		join       = fs.String("join", "", "")
	)
	fs.Usage = printRunUsage
	fs.Parse(args)

	// Print sweet InfluxDB logo and write the process id to file.
	log.Print(logo)
	log.SetPrefix(`[influxd] `)
	log.SetFlags(log.LstdFlags)
	writePIDFile(*pidPath)

	// Parse the configuration and determine if a broker and/or server exist.
	config := parseConfig(*configPath, *hostname)
	configExists := *configPath != ""
	initializing := !fileExists(config.BrokerDir()) && !fileExists(config.DataDir())

	// Create a logging writer.
	logWriter := os.Stderr
	if config.Logging.File != "" {
		var err error
		logWriter, err = os.OpenFile(config.Logging.File, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0660)
		if err != nil {
			log.Fatalf("unable to open log file %s: %s", config.Logging.File, err.Error())
		}
	}
	log.SetOutput(logWriter)

	// Parse join urls from the --join flag.
	var joinURLs []*url.URL
	if *join == "" {
		joinURLs = parseURLs(config.JoinURLs())
	} else {
		joinURLs = parseURLs(*join)
	}

	// Mark the start of the log.
	log.Printf("influxd starting up")

	// Open broker, initialize or join as necessary.
	b := openBroker(config.BrokerDir(), config.BrokerURL(), initializing, joinURLs, logWriter)

	// Start the broker handler.
	var h *Handler
	if b != nil {
		h = &Handler{brokerHandler: messaging.NewHandler(b)}
		go func() { log.Fatal(http.ListenAndServe(config.BrokerAddr(), h)) }()
		log.Printf("broker listening on %s", config.BrokerAddr())
	}

	// Open server, initialize or join as necessary.
	s := openServer(config.DataDir(), config.DataURL(), b, initializing, configExists, joinURLs, logWriter)

	// Start the server handler. Attach to broker if listening on the same port.
	if s != nil {
		sh := httpd.NewHandler(s, config.Authentication.Enabled, version)
		if h != nil && config.BrokerAddr() == config.DataAddr() {
			h.serverHandler = sh
		} else {
			go func() { log.Fatal(http.ListenAndServe(config.DataAddr(), sh)) }()
		}
		log.Printf("data node #%d listening on %s", s.ID(), config.DataAddr())

		// Spin up the collectd server
		if config.Collectd.Enabled {
			c := config.Collectd
			cs := collectd.NewServer(s, c.TypesDB)
			cs.Database = c.Database
			err := collectd.ListenAndServe(cs, c.ConnectionString(config.BindAddress))
			if err != nil {
				log.Printf("failed to start collectd Server: %v\n", err.Error())
			}
		}
		// Spin up any Graphite servers
		for _, c := range config.Graphites {
			if !c.Enabled {
				continue
			}

			// Configure Graphite parsing.
			parser := graphite.NewParser()
			parser.Separator = c.NameSeparatorString()
			parser.LastEnabled = c.LastEnabled()

			// Start the relevant server.
			if strings.ToLower(c.Protocol) == "tcp" {
				g := graphite.NewTCPServer(parser, s)
				g.Database = c.Database
				err := g.ListenAndServe(c.ConnectionString(config.BindAddress))
				if err != nil {
					log.Printf("failed to start TCP Graphite Server: %v\n", err.Error())
				}
			} else if strings.ToLower(c.Protocol) == "udp" {
				g := graphite.NewUDPServer(parser, s)
				g.Database = c.Database
				err := g.ListenAndServe(c.ConnectionString(config.BindAddress))
				if err != nil {
					log.Printf("failed to start UDP Graphite Server: %v\n", err.Error())
				}
			} else {
				log.Fatalf("unrecognized Graphite Server prototcol %s", c.Protocol)
			}
		}
	}

	// Wait indefinitely.
	<-(chan struct{})(nil)
}

// write the current process id to a file specified by path.
func writePIDFile(path string) {
	if path == "" {
		return
	}

	// Ensure the required directory structure exists.
	err := os.MkdirAll(filepath.Dir(path), 0700)
	if err != nil {
		log.Fatal(err)
	}

	// Retrieve the PID and write it.
	pid := strconv.Itoa(os.Getpid())
	if err := ioutil.WriteFile(path, []byte(pid), 0644); err != nil {
		log.Fatal(err)
	}
}

// parses the configuration from a given path. Sets overrides as needed.
func parseConfig(path, hostname string) *Config {
	if path == "" {
		log.Println("No config provided, using default settings")
		return NewConfig()
	}

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

// creates and initializes a broker.
func openBroker(path string, u *url.URL, initializing bool, joinURLs []*url.URL, w io.Writer) *messaging.Broker {
	// Ignore if there's no existing broker and we're not initializing or joining.
	if !fileExists(path) && !initializing && len(joinURLs) == 0 {
		return nil
	}

	// Create broker.
	b := messaging.NewBroker()
	b.SetLogOutput(w)
	if err := b.Open(path, u); err != nil {
		log.Fatalf("failed to open broker: %s", err)
	}

	// If this is a new broker then we can initialie two ways:
	//   1) Start a brand new cluster.
	//   2) Join an existing cluster.
	if initializing {
		if len(joinURLs) == 0 {
			initializeBroker(b)
		} else {
			joinBroker(b, joinURLs)
		}
	}

	return b
}

// initializes a new broker.
func initializeBroker(b *messaging.Broker) {
	if err := b.Initialize(); err != nil {
		log.Fatalf("initialize: %s", err)
	}
}

// joins a broker to an existing cluster.
func joinBroker(b *messaging.Broker, joinURLs []*url.URL) {
	// Attempts to join each server until successful.
	for _, u := range joinURLs {
		if err := b.Join(u); err != nil {
			log.Printf("join: failed to connect to broker: %s: %s", u, err)
		} else {
			log.Printf("join: connected broker to %s", u)
			return
		}
	}
	log.Fatalf("join: failed to connect broker to any specified server")
}

// creates and initializes a server.
func openServer(path string, u *url.URL, b *messaging.Broker, initializing, configExists bool, joinURLs []*url.URL, w io.Writer) *influxdb.Server {
	// Ignore if there's no existing server and we're not initializing or joining.
	if !fileExists(path) && !initializing && len(joinURLs) == 0 {
		return nil
	}

	// Create and open the server.
	s := influxdb.NewServer()
	s.SetLogOutput(w)
	if err := s.Open(path); err != nil {
		log.Fatalf("failed to open data server: %v", err.Error())
	}

	// If the server is uninitialized then initialize or join it.
	if initializing {
		if len(joinURLs) == 0 {
			initializeServer(s, b, w)
		} else {
			joinServer(s, u, joinURLs)
			openServerClient(s, joinURLs, w)
		}
	} else if !configExists {
		// We are spining up a server that has no config,
		// but already has an initialized data directory
		joinURLs = []*url.URL{b.URL()}
		openServerClient(s, joinURLs, w)
	} else {
		if len(joinURLs) == 0 {
			// If a config exists, but no joinUrls are specified, fall back to the broker URL
			// TODO: Make sure we have a leader, and then spin up the server
			joinURLs = []*url.URL{b.URL()}
		}
		openServerClient(s, joinURLs, w)
	}

	return s
}

// initializes a new server that does not yet have an ID.
func initializeServer(s *influxdb.Server, b *messaging.Broker, w io.Writer) {
	// TODO: Create replica using the messaging client.

	// Create replica on broker.
	if err := b.CreateReplica(1); err != nil {
		log.Fatalf("replica creation error: %s", err)
	}

	// Create messaging client.
	c := messaging.NewClient(1)
	c.SetLogOutput(w)
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

// joins a server to an existing cluster.
func joinServer(s *influxdb.Server, u *url.URL, joinURLs []*url.URL) {
	// TODO: Use separate broker and data join urls.

	// Create data node on an existing data node.
	for _, joinURL := range joinURLs {
		if err := s.Join(u, joinURL); err != nil {
			log.Printf("join: failed to connect data node: %s: %s", u, err)
		} else {
			log.Printf("join: connected data node to %s", u)
			return
		}
	}
	log.Fatalf("join: failed to connect data node to any specified server")
}

// opens the messaging client and attaches it to the server.
func openServerClient(s *influxdb.Server, joinURLs []*url.URL, w io.Writer) {
	c := messaging.NewClient(s.ID())
	c.SetLogOutput(w)
	if err := c.Open(filepath.Join(s.Path(), messagingClientFile), joinURLs); err != nil {
		log.Fatalf("messaging client error: %s", err)
	}
	if err := s.SetClient(c); err != nil {
		log.Fatalf("set client error: %s", err)
	}
}

// parses a comma-delimited list of URLs.
func parseURLs(s string) (a []*url.URL) {
	if s == "" {
		return nil
	}

	for _, s := range strings.Split(s, ",") {
		u, err := url.Parse(s)
		if err != nil {
			log.Fatalf("cannot parse urls: %s", err)
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

run starts the broker and data node server. If this is the first time running
the command then a new cluster will be initialized unless the -join argument
is used.

        -config <path>
                          Set the path to the configuration file.

        -hostname <name>
                          Override the hostname, the 'hostname' configuration
                          option will be overridden.

        -join <url>
                          Joins the server to an existing cluster.

        -pidfile <path>
                          Write process ID to a file.
`)
}
