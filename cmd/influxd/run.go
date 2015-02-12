package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/admin"
	"github.com/influxdb/influxdb/collectd"
	"github.com/influxdb/influxdb/graphite"
	"github.com/influxdb/influxdb/httpd"
	"github.com/influxdb/influxdb/messaging"
)

func Run(config *Config, join, version string, logWriter *os.File) (*messaging.Broker, *influxdb.Server) {
	// Parse the configuration and determine if a broker and/or server exist.
	configExists := config != nil
	if config == nil {
		config = NewConfig()
	}
	initializing := !fileExists(config.BrokerDir()) && !fileExists(config.DataDir())

	// Parse join urls from the --join flag.
	var joinURLs []*url.URL
	if join == "" {
		joinURLs = parseURLs(config.JoinURLs())
	} else {
		joinURLs = parseURLs(join)
	}

	// Open broker, initialize or join as necessary.
	b := openBroker(config.BrokerDir(), config.BrokerURL(), initializing, joinURLs, logWriter)

	// Start the broker handler.
	var h *Handler
	if b != nil {
		h = &Handler{brokerHandler: messaging.NewHandler(b.Broker)}
		// We want to make sure we are spun up before we exit this function, so we manually listen and serve
		listener, err := net.Listen("tcp", config.BrokerAddr())
		if err != nil {
			log.Fatal(err)
		}
		go func() { log.Fatal(http.Serve(listener, h)) }()
		log.Printf("broker listening on %s", config.BrokerAddr())
	}

	// Open server, initialize or join as necessary.
	s := openServer(config, b, initializing, configExists, joinURLs, logWriter)
	s.SetAuthenticationEnabled(config.Authentication.Enabled)

	// Enable retention policy enforcement if requested.
	if config.Data.RetentionCheckEnabled {
		interval := time.Duration(config.Data.RetentionCheckPeriod)
		if err := s.StartRetentionPolicyEnforcement(interval); err != nil {
			log.Fatalf("retention policy enforcement failed: %s", err.Error())
		}
		log.Printf("broker enforcing retention policies with check interval of %s", interval)
	}

	// Start the server handler. Attach to broker if listening on the same port.
	if s != nil {
		sh := httpd.NewHandler(s, config.Authentication.Enabled, version)
		if h != nil && config.BrokerAddr() == config.DataAddr() {
			h.serverHandler = sh
		} else {
			// We want to make sure we are spun up before we exit this function, so we manually listen and serve
			listener, err := net.Listen("tcp", config.DataAddr())
			if err != nil {
				log.Fatal(err)
			}
			go func() { log.Fatal(http.Serve(listener, sh)) }()
		}
		log.Printf("data node #%d listening on %s", s.ID(), config.DataAddr())

		// Start the admin interface on the default port
		if config.Admin.Enabled {
			port := fmt.Sprintf(":%d", config.Admin.Port)
			log.Printf("starting admin server on %s", port)
			a := admin.NewServer(port)
			go a.ListenAndServe()
		}

		// broker needs the server to occasionally run continuous queries
		b.RunContinuousQueryLoop(s)

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
	return b.Broker, s
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
func openBroker(path string, u *url.URL, initializing bool, joinURLs []*url.URL, w io.Writer) *influxdb.Broker {
	// Ignore if there's no existing broker and we're not initializing or joining.
	if !fileExists(path) && !initializing && len(joinURLs) == 0 {
		return nil
	}

	// Create broker.
	b := influxdb.NewBroker()
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
func initializeBroker(b *influxdb.Broker) {
	if err := b.Initialize(); err != nil {
		log.Fatalf("initialize: %s", err)
	}
}

// joins a broker to an existing cluster.
func joinBroker(b *influxdb.Broker, joinURLs []*url.URL) {
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
func openServer(config *Config, b *influxdb.Broker, initializing, configExists bool, joinURLs []*url.URL, w io.Writer) *influxdb.Server {
	// Ignore if there's no existing server and we're not initializing or joining.
	if !fileExists(config.Data.Dir) && !initializing && len(joinURLs) == 0 {
		return nil
	}

	// Create and open the server.
	s := influxdb.NewServer()
	s.SetLogOutput(w)
	s.RecomputePreviousN = config.ContinuousQuery.RecomputePreviousN
	s.RecomputeNoOlderThan = time.Duration(config.ContinuousQuery.RecomputeNoOlderThan)
	s.ComputeRunsPerInterval = config.ContinuousQuery.ComputeRunsPerInterval
	s.ComputeNoMoreThan = time.Duration(config.ContinuousQuery.ComputeNoMoreThan)

	if err := s.Open(config.Data.Dir); err != nil {
		log.Fatalf("failed to open data server: %v", err.Error())
	}

	// If the server is uninitialized then initialize or join it.
	if initializing {
		if len(joinURLs) == 0 {
			initializeServer(s, b, w)
		} else {
			joinServer(s, config.DataURL(), joinURLs)
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
func initializeServer(s *influxdb.Server, b *influxdb.Broker, w io.Writer) {
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
