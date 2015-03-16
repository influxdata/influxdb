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
	"github.com/influxdb/influxdb/raft"
	"github.com/influxdb/influxdb/udp"
)

func Run(config *Config, join, version string, logWriter *os.File) (*messaging.Broker, *influxdb.Server) {
	log.Printf("influxdb started, version %s, commit %s", version, commit)

	// Parse the configuration and determine if a broker and/or server exist.
	configExists := config != nil
	if config == nil {
		config = NewConfig()
	}

	var initBroker, initServer bool
	if initBroker = !fileExists(config.BrokerDir()); initBroker {
		log.Printf("Broker directory missing. Need to create a broker.")
	}

	if initServer = !fileExists(config.DataDir()); initServer {
		log.Printf("Data directory missing. Need to create data directory.")
	}
	initServer = initServer || initBroker

	// Parse join urls from the --join flag.
	var joinURLs []url.URL
	if join == "" {
		joinURLs = parseURLs(config.JoinURLs())
	} else {
		joinURLs = parseURLs(join)
	}

	// Open broker & raft log, initialize or join as necessary.
	b, l := openBroker(config.BrokerDir(), config.BrokerURL(), initBroker, joinURLs, logWriter, config.Logging.RaftTracing)

	// Start the broker handler.
	var h *Handler
	if b != nil {
		h = &Handler{
			brokerHandler: &messaging.Handler{
				Broker:      b.Broker,
				RaftHandler: &raft.Handler{Log: l},
			},
		}

		// We want to make sure we are spun up before we exit this function, so we manually listen and serve
		listener, err := net.Listen("tcp", config.BrokerAddr())
		if err != nil {
			log.Fatalf("Broker failed to listen on %s. %s ", config.BrokerAddr(), err)
		}
		go func() {
			err := http.Serve(listener, h)
			if err != nil {
				log.Fatalf("Broker failed to server on %s.: %s", config.BrokerAddr(), err)
			}
		}()
		log.Printf("broker listening on %s", config.BrokerAddr())

		// have it occasionally tell a data node in the cluster to run continuous queries
		if config.ContinuousQuery.Disable {
			log.Printf("Not running continuous queries. [continuous_queries].disable is set to true.")
		} else {
			b.RunContinuousQueryLoop()
		}
	}

	// Open server, initialize or join as necessary.
	s := openServer(config, b, initServer, initBroker, configExists, joinURLs, logWriter)
	s.SetAuthenticationEnabled(config.Authentication.Enabled)

	// Enable retention policy enforcement if requested.
	if config.Data.RetentionCheckEnabled {
		interval := time.Duration(config.Data.RetentionCheckPeriod)
		if err := s.StartRetentionPolicyEnforcement(interval); err != nil {
			log.Fatalf("retention policy enforcement failed: %s", err.Error())
		}
		log.Printf("broker enforcing retention policies with check interval of %s", interval)
	}

	// Start shard group pre-create
	interval := config.ShardGroupPreCreateCheckPeriod()
	if err := s.StartShardGroupsPreCreate(interval); err != nil {
		log.Fatalf("shard group pre-create failed: %s", err.Error())
	}
	log.Printf("shard group pre-create with check interval of %s", interval)

	// Start the server handler. Attach to broker if listening on the same port.
	if s != nil {
		sh := httpd.NewHandler(s, config.Authentication.Enabled, version)
		sh.SetLogOutput(logWriter)
		sh.WriteTrace = config.Logging.WriteTracing

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

		// Start the server bound to a UDP listener
		if config.UDP.Enabled {
			log.Printf("Starting UDP listener on %s", config.DataAddrUDP())
			u := udp.NewUDPServer(s)
			if err := u.ListenAndServe(config.DataAddrUDP()); err != nil {
				log.Printf("Failed to start UDP listener on %s: %s", config.DataAddrUDP(), err)
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

			if err := s.CreateDatabaseIfNotExists(c.DatabaseString()); err != nil {
				log.Fatalf("failed to create database for %s Graphite server: %s", c.Protocol, err.Error())
			}

			// Spin up the server.
			var g graphite.Server
			g, err := graphite.NewServer(c.Protocol, parser, s, c.DatabaseString())
			if err != nil {
				log.Fatalf("failed to initialize %s Graphite server: %s", c.Protocol, err.Error())
			}
			g.SetLogOutput(logWriter)

			err = g.ListenAndServe(c.ConnectionString(config.BindAddress))
			if err != nil {
				log.Fatalf("failed to start %s Graphite server: %s", c.Protocol, err.Error())
			}
		}

		// Start up self-monitoring if enabled.
		if config.Statistics.Enabled {
			database := config.Statistics.Database
			policy := config.Statistics.RetentionPolicy
			interval := time.Duration(config.Statistics.WriteInterval)

			// Ensure database exists.
			if err := s.CreateDatabaseIfNotExists(database); err != nil {
				log.Fatalf("failed to create database %s for internal statistics: %s", database, err.Error())
			}

			// Ensure retention policy exists.
			rp := influxdb.NewRetentionPolicy(policy)
			if err := s.CreateRetentionPolicyIfNotExists(database, rp); err != nil {
				log.Fatalf("failed to create retention policy for internal statistics: %s", err.Error())
			}

			s.StartSelfMonitoring(database, policy, interval)
			log.Printf("started self-monitoring at interval of %s", interval)
		}
	}

	// unless disabled, start the loop to report anonymous usage stats every 24h
	if !config.ReportingDisabled {
		// Make sure we have a config object b4 we try to use it.
		if clusterID := b.Broker.ClusterID(); clusterID != 0 {
			go s.StartReportingLoop(version, clusterID)
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
	err := os.MkdirAll(filepath.Dir(path), 0755)
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
func openBroker(path string, u url.URL, initializing bool, joinURLs []url.URL, w io.Writer, raftTracing bool) (*influxdb.Broker, *raft.Log) {
	// Create raft log.
	l := raft.NewLog()
	l.SetURL(u)
	l.SetLogOutput(w)
	l.DebugEnabled = raftTracing

	// Create broker.
	b := influxdb.NewBroker()
	b.Log = l
	b.SetLogOutput(w)

	// Open broker so it can feed last index data to the log.
	if err := b.Open(path); err != nil {
		log.Fatalf("failed to open broker: %s", err)
	}

	// Attach the broker as the finite state machine of the raft log.
	l.FSM = &messaging.RaftFSM{Broker: b}

	// Open raft log inside broker directory.
	if err := l.Open(filepath.Join(path, "raft")); err != nil {
		log.Fatalf("raft: %s", err)
	}

	// If this is a new broker then we can initialize two ways:
	//   1) Start a brand new cluster.
	//   2) Join an existing cluster.
	if initializing {
		if len(joinURLs) == 0 {
			if err := l.Initialize(); err != nil {
				log.Fatalf("initialize raft log: %s", err)
			}
		} else {
			joinLog(l, joinURLs)
		}
	}

	return b, l
}

// joins a raft log to an existing cluster.
func joinLog(l *raft.Log, joinURLs []url.URL) {
	// Attempts to join each server until successful.
	for _, u := range joinURLs {
		if err := l.Join(u); err != nil {
			log.Printf("join: failed to connect to raft cluster: %s: %s", u, err)
		} else {
			log.Printf("join: connected raft log to %s", u)
			return
		}
	}
	log.Fatalf("join: failed to connect raft log to any specified server")
}

// creates and initializes a server.
func openServer(config *Config, b *influxdb.Broker, initServer, initBroker, configExists bool, joinURLs []url.URL, w io.Writer) *influxdb.Server {
	// Use broker URL is there is no config and there are no join URLs passed.
	clientJoinURLs := joinURLs
	if !configExists || len(joinURLs) == 0 {
		clientJoinURLs = []url.URL{b.URL()}
	}

	// Create messaging client to the brokers.
	c := influxdb.NewMessagingClient()
	c.SetLogOutput(w)
	if err := c.Open(filepath.Join(config.Data.Dir, messagingClientFile)); err != nil {
		log.Fatalf("messaging client error: %s", err)
	}

	// If join URLs were passed in then use them to override the client's URLs.
	if len(clientJoinURLs) > 0 {
		c.SetURLs(clientJoinURLs)
	}

	// If no URLs exist on the client the return an error since we cannot reach a broker.
	if len(c.URLs()) == 0 {
		log.Fatal("messaging client has no broker URLs")
	}

	// Create and open the server.
	s := influxdb.NewServer()
	s.SetLogOutput(w)
	s.WriteTrace = config.Logging.WriteTracing
	s.RetentionAutoCreate = config.Data.RetentionAutoCreate
	s.RecomputePreviousN = config.ContinuousQuery.RecomputePreviousN
	s.RecomputeNoOlderThan = time.Duration(config.ContinuousQuery.RecomputeNoOlderThan)
	s.ComputeRunsPerInterval = config.ContinuousQuery.ComputeRunsPerInterval
	s.ComputeNoMoreThan = time.Duration(config.ContinuousQuery.ComputeNoMoreThan)

	// Open server with data directory and broker client.
	if err := s.Open(config.Data.Dir, c); err != nil {
		log.Fatalf("failed to open data server: %v", err.Error())
	}

	// If the server is uninitialized then initialize or join it.
	if initServer {
		if len(joinURLs) == 0 {
			if initBroker {
				if err := s.Initialize(b.URL()); err != nil {
					log.Fatalf("server initialization error: %s", err)
				}
			}
		} else {
			joinServer(s, config.DataURL(), joinURLs)
		}
	}

	return s
}

// joins a server to an existing cluster.
func joinServer(s *influxdb.Server, u url.URL, joinURLs []url.URL) {
	// TODO: Use separate broker and data join urls.

	// Create data node on an existing data node.
	for _, joinURL := range joinURLs {
		if err := s.Join(&u, &joinURL); err != nil {
			log.Printf("join: failed to connect data node: %s: %s", u, err)
		} else {
			log.Printf("join: connected data node to %s", u)
			return
		}
	}
	log.Fatalf("join: failed to connect data node to any specified server")
}

// parses a comma-delimited list of URLs.
func parseURLs(s string) (a []url.URL) {
	if s == "" {
		return nil
	}

	for _, s := range strings.Split(s, ",") {
		u, err := url.Parse(s)
		if err != nil {
			log.Fatalf("cannot parse urls: %s", err)
		}
		a = append(a, *u)
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
