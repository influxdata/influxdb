package main

import (
	"flag"
	"fmt"
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

type RunCommand struct {
	// The logger passed to the ticker during execution.
	Logger    *log.Logger
	logWriter *os.File
	config    *Config
	hostname  string
	server    *Server
}

func NewRunCommand() *RunCommand {
	return &RunCommand{
		server: &Server{},
	}
}

type Server struct {
	broker   *influxdb.Broker
	dataNode *influxdb.Server
	raftLog  *raft.Log
}

func (s *Server) Close() {
	if s.broker != nil {
		if err := s.broker.Close(); err != nil {
			log.Fatalf("error closing broker: %s", err)
		}
	}

	if s.raftLog != nil {
		if err := s.raftLog.Close(); err != nil {
			log.Fatalf("error closing raft log: %s", err)
		}
	}

	if s.dataNode != nil {
		if err := s.dataNode.Close(); err != nil {
			log.Fatalf("error data broker: %s", err)
		}
	}

}

func (cmd *RunCommand) Run(args ...string) error {
	// Set up logger.
	cmd.Logger = log.New(os.Stderr, "", log.LstdFlags)

	// Parse command flags.
	fs := flag.NewFlagSet("", flag.ExitOnError)
	var configPath, pidfile, hostname, join, cpuprofile, memprofile string

	fs.StringVar(&configPath, "config", "", "")
	fs.StringVar(&pidfile, "pidfile", "", "")
	fs.StringVar(&hostname, "hostname", "", "")
	fs.StringVar(&join, "join", "", "")
	fs.StringVar(&cpuprofile, "cpuprofile", "", "")
	fs.StringVar(&memprofile, "memprofile", "", "")

	fs.Usage = printRunUsage
	fs.Parse(args)
	cmd.hostname = hostname

	// Start profiling, if set.
	startProfiling(cpuprofile, memprofile)
	defer stopProfiling()

	// Print sweet InfluxDB logo and write the process id to file.
	log.Print(logo)
	writePIDFile(pidfile)

	var err error
	// Parse configuration file from disk.
	cmd.config, err = parseConfig(configPath, hostname)
	if err != nil {
		cmd.Logger.Fatal(err)
	} else if configPath == "" {
		cmd.Logger.Println("No config provided, using default settings")
	}

	cmd.Open(cmd.config, join)

	// Wait indefinitely.
	<-(chan struct{})(nil)
	return nil
}

func (cmd *RunCommand) Open(config *Config, join string) (*messaging.Broker, *influxdb.Server) {

	if config != nil {
		cmd.config = config
	}

	log.Printf("influxdb started, version %s, commit %s", version, commit)

	// Parse join urls from the --join flag.
	var brokerURLs []url.URL
	if join == "" {
		brokerURLs = parseURLs(cmd.config.JoinURLs())
	} else {
		brokerURLs = parseURLs(join)
	}

	dataURLs := parseURLs(cmd.config.Data.JoinURLs)

	// Open broker & raft log, initialize or join as necessary.
	if cmd.config.Broker.Enabled {
		cmd.openBroker(brokerURLs)
	}

	// Start the broker handler.
	var h *Handler
	if cmd.server.broker != nil {
		h = &Handler{
			brokerHandler: &messaging.Handler{
				Broker:      cmd.server.broker,
				RaftHandler: &raft.Handler{Log: cmd.server.raftLog},
			},
		}

		// We want to make sure we are spun up before we exit this function, so we manually listen and serve
		listener, err := net.Listen("tcp", cmd.config.BrokerAddr())
		if err != nil {
			log.Fatalf("Broker failed to listen on %s. %s ", cmd.config.BrokerAddr(), err)
		}
		go func() {
			err := http.Serve(listener, h)
			if err != nil {
				log.Fatalf("Broker failed to server on %s.: %s", cmd.config.BrokerAddr(), err)
			}
		}()
		log.Printf("broker listening on %s", cmd.config.BrokerAddr())

		// have it occasionally tell a data node in the cluster to run continuous queries
		if cmd.config.ContinuousQuery.Disabled {
			log.Printf("Not running continuous queries. [continuous_queries].disable is set to true.")
		} else {
			cmd.server.broker.RunContinuousQueryLoop()
		}
	}

	var s *influxdb.Server
	// Open server, initialize or join as necessary.
	if cmd.config.Data.Enabled {
		//FIXME: Need to also pass in dataURLs to bootstrap a data node
		s = cmd.openServer(brokerURLs, dataURLs)
		s.SetAuthenticationEnabled(cmd.config.Authentication.Enabled)

		// Enable retention policy enforcement if requested.
		if cmd.config.Data.RetentionCheckEnabled {
			interval := time.Duration(cmd.config.Data.RetentionCheckPeriod)
			if err := s.StartRetentionPolicyEnforcement(interval); err != nil {
				log.Fatalf("retention policy enforcement failed: %s", err.Error())
			}
			log.Printf("broker enforcing retention policies with check interval of %s", interval)
		}

		// Start shard group pre-create
		interval := cmd.config.ShardGroupPreCreateCheckPeriod()
		if err := s.StartShardGroupsPreCreate(interval); err != nil {
			log.Fatalf("shard group pre-create failed: %s", err.Error())
		}
		log.Printf("shard group pre-create with check interval of %s", interval)
	}

	// Start the server handler. Attach to broker if listening on the same port.
	if s != nil {
		sh := httpd.NewHandler(s, cmd.config.Authentication.Enabled, version)
		sh.WriteTrace = cmd.config.Logging.WriteTracing

		if h != nil && cmd.config.BrokerAddr() == cmd.config.DataAddr() {
			h.serverHandler = sh
		} else {
			// We want to make sure we are spun up before we exit this function, so we manually listen and serve
			listener, err := net.Listen("tcp", cmd.config.DataAddr())
			if err != nil {
				log.Fatal(err)
			}
			go func() { log.Fatal(http.Serve(listener, sh)) }()
		}
		log.Printf("data node #%d listening on %s", s.ID(), cmd.config.DataAddr())

		if config.Snapshot.Enabled {
			// Start snapshot handler.
			go func() {
				log.Fatal(http.ListenAndServe(
					cmd.config.SnapshotAddr(),
					&httpd.SnapshotHandler{
						CreateSnapshotWriter: s.CreateSnapshotWriter,
					},
				))
			}()
			log.Printf("snapshot endpoint listening on %s", cmd.config.SnapshotAddr())
		} else {
			log.Println("snapshot endpoint disabled")
		}

		// Start the admin interface on the default port
		if cmd.config.Admin.Enabled {
			port := fmt.Sprintf(":%d", cmd.config.Admin.Port)
			log.Printf("starting admin server on %s", port)
			a := admin.NewServer(port)
			go a.ListenAndServe()
		}

		// Spin up the collectd server
		if cmd.config.Collectd.Enabled {
			c := cmd.config.Collectd
			cs := collectd.NewServer(s, c.TypesDB)
			cs.Database = c.Database
			err := collectd.ListenAndServe(cs, c.ConnectionString(cmd.config.BindAddress))
			if err != nil {
				log.Printf("failed to start collectd Server: %v\n", err.Error())
			}
		}

		// Start the server bound to a UDP listener
		if cmd.config.UDP.Enabled {
			log.Printf("Starting UDP listener on %s", cmd.config.DataAddrUDP())
			u := udp.NewUDPServer(s)
			if err := u.ListenAndServe(cmd.config.DataAddrUDP()); err != nil {
				log.Printf("Failed to start UDP listener on %s: %s", cmd.config.DataAddrUDP(), err)
			}

		}

		// Spin up any Graphite servers
		for _, c := range cmd.config.Graphites {
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

			err = g.ListenAndServe(c.ConnectionString(cmd.config.BindAddress))
			if err != nil {
				log.Fatalf("failed to start %s Graphite server: %s", c.Protocol, err.Error())
			}
		}

		// Start up self-monitoring if enabled.
		if cmd.config.Statistics.Enabled {
			database := cmd.config.Statistics.Database
			policy := cmd.config.Statistics.RetentionPolicy
			interval := time.Duration(cmd.config.Statistics.WriteInterval)

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
	if !cmd.config.ReportingDisabled {

		if cmd.config.Broker.Enabled && cmd.config.Data.Enabled {
			// Make sure we have a config object b4 we try to use it.
			if clusterID := cmd.server.broker.Broker.ClusterID(); clusterID != 0 {
				go s.StartReportingLoop(clusterID)
			}
		} else {
			log.Fatalln("failed to start reporting because not running as a broker and a data node")
		}
	}

	var b *messaging.Broker
	if cmd.server.broker != nil {
		b = cmd.server.broker.Broker
	}
	return b, s
}

func (cmd *RunCommand) Close() {
	cmd.server.Close()
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

// parseConfig parses the configuration from a given path. Sets overrides as needed.
func parseConfig(path, hostname string) (*Config, error) {
	if path == "" {
		c, err := NewTestConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to generate default config: %s. Please supply an explicit configuration file", err.Error())
		}
		return c, nil
	}

	// Parse configuration.
	config, err := ParseConfigFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %s", err)
	}

	// Override config properties.
	if hostname != "" {
		config.Hostname = hostname
	}

	return config, nil
}

// creates and initializes a broker.
func (cmd *RunCommand) openBroker(brokerURLs []url.URL) {
	path := cmd.config.BrokerDir()
	u := cmd.config.BrokerURL()
	raftTracing := cmd.config.Logging.RaftTracing

	// Create broker
	b := influxdb.NewBroker()
	cmd.server.broker = b

	// Create raft log.
	l := raft.NewLog()
	l.SetURL(u)
	l.DebugEnabled = raftTracing
	b.Log = l
	cmd.server.raftLog = l

	// Open broker so it can feed last index data to the log.
	if err := b.Open(path); err != nil {
		log.Fatalf("failed to open broker at %s : %s", path, err)
	}
	log.Printf("broker opened at %s", path)

	// Attach the broker as the f	inite state machine of the raft log.
	l.FSM = &messaging.RaftFSM{Broker: b}

	// Open raft log inside broker directory.
	if err := l.Open(filepath.Join(path, "raft")); err != nil {
		log.Fatalf("raft: %s", err)
	}

	// Checks to see if the raft index is 0.  If it's 0, it's the first
	// node in the cluster and must initialize
	if i, _ := l.LastLogIndexTerm(); i == 0 {
		if err := l.Initialize(); err != nil {
			log.Fatalf("initialize raft log: %s", err)
		}
		u := b.Broker.URL()
		log.Printf("initialized broker: %s\n", (&u).String())
		return
	}

	// If we have join URLs, attemp to join an existing cluster
	if len(brokerURLs) > 0 {
		joinLog(l, brokerURLs)
	}
}

// joins a raft log to an existing cluster.
func joinLog(l *raft.Log, brokerURLs []url.URL) {
	// Attempts to join each server until successful.
	for _, u := range brokerURLs {
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
func (cmd *RunCommand) openServer(brokerURLs []url.URL, dataURLs []url.URL) *influxdb.Server {

	// Create messaging client to the brokers.
	c := influxdb.NewMessagingClient()
	if err := c.Open(filepath.Join(cmd.config.Data.Dir, messagingClientFile)); err != nil {
		log.Fatalf("messaging client error: %s", err)
	}

	// If join URLs were passed in then use them to override the client's URLs.
	if len(brokerURLs) > 0 {
		c.SetURLs(brokerURLs)
	} else if cmd.server.broker != nil {
		c.SetURLs([]url.URL{cmd.server.broker.URL()})
	}

	// If no URLs exist on the client the return an error since we cannot reach a broker.
	if len(c.URLs()) == 0 {
		log.Fatal("messaging client has no broker URLs")
	}

	// Create and open the server.
	s := influxdb.NewServer()

	s.WriteTrace = cmd.config.Logging.WriteTracing
	s.RetentionAutoCreate = cmd.config.Data.RetentionAutoCreate
	s.RecomputePreviousN = cmd.config.ContinuousQuery.RecomputePreviousN
	s.RecomputeNoOlderThan = time.Duration(cmd.config.ContinuousQuery.RecomputeNoOlderThan)
	s.ComputeRunsPerInterval = cmd.config.ContinuousQuery.ComputeRunsPerInterval
	s.ComputeNoMoreThan = time.Duration(cmd.config.ContinuousQuery.ComputeNoMoreThan)
	s.Version = version
	s.CommitHash = commit
	cmd.server.dataNode = s

	// Open server with data directory and broker client.
	if err := s.Open(cmd.config.Data.Dir, c); err != nil {
		log.Fatalf("failed to open data server: %v", err.Error())
	}
	log.Printf("data server opened at %s", cmd.config.Data.Dir)

	if len(dataURLs) > 0 {
		joinServer(s, cmd.config.DataURL(), dataURLs)
		return s
	}

	dataNodeIndex := s.Index()
	if dataNodeIndex == 0 && len(dataURLs) == 0 {
		if err := s.Initialize(cmd.config.DataURL()); err != nil {
			log.Fatalf("server initialization error: %s", err)
		}
		u := cmd.config.DataURL()
		log.Printf("initialized data node: %s\n", (&u).String())
		return s
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
