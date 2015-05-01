package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/collectd"
	"github.com/influxdb/influxdb/graphite"
	"github.com/influxdb/influxdb/opentsdb"
	"github.com/influxdb/influxdb/udp"
)

type RunCommand struct {
	// The logger passed to the ticker during execution.
	logWriter *os.File
	config    *Config
	hostname  string
	node      *Node
}

func NewRunCommand() *RunCommand {
	return &RunCommand{}
}

func (cmd *RunCommand) ParseConfig(path string) error {
	// Parse configuration file from disk.
	if path != "" {
		var err error
		cmd.config, err = ParseConfigFile(path)
		if err != nil {
			return fmt.Errorf("error parsing configuration %s - %s\n", path, err)
		}
		log.Printf("using configuration at: %s\n", path)
	} else {
		var err error
		cmd.config, err = NewTestConfig()

		if err != nil {
			return fmt.Errorf("error parsing default config: %s\n", err)
		}

		log.Println("no configuration provided, using default settings")
	}

	// Override config properties.
	if cmd.hostname != "" {
		cmd.config.Hostname = cmd.hostname
	}
	cmd.node.hostname = cmd.config.Hostname
	return nil
}

func (cmd *RunCommand) Run(args ...string) error {
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
	fmt.Print(logo)
	writePIDFile(pidfile)

	// Set parallelism.
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.Printf("GOMAXPROCS set to %d", runtime.GOMAXPROCS(0))

	// Parse config
	if err := cmd.ParseConfig(configPath); err != nil {
		log.Fatal(err)
	}

	// Use the config JoinURLs by default
	joinURLs := cmd.config.Initialization.JoinURLs

	// If a -join flag was passed, these should override the config
	if join != "" {
		joinURLs = join
	}
	cmd.CheckConfig()
	cmd.Open(cmd.config, joinURLs)

	// Wait indefinitely.
	<-(chan struct{})(nil)
	return nil
}

// CheckConfig validates the configuration
func (cmd *RunCommand) CheckConfig() {
	// Set any defaults that aren't set
	// TODO: bring more defaults in here instead of letting helpers do it

	// Normalize Graphite configs
	for i, _ := range cmd.config.Graphites {
		if cmd.config.Graphites[i].BindAddress == "" {
			cmd.config.Graphites[i].BindAddress = cmd.config.BindAddress
		}
		if cmd.config.Graphites[i].Port == 0 {
			cmd.config.Graphites[i].Port = graphite.DefaultGraphitePort
		}
	}

	// Normalize openTSDB config
	if cmd.config.OpenTSDB.Addr == "" {
		cmd.config.OpenTSDB.Addr = cmd.config.BindAddress
	}

	if cmd.config.OpenTSDB.Port == 0 {
		cmd.config.OpenTSDB.Port = opentsdb.DefaultPort
	}

	// Validate that we have a sane config
	if !(cmd.config.Data.Enabled || cmd.config.Broker.Enabled) {
		log.Fatal("Node must be configured as a broker node, data node, or as both.  Run `influxd config` to generate a valid configuration.")
	}

	if cmd.config.Broker.Enabled && cmd.config.Broker.Dir == "" {
		log.Fatal("Broker.Dir must be specified.  Run `influxd config` to generate a valid configuration.")
	}

	if cmd.config.Data.Enabled && cmd.config.Data.Dir == "" {
		log.Fatal("Data.Dir must be specified.  Run `influxd config` to generate a valid configuration.")
	}
}

func (cmd *RunCommand) Open(config *Config, join string) *Node {
	if config != nil {
		cmd.config = config
	}

	cmd.node = NewNodeWithConfig(cmd.config)

	log.Printf("influxdb started, version %s, commit %s", version, commit)

	// Parse join urls from the --join flag.
	joinURLs := parseURLs(join)

	// Start the broker handler.
	h := &Handler{Config: config}
	if err := cmd.node.openClusterListener(cmd.config.ClusterAddr(), h); err != nil {
		log.Fatalf("Cluster server failed to listen on %s. %s ", cmd.config.ClusterAddr(), err)
	}
	log.Printf("Cluster server listening on %s", cmd.node.ClusterAddr().String())

	// Open broker & raft log, initialize or join as necessary.
	if cmd.config.Broker.Enabled {
		cmd.node.openBroker(joinURLs, h)
		// If were running as a broker locally, always connect to it since it must
		// be ready before we can start the data node.
		joinURLs = []url.URL{*cmd.node.ClusterURL()}
	}

	var s *influxdb.Server
	// Open server, initialize or join as necessary.
	if cmd.config.Data.Enabled {

		//FIXME: Need to also pass in dataURLs to bootstrap a data node
		s = cmd.node.openServer(joinURLs)
		cmd.node.DataNode = s
		s.SetAuthenticationEnabled(cmd.config.Authentication.Enabled)
		log.Printf("authentication enabled: %v\n", cmd.config.Authentication.Enabled)

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
		h.Server = s
		if config.Snapshot.Enabled {
			log.Printf("snapshot server listening on %s", cmd.config.ClusterAddr())
		} else {
			log.Printf("snapshot server disabled")
		}

		if cmd.config.Admin.Enabled {
			if err := cmd.node.openAdminServer(cmd.config.Admin.Port); err != nil {
				log.Fatalf("admin server failed to listen on :%d: %s", cmd.config.Admin.Port, err)
			}
			log.Printf("admin server listening on :%d", cmd.config.Admin.Port)
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
			log.Printf("Starting UDP listener on %s", cmd.config.APIAddrUDP())
			u := udp.NewUDPServer(s)
			if err := u.ListenAndServe(cmd.config.APIAddrUDP()); err != nil {
				log.Printf("Failed to start UDP listener on %s: %s", cmd.config.APIAddrUDP(), err)
			}

		}

		// Spin up any Graphite servers
		for _, graphiteConfig := range cmd.config.Graphites {
			if !graphiteConfig.Enabled {
				continue
			}

			// Configure Graphite parsing.
			parser := graphite.NewParser()
			parser.Separator = graphiteConfig.NameSeparatorString()
			parser.LastEnabled = graphiteConfig.LastEnabled()

			if err := s.CreateDatabaseIfNotExists(graphiteConfig.DatabaseString()); err != nil {
				log.Fatalf("failed to create database for %s Graphite server: %s", graphiteConfig.Protocol, err.Error())
			}

			// Spin up the server.
			var g graphite.Server
			g, err := graphite.NewServer(graphiteConfig.Protocol, parser, s, graphiteConfig.DatabaseString())
			if err != nil {
				log.Fatalf("failed to initialize %s Graphite server: %s", graphiteConfig.Protocol, err.Error())
			}

			err = g.ListenAndServe(graphiteConfig.ConnectionString())
			if err != nil {
				log.Fatalf("failed to start %s Graphite server: %s", graphiteConfig.Protocol, err.Error())
			}
			cmd.node.GraphiteServers = append(cmd.node.GraphiteServers, g)
		}

		// Spin up any OpenTSDB servers
		if config.OpenTSDB.Enabled {
			o := config.OpenTSDB
			db := o.DatabaseString()
			laddr := o.ListenAddress()
			policy := o.RetentionPolicy

			if err := s.CreateDatabaseIfNotExists(db); err != nil {
				log.Fatalf("failed to create database for OpenTSDB server: %s", err.Error())
			}

			if policy != "" {
				// Ensure retention policy exists.
				rp := influxdb.NewRetentionPolicy(policy)
				if err := s.CreateRetentionPolicyIfNotExists(db, rp); err != nil {
					log.Fatalf("failed to create retention policy for OpenTSDB: %s", err.Error())
				}
			}

			os := opentsdb.NewServer(s, policy, db)

			log.Println("Starting OpenTSDB service on", laddr)
			go os.ListenAndServe(laddr)
			cmd.node.OpenTSDBServer = os
		}

		// Start up self-monitoring if enabled.
		if cmd.config.Monitoring.Enabled {
			database := monitoringDatabase
			policy := monitoringRetentionPolicy
			interval := time.Duration(cmd.config.Monitoring.WriteInterval)

			// Ensure database exists.
			if err := s.CreateDatabaseIfNotExists(database); err != nil {
				log.Fatalf("failed to create database %s for internal monitoring: %s", database, err.Error())
			}

			// Ensure retention policy exists.
			rp := influxdb.NewRetentionPolicy(policy)
			if err := s.CreateRetentionPolicyIfNotExists(database, rp); err != nil {
				log.Fatalf("failed to create retention policy for internal monitoring: %s", err.Error())
			}

			s.StartSelfMonitoring(database, policy, interval)
			log.Printf("started self-monitoring at interval of %s", interval)
		}
	}

	// unless disabled, start the loop to report anonymous usage stats every 24h
	if !cmd.config.ReportingDisabled {
		if cmd.config.Broker.Enabled && cmd.config.Data.Enabled {
			// Make sure we have a config object b4 we try to use it.
			if clusterID := cmd.node.Broker.Broker.ClusterID(); clusterID != 0 {
				go s.StartReportingLoop(clusterID)
			}
		} else {
			log.Fatalln("failed to start reporting because not running as a broker and a data node")
		}
	}

	if cmd.node.Broker != nil {
		// have it occasionally tell a data node in the cluster to run continuous queries
		if cmd.config.ContinuousQuery.Disabled {
			log.Printf("Not running continuous queries. [continuous_queries].disabled is set to true.")
		} else {
			cmd.node.Broker.RunContinuousQueryLoop()
		}
	}

	if cmd.config.APIAddr() != cmd.config.ClusterAddr() {
		err := cmd.node.openAPIListener(cmd.config.APIAddr(), h)
		if err != nil {
			log.Fatalf("API server failed to listen on %s. %s ", cmd.config.APIAddr(), err)
		}
	}
	log.Printf("API server listening on %s", cmd.config.APIAddr())

	return cmd.node
}

func (cmd *RunCommand) Close() {
	cmd.node.Close()
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
