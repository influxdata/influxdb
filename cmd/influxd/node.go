package main

import (
	"log"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/admin"
	"github.com/influxdb/influxdb/collectd"
	"github.com/influxdb/influxdb/graphite"
	"github.com/influxdb/influxdb/messaging"
	"github.com/influxdb/influxdb/opentsdb"
	"github.com/influxdb/influxdb/raft"
	"github.com/influxdb/influxdb/udp"
)

// Node represent a member of a cluster.  A Node could serve as broker, a data node
// or both.
type Node struct {
	Broker   *influxdb.Broker
	DataNode *influxdb.Server
	raftLog  *raft.Log
	config   *Config

	adminServer     *admin.Server
	clusterListener net.Listener      // The cluster TCP listener
	apiListener     net.Listener      // The API TCP listener
	GraphiteServers []graphite.Server // The Graphite Servers
	OpenTSDBServer  *opentsdb.Server  // The OpenTSDB Server
}

func NewNodeWithConfig(config *Config) *Node {
	return &Node{config: config}
}

// ClusterAddr returns the cluster listen address
func (n *Node) ClusterAddr() net.Addr {
	return n.clusterListener.Addr()
}

// ClusterURL returns the URL where other members can reach this node
func (n *Node) ClusterURL() *url.URL {
	// Find out which port the cluster started on
	_, p, e := net.SplitHostPort(n.ClusterAddr().String())
	if e != nil {
		panic(e)
	}

	h := net.JoinHostPort(n.config.Hostname, p)
	return &url.URL{
		Scheme: "http",
		Host:   h,
	}
}

func (n *Node) Open(join string) *Node {
	log.Printf("influxdb started, version %s, commit %s", version, commit)

	// Parse join urls from the --join flag.
	joinURLs := parseURLs(join)

	// Start the broker handler.
	h := &Handler{Config: n.config}
	if err := n.openClusterListener(n.config.ClusterAddr(), h); err != nil {
		log.Fatalf("Cluster server failed to listen on %s. %s ", n.config.ClusterAddr(), err)
	}
	log.Printf("Cluster server listening on %s", n.ClusterAddr().String())

	// Open broker & raft log, initialize or join as necessary.
	if n.config.Broker.Enabled {
		n.openBroker(joinURLs, h)
		// If were running as a broker locally, always connect to it since it must
		// be ready before we can start the data node.
		joinURLs = []url.URL{*n.ClusterURL()}
	}

	var s *influxdb.Server
	// Open server, initialize or join as necessary.
	if n.config.Data.Enabled {

		//FIXME: Need to also pass in dataURLs to bootstrap a data node
		s = n.openServer(joinURLs)
		n.DataNode = s
		s.SetAuthenticationEnabled(n.config.Authentication.Enabled)
		log.Printf("authentication enabled: %v\n", n.config.Authentication.Enabled)

		// Enable retention policy enforcement if requested.
		if n.config.Data.RetentionCheckEnabled {
			interval := time.Duration(n.config.Data.RetentionCheckPeriod)
			if err := s.StartRetentionPolicyEnforcement(interval); err != nil {
				log.Fatalf("retention policy enforcement failed: %s", err.Error())
			}
			log.Printf("broker enforcing retention policies with check interval of %s", interval)
		}

		// Start shard group pre-create
		interval := n.config.ShardGroupPreCreateCheckPeriod()
		if err := s.StartShardGroupsPreCreate(interval); err != nil {
			log.Fatalf("shard group pre-create failed: %s", err.Error())
		}
		log.Printf("shard group pre-create with check interval of %s", interval)
	}

	// Start the server handler. Attach to broker if listening on the same port.
	if s != nil {
		h.Server = s
		if n.config.Snapshot.Enabled {
			log.Printf("snapshot server listening on %s", n.config.ClusterAddr())
		} else {
			log.Printf("snapshot server disabled")
		}

		if n.config.Admin.Enabled {
			if err := n.openAdminServer(n.config.Admin.Port); err != nil {
				log.Fatalf("admin server failed to listen on :%d: %s", n.config.Admin.Port, err)
			}
			log.Printf("admin server listening on :%d", n.config.Admin.Port)
		}

		// Spin up the collectd server
		if n.config.Collectd.Enabled {
			c := n.config.Collectd
			cs := collectd.NewServer(s, c.TypesDB)
			cs.Database = c.Database
			err := collectd.ListenAndServe(cs, c.ConnectionString(n.config.BindAddress))
			if err != nil {
				log.Printf("failed to start collectd Server: %v\n", err.Error())
			}
		}

		// Start the server bound to a UDP listener
		if n.config.UDP.Enabled {
			log.Printf("Starting UDP listener on %s", n.config.APIAddrUDP())
			u := udp.NewUDPServer(s)
			if err := u.ListenAndServe(n.config.APIAddrUDP()); err != nil {
				log.Printf("Failed to start UDP listener on %s: %s", n.config.APIAddrUDP(), err)
			}

		}

		// Spin up any Graphite servers
		for _, graphiteConfig := range n.config.Graphites {
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
			n.GraphiteServers = append(n.GraphiteServers, g)
		}

		// Spin up any OpenTSDB servers
		if n.config.OpenTSDB.Enabled {
			o := n.config.OpenTSDB
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
			n.OpenTSDBServer = os
		}

		// Start up self-monitoring if enabled.
		if n.config.Monitoring.Enabled {
			database := monitoringDatabase
			policy := monitoringRetentionPolicy
			interval := time.Duration(n.config.Monitoring.WriteInterval)

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
	if !n.config.ReportingDisabled {
		if n.config.Broker.Enabled && n.config.Data.Enabled {
			// Make sure we have a config object b4 we try to use it.
			if clusterID := n.Broker.Broker.ClusterID(); clusterID != 0 {
				go s.StartReportingLoop(clusterID)
			}
		} else {
			log.Fatalln("failed to start reporting because not running as a broker and a data node")
		}
	}

	if n.Broker != nil {
		// have it occasionally tell a data node in the cluster to run continuous queries
		if n.config.ContinuousQuery.Disabled {
			log.Printf("Not running continuous queries. [continuous_queries].disabled is set to true.")
		} else {
			n.Broker.RunContinuousQueryLoop()
		}
	}

	if n.config.APIAddr() != n.config.ClusterAddr() {
		err := n.openAPIListener(n.config.APIAddr(), h)
		if err != nil {
			log.Fatalf("API server failed to listen on %s. %s ", n.config.APIAddr(), err)
		}
	}
	log.Printf("API server listening on %s", n.config.APIAddr())

	return n
}

// creates and initializes a broker.
func (n *Node) openBroker(brokerURLs []url.URL, h *Handler) {
	path := n.config.BrokerDir()
	u := n.ClusterURL()
	raftTracing := n.config.Logging.RaftTracing

	// Create broker
	b := influxdb.NewBroker()
	b.TruncationInterval = time.Duration(n.config.Broker.TruncationInterval)
	b.MaxTopicSize = n.config.Broker.MaxTopicSize
	b.MaxSegmentSize = n.config.Broker.MaxSegmentSize
	n.Broker = b

	// Create raft log.
	l := raft.NewLog()
	l.SetURL(*u)
	l.DebugEnabled = raftTracing
	b.Log = l
	n.raftLog = l

	// Create Raft clock.
	clk := raft.NewClock()
	clk.ApplyInterval = time.Duration(n.config.Raft.ApplyInterval)
	clk.ElectionTimeout = time.Duration(n.config.Raft.ElectionTimeout)
	clk.HeartbeatInterval = time.Duration(n.config.Raft.HeartbeatInterval)
	clk.ReconnectTimeout = time.Duration(n.config.Raft.ReconnectTimeout)
	l.Clock = clk

	// Open broker so it can feed last index data to the log.
	if err := b.Open(path); err != nil {
		log.Fatalf("failed to open broker at %s : %s", path, err)
	}
	log.Printf("broker opened at %s", path)

	// Attach the broker as the finite state machine of the raft log.
	l.FSM = &messaging.RaftFSM{Broker: b}

	// Open raft log inside broker directory.
	if err := l.Open(filepath.Join(path, "raft")); err != nil {
		log.Fatalf("raft: %s", err)
	}

	// Attach broker and log to handler.
	h.Broker = b
	h.Log = l

	// Checks to see if the raft index is 0.  If it's 0, it might be the first
	// node in the cluster and must initialize or join
	index, _ := l.LastLogIndexTerm()
	if index == 0 {
		// If we have join URLs, then attemp to join the cluster
		if len(brokerURLs) > 0 {
			n.joinLog(l, brokerURLs)
			return
		}

		if err := l.Initialize(); err != nil {
			log.Fatalf("initialize raft log: %s", err)
		}

		u := b.Broker.URL()
		log.Printf("initialized broker: %s\n", (&u).String())
	} else {
		log.Printf("broker already member of cluster.  Using existing state and ignoring join URLs")
	}
}

// joins a raft log to an existing cluster.
func (n *Node) joinLog(l *raft.Log, brokerURLs []url.URL) {
	// Attempts to join each server until successful.
	for _, u := range brokerURLs {
		if err := l.Join(u); err == raft.ErrInitialized {
			return
		} else if err != nil {
			log.Printf("join: failed to connect to raft cluster: %s: %s", (&u).String(), err)
		} else {
			log.Printf("join: connected raft log to %s", (&u).String())
			return
		}
	}
	log.Fatalf("join: failed to connect raft log to any specified server")
}

// Close stops all listeners and services on the node
func (n *Node) Close() error {
	if err := n.closeClusterListener(); err != nil {
		return err
	}

	if err := n.closeAPIListener(); err != nil {
		return err
	}

	if err := n.closeAdminServer(); err != nil {
		return err
	}

	for _, g := range n.GraphiteServers {
		if err := g.Close(); err != nil {
			return err
		}
	}

	if n.OpenTSDBServer != nil {
		if err := n.OpenTSDBServer.Close(); err != nil {
			return err
		}
	}

	if n.DataNode != nil {
		if err := n.DataNode.Close(); err != nil {
			return err
		}
	}

	if n.raftLog != nil {
		if err := n.raftLog.Close(); err != nil {
			return err
		}
	}

	if n.Broker != nil {
		if err := n.Broker.Close(); err != nil {
			return err
		}
	}

	return nil
}

// creates and initializes a server.
func (n *Node) openServer(joinURLs []url.URL) *influxdb.Server {

	// Create messaging client to the brokers.
	c := influxdb.NewMessagingClient(*n.ClusterURL())
	c.SetURLs(joinURLs)

	if err := c.Open(filepath.Join(n.config.Data.Dir, messagingClientFile)); err != nil {
		log.Fatalf("messaging client error: %s", err)
	}

	// If no URLs exist on the client the return an error since we cannot reach a broker.
	if len(c.URLs()) == 0 {
		log.Fatal("messaging client has no broker URLs")
	}

	// Create and open the server.
	s := influxdb.NewServer()

	s.WriteTrace = n.config.Logging.WriteTracing
	s.RetentionAutoCreate = n.config.Data.RetentionAutoCreate
	s.RecomputePreviousN = n.config.ContinuousQuery.RecomputePreviousN
	s.RecomputeNoOlderThan = time.Duration(n.config.ContinuousQuery.RecomputeNoOlderThan)
	s.ComputeRunsPerInterval = n.config.ContinuousQuery.ComputeRunsPerInterval
	s.ComputeNoMoreThan = time.Duration(n.config.ContinuousQuery.ComputeNoMoreThan)
	s.Version = version
	s.CommitHash = commit

	// Open server with data directory and broker client.
	if err := s.Open(n.config.Data.Dir, c); err != nil {
		log.Fatalf("failed to open data node: %v", err.Error())
	}
	log.Printf("data node(%d) opened at %s", s.ID(), n.config.Data.Dir)

	// Give brokers time to elect a leader if entire cluster is being restarted.
	time.Sleep(1 * time.Second)

	if s.ID() == 0 {
		n.joinOrInitializeServer(s, *n.ClusterURL(), joinURLs)
	} else {
		log.Printf("data node already member of cluster. Using existing state and ignoring join URLs")
	}

	return s
}

// joinOrInitializeServer joins a new server to an existing cluster or initializes it as the first
// member of the cluster
func (n *Node) joinOrInitializeServer(s *influxdb.Server, u url.URL, joinURLs []url.URL) {
	// Create data node on an existing data node.
	for _, joinURL := range joinURLs {
		if err := s.Join(&u, &joinURL); err == influxdb.ErrDataNodeNotFound {
			// No data nodes could be found to join.  We're the first.
			if err := s.Initialize(u); err != nil {
				log.Fatalf("server initialization error(1): %s", err)
			}
			log.Printf("initialized data node: %s\n", (&u).String())
			return
		} else if err != nil {
			// does not return so that the next joinURL can be tried
			log.Printf("join: failed to connect data node: %s: %s", (&u).String(), err)
		} else {
			log.Printf("join: connected data node to %s", u)
			return
		}
	}

	if len(joinURLs) == 0 {
		if err := s.Initialize(u); err != nil {
			log.Fatalf("server initialization error(2): %s", err)
		}
		log.Printf("initialized data node: %s\n", (&u).String())
		return
	}

	log.Fatalf("join: failed to connect data node to any specified server")
}

func (n *Node) openAdminServer(port int) error {
	// Start the admin interface on the default port
	addr := net.JoinHostPort("", strconv.Itoa(port))
	n.adminServer = admin.NewServer(addr)
	return n.adminServer.ListenAndServe()
}

func (n *Node) closeAdminServer() error {
	if n.adminServer != nil {
		return n.adminServer.Close()
	}
	return nil
}

func (n *Node) openListener(desc, addr string, h http.Handler) (net.Listener, error) {
	var err error
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	go func() {
		err := http.Serve(listener, h)

		// The listener was closed so exit
		// See https://github.com/golang/go/issues/4373
		if strings.Contains(err.Error(), "closed") {
			return
		}
		if err != nil {
			log.Fatalf("%s server failed to serve on %s: %s", desc, addr, err)
		}
	}()
	return listener, nil

}

func (n *Node) openAPIListener(addr string, h http.Handler) error {
	var err error
	n.apiListener, err = n.openListener("API", addr, h)
	if err != nil {
		return err
	}
	return nil
}

func (n *Node) closeAPIListener() error {
	var err error
	if n.apiListener != nil {
		err = n.apiListener.Close()
		n.apiListener = nil
	}
	return err
}

func (n *Node) openClusterListener(addr string, h http.Handler) error {
	var err error
	n.clusterListener, err = n.openListener("Cluster", addr, h)
	if err != nil {
		return err
	}
	return nil
}

func (n *Node) closeClusterListener() error {
	var err error
	if n.clusterListener != nil {
		err = n.clusterListener.Close()
		n.clusterListener = nil
	}
	return err
}
