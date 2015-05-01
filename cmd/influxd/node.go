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
	"github.com/influxdb/influxdb/graphite"
	"github.com/influxdb/influxdb/messaging"
	"github.com/influxdb/influxdb/opentsdb"
	"github.com/influxdb/influxdb/raft"
)

// Node represent a member of a cluster.  A Node could serve as broker, a data node
// or both.
type Node struct {
	Broker   *influxdb.Broker
	DataNode *influxdb.Server
	raftLog  *raft.Log
	config   *Config

	hostname string

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

	h := net.JoinHostPort(n.hostname, p)
	return &url.URL{
		Scheme: "http",
		Host:   h,
	}
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
