package run

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cluster"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/services/admin"
	"github.com/influxdata/influxdb/services/collectd"
	"github.com/influxdata/influxdb/services/continuous_querier"
	"github.com/influxdata/influxdb/services/copier"
	"github.com/influxdata/influxdb/services/graphite"
	"github.com/influxdata/influxdb/services/hh"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/opentsdb"
	"github.com/influxdata/influxdb/services/precreator"
	"github.com/influxdata/influxdb/services/retention"
	"github.com/influxdata/influxdb/services/snapshotter"
	"github.com/influxdata/influxdb/services/subscriber"
	"github.com/influxdata/influxdb/services/udp"
	"github.com/influxdata/influxdb/tcp"
	"github.com/influxdata/influxdb/tsdb"
	client "github.com/influxdata/usage-client/v1"
	// Initialize the engine packages
	_ "github.com/influxdata/influxdb/tsdb/engine"
)

// BuildInfo represents the build details for the server code.
type BuildInfo struct {
	Version string
	Commit  string
	Branch  string
	Time    string
}

// Server represents a container for the metadata and storage data and services.
// It is built using a Config and it manages the startup and shutdown of all
// services in the proper order.
type Server struct {
	buildInfo BuildInfo

	err     chan error
	closing chan struct{}

	BindAddress string
	Listener    net.Listener

	Node *influxdb.Node

	MetaClient  *meta.Client
	MetaService *meta.Service

	TSDBStore       *tsdb.Store
	QueryExecutor   *cluster.QueryExecutor
	PointsWriter    *cluster.PointsWriter
	ShardWriter     *cluster.ShardWriter
	IteratorCreator *cluster.IteratorCreator
	HintedHandoff   *hh.Service
	Subscriber      *subscriber.Service

	Services []Service

	// These references are required for the tcp muxer.
	ClusterService     *cluster.Service
	SnapshotterService *snapshotter.Service
	CopierService      *copier.Service

	Monitor *monitor.Monitor

	// Server reporting and registration
	reportingDisabled bool

	// Profiling
	CPUProfile string
	MemProfile string

	// joinPeers are the metaservers specified at run time to join this server to
	joinPeers []string

	// metaUseTLS specifies if we should use a TLS connection to the meta servers
	metaUseTLS bool

	// httpAPIAddr is the host:port combination for the main HTTP API for querying and writing data
	httpAPIAddr string

	// httpUseTLS specifies if we should use a TLS connection to the http servers
	httpUseTLS bool

	// tcpAddr is the host:port combination for the TCP listener that services mux onto
	tcpAddr string

	config *Config
}

// NewServer returns a new instance of Server built from a config.
func NewServer(c *Config, buildInfo *BuildInfo) (*Server, error) {
	// We need to ensure that a meta directory always exists even if
	// we don't start the meta store.  node.json is always stored under
	// the meta directory.
	if err := os.MkdirAll(c.Meta.Dir, 0777); err != nil {
		return nil, fmt.Errorf("mkdir all: %s", err)
	}

	// 0.10-rc1 and prior would sometimes put the node.json at the root
	// dir which breaks backup/restore and restarting nodes.  This moves
	// the file from the root so it's always under the meta dir.
	oldPath := filepath.Join(filepath.Dir(c.Meta.Dir), "node.json")
	newPath := filepath.Join(c.Meta.Dir, "node.json")

	if _, err := os.Stat(oldPath); err == nil {
		if err := os.Rename(oldPath, newPath); err != nil {
			return nil, err
		}
	}

	// 0.11 we no longer use peers.json.  Remove the file if we have one on disk.
	os.RemoveAll(filepath.Join(c.Meta.Dir, "peers.json"))

	node, err := influxdb.LoadNode(c.Meta.Dir)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		node = influxdb.NewNode(c.Meta.Dir)
	}

	// In 0.11 we removed MetaServers from node.json.  To avoid confusion for
	// existing users, force a re-save of the node.json file to remove that property
	// if it happens to exist.
	nodeContents, err := ioutil.ReadFile(filepath.Join(c.Meta.Dir, "node.json"))
	if err == nil && strings.Contains(string(nodeContents), "MetaServers") {
		node.Save()
	}

	// In 0.10.0 bind-address got moved to the top level. Check
	// The old location to keep things backwards compatible
	bind := c.BindAddress
	if c.Meta.BindAddress != "" {
		bind = c.Meta.BindAddress
	}

	if !c.Data.Enabled && !c.Meta.Enabled {
		return nil, fmt.Errorf("must run as either meta node or data node or both")
	}

	s := &Server{
		buildInfo: *buildInfo,
		err:       make(chan error),
		closing:   make(chan struct{}),

		BindAddress: bind,

		Node:       node,
		MetaClient: meta.NewClient(),

		Monitor: monitor.New(c.Monitor),

		reportingDisabled: c.ReportingDisabled,
		joinPeers:         c.Meta.JoinPeers,
		metaUseTLS:        c.Meta.HTTPSEnabled,

		httpAPIAddr: c.HTTPD.BindAddress,
		httpUseTLS:  c.HTTPD.HTTPSEnabled,
		tcpAddr:     bind,

		config: c,
	}

	if c.Meta.Enabled {
		s.MetaService = meta.NewService(c.Meta)
		s.MetaService.Version = s.buildInfo.Version
		s.MetaService.Node = s.Node
	}

	if c.Data.Enabled {
		s.TSDBStore = tsdb.NewStore(c.Data.Dir)
		s.TSDBStore.EngineOptions.Config = c.Data

		// Copy TSDB configuration.
		s.TSDBStore.EngineOptions.EngineVersion = c.Data.Engine
		s.TSDBStore.EngineOptions.MaxWALSize = c.Data.MaxWALSize
		s.TSDBStore.EngineOptions.WALFlushInterval = time.Duration(c.Data.WALFlushInterval)
		s.TSDBStore.EngineOptions.WALPartitionFlushDelay = time.Duration(c.Data.WALPartitionFlushDelay)

		// Set the shard writer
		s.ShardWriter = cluster.NewShardWriter(time.Duration(c.Cluster.ShardWriterTimeout),
			c.Cluster.MaxRemoteWriteConnections)

		// Create the hinted handoff service
		s.HintedHandoff = hh.NewService(c.HintedHandoff, s.ShardWriter, s.MetaClient)
		s.HintedHandoff.Monitor = s.Monitor

		// Create the Subscriber service
		s.Subscriber = subscriber.NewService(c.Subscriber)

		// Initialize points writer.
		s.PointsWriter = cluster.NewPointsWriter()
		s.PointsWriter.WriteTimeout = time.Duration(c.Cluster.WriteTimeout)
		s.PointsWriter.TSDBStore = s.TSDBStore
		s.PointsWriter.ShardWriter = s.ShardWriter
		s.PointsWriter.HintedHandoff = s.HintedHandoff
		s.PointsWriter.Subscriber = s.Subscriber
		s.PointsWriter.Node = s.Node

		// Initialize meta executor.
		metaExecutor := cluster.NewMetaExecutor()
		metaExecutor.MetaClient = s.MetaClient
		metaExecutor.Node = s.Node

		// Initialize query executor.
		s.QueryExecutor = cluster.NewQueryExecutor()
		s.QueryExecutor.MetaClient = s.MetaClient
		s.QueryExecutor.TSDBStore = s.TSDBStore
		s.QueryExecutor.Monitor = s.Monitor
		s.QueryExecutor.PointsWriter = s.PointsWriter
		s.QueryExecutor.MetaExecutor = metaExecutor
		if c.Data.QueryLogEnabled {
			s.QueryExecutor.LogOutput = os.Stderr
		}

		// Initialize the monitor
		s.Monitor.Version = s.buildInfo.Version
		s.Monitor.Commit = s.buildInfo.Commit
		s.Monitor.Branch = s.buildInfo.Branch
		s.Monitor.BuildTime = s.buildInfo.Time
		s.Monitor.PointsWriter = (*monitorPointsWriter)(s.PointsWriter)
	}

	return s, nil
}

func (s *Server) appendClusterService(c cluster.Config) {
	srv := cluster.NewService(c)
	srv.TSDBStore = s.TSDBStore
	srv.MetaClient = s.MetaClient
	s.Services = append(s.Services, srv)
	s.ClusterService = srv
}

func (s *Server) appendSnapshotterService() {
	srv := snapshotter.NewService()
	srv.TSDBStore = s.TSDBStore
	srv.MetaClient = s.MetaClient
	srv.Node = s.Node
	s.Services = append(s.Services, srv)
	s.SnapshotterService = srv
}

func (s *Server) appendCopierService() {
	srv := copier.NewService()
	srv.TSDBStore = s.TSDBStore
	s.Services = append(s.Services, srv)
	s.CopierService = srv
}

func (s *Server) appendRetentionPolicyService(c retention.Config) {
	if !c.Enabled {
		return
	}
	srv := retention.NewService(c)
	srv.MetaClient = s.MetaClient
	srv.TSDBStore = s.TSDBStore
	s.Services = append(s.Services, srv)
}

func (s *Server) appendAdminService(c admin.Config) {
	if !c.Enabled {
		return
	}
	srv := admin.NewService(c)
	s.Services = append(s.Services, srv)
}

func (s *Server) appendHTTPDService(c httpd.Config) {
	if !c.Enabled {
		return
	}
	srv := httpd.NewService(c)
	srv.Handler.MetaClient = s.MetaClient
	srv.Handler.QueryAuthorizer = meta.NewQueryAuthorizer(s.MetaClient)
	srv.Handler.QueryExecutor = s.QueryExecutor
	srv.Handler.PointsWriter = s.PointsWriter
	srv.Handler.Version = s.buildInfo.Version

	// If a ContinuousQuerier service has been started, attach it.
	for _, srvc := range s.Services {
		if cqsrvc, ok := srvc.(continuous_querier.ContinuousQuerier); ok {
			srv.Handler.ContinuousQuerier = cqsrvc
		}
	}

	s.Services = append(s.Services, srv)
}

func (s *Server) appendCollectdService(c collectd.Config) {
	if !c.Enabled {
		return
	}
	srv := collectd.NewService(c)
	srv.MetaClient = s.MetaClient
	srv.PointsWriter = s.PointsWriter
	s.Services = append(s.Services, srv)
}

func (s *Server) appendOpenTSDBService(c opentsdb.Config) error {
	if !c.Enabled {
		return nil
	}
	srv, err := opentsdb.NewService(c)
	if err != nil {
		return err
	}
	srv.PointsWriter = s.PointsWriter
	srv.MetaClient = s.MetaClient
	s.Services = append(s.Services, srv)
	return nil
}

func (s *Server) appendGraphiteService(c graphite.Config) error {
	if !c.Enabled {
		return nil
	}
	srv, err := graphite.NewService(c)
	if err != nil {
		return err
	}

	srv.PointsWriter = s.PointsWriter
	srv.MetaClient = s.MetaClient
	srv.Monitor = s.Monitor
	s.Services = append(s.Services, srv)
	return nil
}

func (s *Server) appendPrecreatorService(c precreator.Config) error {
	if !c.Enabled {
		return nil
	}
	srv, err := precreator.NewService(c)
	if err != nil {
		return err
	}

	srv.MetaClient = s.MetaClient
	s.Services = append(s.Services, srv)
	return nil
}

func (s *Server) appendUDPService(c udp.Config) {
	if !c.Enabled {
		return
	}
	srv := udp.NewService(c)
	srv.PointsWriter = s.PointsWriter
	srv.MetaClient = s.MetaClient
	s.Services = append(s.Services, srv)
}

func (s *Server) appendContinuousQueryService(c continuous_querier.Config) {
	if !c.Enabled {
		return
	}
	srv := continuous_querier.NewService(c)
	srv.MetaClient = s.MetaClient
	srv.QueryExecutor = s.QueryExecutor
	s.Services = append(s.Services, srv)
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return s.err }

// Open opens the meta and data store and all services.
func (s *Server) Open() error {
	// Start profiling, if set.
	startProfile(s.CPUProfile, s.MemProfile)

	// Open shared TCP connection.
	ln, err := net.Listen("tcp", s.BindAddress)
	if err != nil {
		return fmt.Errorf("listen: %s", err)
	}
	s.Listener = ln

	// Multiplex listener.
	mux := tcp.NewMux()
	go mux.Serve(ln)

	if s.MetaService != nil {
		s.MetaService.RaftListener = mux.Listen(meta.MuxHeader)
		// Open meta service.
		if err := s.MetaService.Open(); err != nil {
			return fmt.Errorf("open meta service: %s", err)
		}
		go s.monitorErrorChan(s.MetaService.Err())
	}

	// initialize MetaClient.
	if err = s.initializeMetaClient(); err != nil {
		return err
	}

	if s.TSDBStore != nil {
		// Append services.
		s.appendClusterService(s.config.Cluster)
		s.appendPrecreatorService(s.config.Precreator)
		s.appendSnapshotterService()
		s.appendCopierService()
		s.appendAdminService(s.config.Admin)
		s.appendContinuousQueryService(s.config.ContinuousQuery)
		s.appendHTTPDService(s.config.HTTPD)
		s.appendCollectdService(s.config.Collectd)
		if err := s.appendOpenTSDBService(s.config.OpenTSDB); err != nil {
			return err
		}
		for _, g := range s.config.UDPs {
			s.appendUDPService(g)
		}
		s.appendRetentionPolicyService(s.config.Retention)
		for _, g := range s.config.Graphites {
			if err := s.appendGraphiteService(g); err != nil {
				return err
			}
		}

		s.Subscriber.MetaClient = s.MetaClient
		s.ShardWriter.MetaClient = s.MetaClient
		s.HintedHandoff.MetaClient = s.MetaClient
		s.Subscriber.MetaClient = s.MetaClient
		s.PointsWriter.MetaClient = s.MetaClient
		s.Monitor.MetaClient = s.MetaClient

		s.ClusterService.Listener = mux.Listen(cluster.MuxHeader)
		s.SnapshotterService.Listener = mux.Listen(snapshotter.MuxHeader)
		s.CopierService.Listener = mux.Listen(copier.MuxHeader)

		// Open TSDB store.
		if err := s.TSDBStore.Open(); err != nil {
			return fmt.Errorf("open tsdb store: %s", err)
		}

		// Open the hinted handoff service
		if err := s.HintedHandoff.Open(); err != nil {
			return fmt.Errorf("open hinted handoff: %s", err)
		}

		// Open the subcriber service
		if err := s.Subscriber.Open(); err != nil {
			return fmt.Errorf("open subscriber: %s", err)
		}

		// Open the points writer service
		if err := s.PointsWriter.Open(); err != nil {
			return fmt.Errorf("open points writer: %s", err)
		}

		// Open the monitor service
		if err := s.Monitor.Open(); err != nil {
			return fmt.Errorf("open monitor: %v", err)
		}

		for _, service := range s.Services {
			if err := service.Open(); err != nil {
				return fmt.Errorf("open service: %s", err)
			}
		}
	}

	// Start the reporting service, if not disabled.
	if !s.reportingDisabled {
		go s.startServerReporting()
	}

	return nil
}

// Close shuts down the meta and data stores and all services.
func (s *Server) Close() error {
	stopProfile()

	// Close the listener first to stop any new connections
	if s.Listener != nil {
		s.Listener.Close()
	}

	// Close services to allow any inflight requests to complete
	// and prevent new requests from being accepted.
	for _, service := range s.Services {
		service.Close()
	}

	if s.Monitor != nil {
		s.Monitor.Close()
	}

	if s.PointsWriter != nil {
		s.PointsWriter.Close()
	}

	if s.HintedHandoff != nil {
		s.HintedHandoff.Close()
	}

	// Close the TSDBStore, no more reads or writes at this point
	if s.TSDBStore != nil {
		s.TSDBStore.Close()
	}

	if s.Subscriber != nil {
		s.Subscriber.Close()
	}

	// Finally close the meta-store since everything else depends on it
	if s.MetaService != nil {
		s.MetaService.Close()
	}

	if s.MetaClient != nil {
		s.MetaClient.Close()
	}

	close(s.closing)
	return nil
}

// startServerReporting starts periodic server reporting.
func (s *Server) startServerReporting() {
	for {
		select {
		case <-s.closing:
			return
		default:
		}
		s.reportServer()
		<-time.After(24 * time.Hour)
	}
}

// reportServer reports anonymous statistics about the system.
func (s *Server) reportServer() {
	dis, err := s.MetaClient.Databases()
	if err != nil {
		log.Printf("failed to retrieve databases for reporting: %s", err.Error())
		return
	}
	numDatabases := len(dis)

	numMeasurements := 0
	numSeries := 0

	// Only needed in the case of a data node
	if s.TSDBStore != nil {
		for _, di := range dis {
			d := s.TSDBStore.DatabaseIndex(di.Name)
			if d == nil {
				// No data in this store for this database.
				continue
			}
			m, s := d.MeasurementSeriesCounts()
			numMeasurements += m
			numSeries += s
		}
	}

	clusterID := s.MetaClient.ClusterID()
	if err != nil {
		log.Printf("failed to retrieve cluster ID for reporting: %s", err.Error())
		return
	}

	cl := client.New("")
	usage := client.Usage{
		Product: "influxdb",
		Data: []client.UsageData{
			{
				Values: client.Values{
					"os":               runtime.GOOS,
					"arch":             runtime.GOARCH,
					"version":          s.buildInfo.Version,
					"server_id":        fmt.Sprintf("%v", s.Node.ID),
					"cluster_id":       fmt.Sprintf("%v", clusterID),
					"num_series":       numSeries,
					"num_measurements": numMeasurements,
					"num_databases":    numDatabases,
				},
			},
		},
	}

	log.Printf("Sending anonymous usage statistics to m.influxdb.com")

	go cl.Save(usage)
}

// monitorErrorChan reads an error channel and resends it through the server.
func (s *Server) monitorErrorChan(ch <-chan error) {
	for {
		select {
		case err, ok := <-ch:
			if !ok {
				return
			}
			s.err <- err
		case <-s.closing:
			return
		}
	}
}

// initializeMetaClient will set the MetaClient and join the node to the cluster if needed
func (s *Server) initializeMetaClient() error {
	// It's the first time starting up and we need to either join
	// the cluster or initialize this node as the first member
	if len(s.joinPeers) == 0 {
		// start up a new single node cluster
		if s.MetaService == nil {
			return fmt.Errorf("server not set to join existing cluster must run also as a meta node")
		}
		s.MetaClient.SetMetaServers([]string{s.MetaService.HTTPAddr()})
		s.MetaClient.SetTLS(s.metaUseTLS)
	} else {
		// join this node to the cluster
		s.MetaClient.SetMetaServers(s.joinPeers)
		s.MetaClient.SetTLS(s.metaUseTLS)
	}
	if err := s.MetaClient.Open(); err != nil {
		return err
	}

	// if the node ID is > 0 then we need to initialize the metaclient
	if s.Node.ID > 0 {
		s.MetaClient.WaitForDataChanged()
	}

	if s.config.Data.Enabled {
		// If we've already created a data node for our id, we're done
		if _, err := s.MetaClient.DataNode(s.Node.ID); err == nil {
			return nil
		}

		n, err := s.MetaClient.CreateDataNode(s.HTTPAddr(), s.TCPAddr())
		for err != nil {
			log.Printf("Unable to create data node. retry in 1s: %s", err.Error())
			time.Sleep(time.Second)
			n, err = s.MetaClient.CreateDataNode(s.HTTPAddr(), s.TCPAddr())
		}
		s.Node.ID = n.ID

		if err := s.Node.Save(); err != nil {
			return err
		}
	}

	return nil
}

// HTTPAddr returns the HTTP address used by other nodes for HTTP queries and writes.
func (s *Server) HTTPAddr() string {
	return s.remoteAddr(s.httpAPIAddr)
}

// TCPAddr returns the TCP address used by other nodes for cluster communication.
func (s *Server) TCPAddr() string {
	return s.remoteAddr(s.tcpAddr)
}

func (s *Server) remoteAddr(addr string) string {
	hostname := s.config.Hostname
	if hostname == "" {
		hostname = meta.DefaultHostname
	}
	remote, err := meta.DefaultHost(hostname, addr)
	if err != nil {
		return addr
	}
	return remote
}

// MetaServers returns the meta node HTTP addresses used by this server.
func (s *Server) MetaServers() []string {
	return s.MetaClient.MetaServers()
}

// Service represents a service attached to the server.
type Service interface {
	Open() error
	Close() error
}

// prof stores the file locations of active profiles.
var prof struct {
	cpu *os.File
	mem *os.File
}

// StartProfile initializes the cpu and memory profile, if specified.
func startProfile(cpuprofile, memprofile string) {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatalf("cpuprofile: %v", err)
		}
		log.Printf("writing CPU profile to: %s\n", cpuprofile)
		prof.cpu = f
		pprof.StartCPUProfile(prof.cpu)
	}

	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatalf("memprofile: %v", err)
		}
		log.Printf("writing mem profile to: %s\n", memprofile)
		prof.mem = f
		runtime.MemProfileRate = 4096
	}

}

// StopProfile closes the cpu and memory profiles if they are running.
func stopProfile() {
	if prof.cpu != nil {
		pprof.StopCPUProfile()
		prof.cpu.Close()
		log.Println("CPU profile stopped")
	}
	if prof.mem != nil {
		pprof.Lookup("heap").WriteTo(prof.mem, 0)
		prof.mem.Close()
		log.Println("mem profile stopped")
	}
}

type tcpaddr struct{ host string }

func (a *tcpaddr) Network() string { return "tcp" }
func (a *tcpaddr) String() string  { return a.host }

// monitorPointsWriter is a wrapper around `cluster.PointsWriter` that helps
// to prevent a circular dependency between the `cluster` and `monitor` packages.
type monitorPointsWriter cluster.PointsWriter

func (pw *monitorPointsWriter) WritePoints(database, retentionPolicy string, points models.Points) error {
	return (*cluster.PointsWriter)(pw).WritePoints(&cluster.WritePointsRequest{
		Database:         database,
		RetentionPolicy:  retentionPolicy,
		ConsistencyLevel: cluster.ConsistencyLevelOne,
		Points:           points,
	})
}
