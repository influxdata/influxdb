package run

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/dependencies/testing"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/coordinator"
	influxdb2 "github.com/influxdata/influxdb/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor"
	prometheus2 "github.com/influxdata/influxdb/prometheus"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/query/control"
	"github.com/influxdata/influxdb/services/collectd"
	"github.com/influxdata/influxdb/services/continuous_querier"
	"github.com/influxdata/influxdb/services/graphite"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/opentsdb"
	"github.com/influxdata/influxdb/services/precreator"
	"github.com/influxdata/influxdb/services/retention"
	"github.com/influxdata/influxdb/services/snapshotter"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/influxdb/services/subscriber"
	"github.com/influxdata/influxdb/services/udp"
	reads "github.com/influxdata/influxdb/storage/flux"
	"github.com/influxdata/influxdb/tcp"
	"github.com/influxdata/influxdb/tsdb"

	// Initialize the engine package
	_ "github.com/influxdata/influxdb/tsdb/engine"

	// Initialize the index package
	_ "github.com/influxdata/influxdb/tsdb/index"
	client "github.com/influxdata/usage-client/v1"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var startTime time.Time

func init() {
	startTime = time.Now().UTC()
}

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

	Logger    *zap.Logger
	MuxLogger *log.Logger

	MetaClient *meta.Client

	TSDBStore     *tsdb.Store
	QueryExecutor *query.Executor
	PointsWriter  *coordinator.PointsWriter
	Subscriber    *subscriber.Service

	Services []Service

	Prometheus *prometheus.Registry

	// These references are required for the tcp muxer.
	SnapshotterService *snapshotter.Service

	Monitor *monitor.Monitor

	// Server reporting and registration
	reportingDisabled bool

	// Profiling
	CPUProfile            string
	CPUProfileWriteCloser io.WriteCloser
	MemProfile            string
	MemProfileWriteCloser io.WriteCloser

	// httpAPIAddr is the host:port combination for the main HTTP API for querying and writing data
	httpAPIAddr string

	// httpUseTLS specifies if we should use a TLS connection to the http servers
	httpUseTLS bool

	// tcpAddr is the host:port combination for the TCP listener that services mux onto
	tcpAddr string

	config *Config
}

// updateTLSConfig stores with into the tls config pointed at by into but only if with is not nil
// and into is nil. Think of it as setting the default value.
func updateTLSConfig(into **tls.Config, with *tls.Config) {
	if with != nil && into != nil && *into == nil {
		*into = with
	}
}

// NewServer returns a new instance of Server built from a config.
func NewServer(c *Config, buildInfo *BuildInfo) (*Server, error) {
	// First grab the base tls config we will use for all clients and servers
	tlsConfig, err := c.TLS.Parse()
	if err != nil {
		return nil, fmt.Errorf("tls configuration: %v", err)
	}

	// Update the TLS values on each of the configs to be the parsed one if
	// not already specified (set the default).
	updateTLSConfig(&c.HTTPD.TLS, tlsConfig)
	updateTLSConfig(&c.Subscriber.TLS, tlsConfig)
	for i := range c.OpenTSDBInputs {
		updateTLSConfig(&c.OpenTSDBInputs[i].TLS, tlsConfig)
	}

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

	_, err = influxdb.LoadNode(c.Meta.Dir)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	}

	if err := raftDBExists(c.Meta.Dir); err != nil {
		return nil, err
	}

	// In 0.10.0 bind-address got moved to the top level. Check
	// The old location to keep things backwards compatible
	bind := c.BindAddress

	s := &Server{
		buildInfo: *buildInfo,
		err:       make(chan error),
		closing:   make(chan struct{}),

		BindAddress: bind,
		Prometheus:  prometheus.NewRegistry(),

		Logger:    logger.New(os.Stderr),
		MuxLogger: tcp.MuxLogger(os.Stderr),

		MetaClient: meta.NewClient(c.Meta),

		reportingDisabled: c.ReportingDisabled,

		httpAPIAddr: c.HTTPD.BindAddress,
		httpUseTLS:  c.HTTPD.HTTPSEnabled,
		tcpAddr:     bind,

		config: c,
	}
	s.Monitor = monitor.New(s, c.Monitor)
	s.config.registerDiagnostics(s.Monitor)

	if err := s.MetaClient.Open(); err != nil {
		return nil, err
	}

	s.TSDBStore = tsdb.NewStore(c.Data.Dir)
	s.TSDBStore.EngineOptions.Config = c.Data

	// Copy TSDB configuration.
	s.TSDBStore.EngineOptions.EngineVersion = c.Data.Engine
	s.TSDBStore.EngineOptions.IndexVersion = c.Data.Index

	// Create the Subscriber service
	s.Subscriber = subscriber.NewService(c.Subscriber)

	// Initialize points writer.
	s.PointsWriter = coordinator.NewPointsWriter()
	s.PointsWriter.WriteTimeout = time.Duration(c.Coordinator.WriteTimeout)
	s.PointsWriter.TSDBStore = s.TSDBStore

	// Initialize query executor.
	s.QueryExecutor = query.NewExecutor()
	s.QueryExecutor.StatementExecutor = &coordinator.StatementExecutor{
		MetaClient:  s.MetaClient,
		TaskManager: s.QueryExecutor.TaskManager,
		TSDBStore:   s.TSDBStore,
		ShardMapper: &coordinator.LocalShardMapper{
			MetaClient: s.MetaClient,
			TSDBStore:  coordinator.LocalTSDBStore{Store: s.TSDBStore},
		},
		StrictErrorHandling: s.TSDBStore.EngineOptions.Config.StrictErrorHandling,
		Monitor:             s.Monitor,
		PointsWriter:        s.PointsWriter,
		MaxSelectPointN:     c.Coordinator.MaxSelectPointN,
		MaxSelectSeriesN:    c.Coordinator.MaxSelectSeriesN,
		MaxSelectBucketsN:   c.Coordinator.MaxSelectBucketsN,
	}
	s.QueryExecutor.TaskManager.QueryTimeout = time.Duration(c.Coordinator.QueryTimeout)
	s.QueryExecutor.TaskManager.LogQueriesAfter = time.Duration(c.Coordinator.LogQueriesAfter)
	s.QueryExecutor.TaskManager.MaxConcurrentQueries = c.Coordinator.MaxConcurrentQueries

	// Initialize the monitor
	s.Monitor.Version = s.buildInfo.Version
	s.Monitor.Commit = s.buildInfo.Commit
	s.Monitor.Branch = s.buildInfo.Branch
	s.Monitor.BuildTime = s.buildInfo.Time
	s.Monitor.PointsWriter = (*monitorPointsWriter)(s.PointsWriter)
	return s, nil
}

// Statistics returns statistics for the services running in the Server.
func (s *Server) Statistics(tags map[string]string) []models.Statistic {
	var statistics []models.Statistic
	statistics = append(statistics, s.QueryExecutor.Statistics(tags)...)
	statistics = append(statistics, s.TSDBStore.Statistics(tags)...)
	statistics = append(statistics, s.PointsWriter.Statistics(tags)...)
	statistics = append(statistics, s.Subscriber.Statistics(tags)...)
	for _, srv := range s.Services {
		if m, ok := srv.(monitor.Reporter); ok {
			statistics = append(statistics, m.Statistics(tags)...)
		}
	}
	metricFamily, err := s.Prometheus.Gather()
	if err == nil {
		statistics = append(statistics, prometheus2.PrometheusToStatistics(metricFamily, tags)...)
	}
	return statistics
}

func (s *Server) appendSnapshotterService() {
	srv := snapshotter.NewService()
	srv.TSDBStore = s.TSDBStore
	srv.MetaClient = s.MetaClient
	s.Services = append(s.Services, srv)
	s.SnapshotterService = srv
}

// SetLogOutput sets the logger used for all messages. It must not be called
// after the Open method has been called.
func (s *Server) SetLogOutput(w io.Writer) {
	s.Logger = logger.New(w)
	s.MuxLogger = tcp.MuxLogger(w)
}

func (s *Server) appendMonitorService() {
	s.Services = append(s.Services, s.Monitor)
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

func (s *Server) appendHTTPDService(c httpd.Config) error {
	if !c.Enabled {
		return nil
	}
	srv := httpd.NewService(c)
	srv.Handler.MetaClient = s.MetaClient
	authorizer := meta.NewQueryAuthorizer(s.MetaClient)
	srv.Handler.QueryAuthorizer = authorizer
	srv.Handler.WriteAuthorizer = meta.NewWriteAuthorizer(s.MetaClient)
	srv.Handler.QueryExecutor = s.QueryExecutor
	srv.Handler.Monitor = s.Monitor
	srv.Handler.PointsWriter = s.PointsWriter
	srv.Handler.Version = s.buildInfo.Version
	srv.Handler.BuildType = "OSS"
	ss := storage.NewStore(s.TSDBStore, s.MetaClient)
	srv.Handler.Store = ss
	if s.config.HTTPD.FluxEnabled {
		storageDep, err := influxdb2.NewDependencies(s.MetaClient, reads.NewReader(ss), authorizer, c.AuthEnabled, s.PointsWriter)
		if err != nil {
			return err
		}
		srv.Handler.Controller, err = control.New(
			s.config.FluxController,
			s.Logger.With(zap.String("service", "flux-controller")),
			[]flux.Dependency{storageDep, testing.FrameworkConfig{}},
		)
		if err != nil {
			return err
		}
		s.Prometheus.MustRegister(srv.Handler.Controller.PrometheusCollectors()...)
	}

	s.Services = append(s.Services, srv)
	return nil
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
	srv := precreator.NewService(c)
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
	srv.Monitor = s.Monitor
	s.Services = append(s.Services, srv)
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return s.err }

// Open opens the meta and data store and all services.
func (s *Server) Open() error {
	// Start profiling if requested.
	if err := s.startProfile(); err != nil {
		return err
	}

	// Open shared TCP connection.
	ln, err := net.Listen("tcp", s.BindAddress)
	if err != nil {
		return fmt.Errorf("listen: %s", err)
	}
	s.Listener = ln

	// Multiplex listener.
	mux := tcp.NewMux(s.MuxLogger)
	go mux.Serve(ln)

	// Append services.
	s.appendMonitorService()
	s.appendPrecreatorService(s.config.Precreator)
	s.appendSnapshotterService()
	s.appendContinuousQueryService(s.config.ContinuousQuery)
	if err := s.appendHTTPDService(s.config.HTTPD); err != nil {
		return err
	}
	s.appendRetentionPolicyService(s.config.Retention)
	for _, i := range s.config.GraphiteInputs {
		if err := s.appendGraphiteService(i); err != nil {
			return err
		}
	}
	for _, i := range s.config.CollectdInputs {
		s.appendCollectdService(i)
	}
	for _, i := range s.config.OpenTSDBInputs {
		if err := s.appendOpenTSDBService(i); err != nil {
			return err
		}
	}
	for _, i := range s.config.UDPInputs {
		s.appendUDPService(i)
	}

	s.Subscriber.MetaClient = s.MetaClient
	s.PointsWriter.MetaClient = s.MetaClient
	s.Monitor.MetaClient = s.MetaClient

	s.SnapshotterService.Listener = mux.Listen(snapshotter.MuxHeader)

	// Configure logging for all services and clients.
	if s.config.Meta.LoggingEnabled {
		s.MetaClient.WithLogger(s.Logger)
	}
	s.TSDBStore.WithLogger(s.Logger)
	if s.config.Data.QueryLogEnabled {
		s.QueryExecutor.WithLogger(s.Logger)
	}
	s.PointsWriter.WithLogger(s.Logger)
	s.Subscriber.WithLogger(s.Logger)
	for _, svc := range s.Services {
		svc.WithLogger(s.Logger)
	}
	s.SnapshotterService.WithLogger(s.Logger)
	s.Monitor.WithLogger(s.Logger)

	// Open TSDB store.
	if err := s.TSDBStore.Open(); err != nil {
		return fmt.Errorf("open tsdb store: %s", err)
	}

	// Add the subscriber before opening the PointsWriter
	s.PointsWriter.Subscriber = s.Subscriber

	// Open the subscriber service
	if err := s.Subscriber.Open(); err != nil {
		return fmt.Errorf("open subscriber: %s", err)
	}

	// Open the points writer service
	if err := s.PointsWriter.Open(); err != nil {
		return fmt.Errorf("open points writer: %s", err)
	}

	for _, service := range s.Services {
		if err := service.Open(); err != nil {
			return fmt.Errorf("open service: %s", err)
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
	s.stopProfile()

	// Close the listener first to stop any new connections
	if s.Listener != nil {
		s.Listener.Close()
	}

	// Close services to allow any inflight requests to complete
	// and prevent new requests from being accepted.
	for _, service := range s.Services {
		service.Close()
	}

	s.config.deregisterDiagnostics(s.Monitor)

	if s.PointsWriter != nil {
		s.PointsWriter.Close()
	}

	if s.QueryExecutor != nil {
		s.QueryExecutor.Close()
	}

	// Close the TSDBStore, no more reads or writes at this point
	if s.TSDBStore != nil {
		s.TSDBStore.Close()
	}

	if s.Subscriber != nil {
		s.Subscriber.Close()
	}

	if s.MetaClient != nil {
		s.MetaClient.Close()
	}

	close(s.closing)
	return nil
}

// startServerReporting starts periodic server reporting.
func (s *Server) startServerReporting() {
	s.reportServer()

	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			s.reportServer()
		}
	}
}

// reportServer reports usage statistics about the system.
func (s *Server) reportServer() {
	dbs := s.MetaClient.Databases()
	numDatabases := len(dbs)

	var (
		numMeasurements int64
		numSeries       int64
	)

	for _, db := range dbs {
		name := db.Name
		// Use the context.Background() to avoid timing out on this.
		n, err := s.TSDBStore.SeriesCardinality(context.Background(), name)
		if err != nil {
			s.Logger.Error(fmt.Sprintf("Unable to get series cardinality for database %s: %v", name, err))
		} else {
			numSeries += n
		}

		// Use the context.Background() to avoid timing out on this.
		n, err = s.TSDBStore.MeasurementsCardinality(context.Background(), name)
		if err != nil {
			s.Logger.Error(fmt.Sprintf("Unable to get measurement cardinality for database %s: %v", name, err))
		} else {
			numMeasurements += n
		}
	}

	clusterID := s.MetaClient.ClusterID()
	cl := client.New("")
	usage := client.Usage{
		Product: "influxdb",
		Data: []client.UsageData{
			{
				Values: client.Values{
					"os":               runtime.GOOS,
					"arch":             runtime.GOARCH,
					"version":          s.buildInfo.Version,
					"cluster_id":       fmt.Sprintf("%v", clusterID),
					"num_series":       numSeries,
					"num_measurements": numMeasurements,
					"num_databases":    numDatabases,
					"uptime":           time.Since(startTime).Seconds(),
				},
			},
		},
	}

	s.Logger.Info("Sending usage statistics to usage.influxdata.com")

	go cl.Save(usage)
}

// Service represents a service attached to the server.
type Service interface {
	WithLogger(log *zap.Logger)
	Open() error
	Close() error
}

// prof stores the file locations of active profiles.
// StartProfile initializes the cpu and memory profile, if specified.
func (s *Server) startProfile() error {
	if s.CPUProfile != "" {
		f, err := os.Create(s.CPUProfile)
		if err != nil {
			return fmt.Errorf("cpuprofile: %v", err)
		}

		s.CPUProfileWriteCloser = f
		if err := pprof.StartCPUProfile(s.CPUProfileWriteCloser); err != nil {
			return err
		}

		log.Printf("Writing CPU profile to: %s\n", s.CPUProfile)
	}

	if s.MemProfile != "" {
		f, err := os.Create(s.MemProfile)
		if err != nil {
			return fmt.Errorf("memprofile: %v", err)
		}

		s.MemProfileWriteCloser = f
		runtime.MemProfileRate = 4096

		log.Printf("Writing mem profile to: %s\n", s.MemProfile)
	}

	return nil
}

// StopProfile closes the cpu and memory profiles if they are running.
func (s *Server) stopProfile() error {
	if s.CPUProfileWriteCloser != nil {
		pprof.StopCPUProfile()
		if err := s.CPUProfileWriteCloser.Close(); err != nil {
			return err
		}
		log.Println("CPU profile stopped")
	}

	if s.MemProfileWriteCloser != nil {
		pprof.Lookup("heap").WriteTo(s.MemProfileWriteCloser, 0)
		if err := s.MemProfileWriteCloser.Close(); err != nil {
			return err
		}
		log.Println("mem profile stopped")
	}

	return nil
}

// monitorPointsWriter is a wrapper around `coordinator.PointsWriter` that helps
// to prevent a circular dependency between the `cluster` and `monitor` packages.
type monitorPointsWriter coordinator.PointsWriter

func (pw *monitorPointsWriter) WritePoints(database, retentionPolicy string, points models.Points) error {
	writeCtx := tsdb.WriteContext{
		UserId: tsdb.MonitorUser,
	}
	return (*coordinator.PointsWriter)(pw).WritePointsPrivileged(writeCtx, database, retentionPolicy, models.ConsistencyLevelAny, points)
}

func raftDBExists(dir string) error {
	// Check to see if there is a raft db, if so, error out with a message
	// to downgrade, export, and then import the meta data
	raftFile := filepath.Join(dir, "raft.db")
	if _, err := os.Stat(raftFile); err == nil {
		return fmt.Errorf("detected %s. To proceed, you'll need to either 1) downgrade to v0.11.x, export your metadata, upgrade to the current version again, and then import the metadata or 2) delete the file, which will effectively reset your database. For more assistance with the upgrade, see: https://docs.influxdata.com/influxdb/v0.12/administration/upgrading/", raftFile)
	}
	return nil
}
