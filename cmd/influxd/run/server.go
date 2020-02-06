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

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/flux/control"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services"
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
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/tcp"
	"github.com/influxdata/influxdb/tsdb"
	client "github.com/influxdata/usage-client/v1"
	"go.uber.org/zap"

	// Initialize the engine package
	_ "github.com/influxdata/influxdb/tsdb/engine"
	// Initialize the index package
	_ "github.com/influxdata/influxdb/tsdb/index"
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

	err chan error

	BindAddress string
	Listener    net.Listener

	Logger *zap.Logger

	MetaClient *meta.Client

	TSDBStore     *tsdb.Store
	QueryExecutor *query.Executor
	PointsWriter  *coordinator.PointsWriter
	Subscriber    *subscriber.Service

	Services []Service

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

		BindAddress: bind,

		Logger: logger.New(os.Stderr),

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
		Monitor:           s.Monitor,
		PointsWriter:      s.PointsWriter,
		MaxSelectPointN:   c.Coordinator.MaxSelectPointN,
		MaxSelectSeriesN:  c.Coordinator.MaxSelectSeriesN,
		MaxSelectBucketsN: c.Coordinator.MaxSelectBucketsN,
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

func (s *Server) appendHTTPDService(c httpd.Config, reg services.Registry) {
	if !c.Enabled {
		return
	}
	srv := httpd.NewService(c, reg)
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
	srv.Handler.Controller = control.NewController(s.MetaClient, reads.NewReader(ss), authorizer, c.AuthEnabled, s.Logger)

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
	return s.OpenWithContext(context.Background())
}

func (s *Server) OpenWithContext(ctx context.Context) error {
	// Start profiling, if set.
	s.startProfile()

	// Open shared TCP connection.
	ln, err := net.Listen("tcp", s.BindAddress)
	if err != nil {
		return fmt.Errorf("listen: %s", err)
	}
	s.Listener = ln

	// Multiplex listener.
	mux := tcp.NewMux()
	go mux.Serve(ln)

	reg := services.NewRegistry()

	// Append services.
	s.appendMonitorService()
	s.appendPrecreatorService(s.config.Precreator)
	s.appendSnapshotterService()
	s.appendContinuousQueryService(s.config.ContinuousQuery)
	s.appendHTTPDService(s.config.HTTPD, reg)
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

	// append usp services
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

	// append TSDB Store, Subcriber, and PointsWriter services to our Services slice.
	s.Services = append(s.Services, s.TSDBStore, s.Subscriber, s.PointsWriter)

	s.PointsWriter.AddWriteSubscriber(s.Subscriber.Points())

	// asyncronously start all services. we run this loop in a go routine so we
	// can block on the supplied context as soon as possible.  FIXME: this might
	// be unnessesary.
	sem := make(chan struct{}, len(s.Services))
	for _, service := range s.Services {
		go func(svc Service) {
			// start service
			//
			// we begin by "incrementing" our semaphore to indicate that a service
			// has started.
			//
			// as services exit, we read back from the same channel to remove our
			// place in the queue.
			//
			// to test if all services are complete, we can attempt to write to the
			// channel len(s.Services) times which will block until all services have
			// exited.
			sem <- struct{}{}
			if err := svc.Run(ctx, reg); err != nil {
				// if there was an error, report it to s.err s.err <- err
				//
				// subtle: we want to report errors to s.err but if nothing is
				// receiving messages on that channel, we do not want to block!
				select {
				case s.err <- err:
				default:
				}
			}
			// indicate that this service is done by decrementing our semaphore.
			<-sem
		}(service)
	}

	// Start the reporting service, if not disabled.
	if !s.reportingDisabled {
		go s.startServerReporting(ctx)
	}

	// wait for a cancellation signal
	<-ctx.Done()
	s.stopProfile()

	// Close the listener first to stop any new connections
	if s.Listener != nil {
		s.Listener.Close()
	}

	// wait for services to complete.
	for i, end := 0, len(s.Services); i < end; i++ {
		// receive signal when service has ended.
		sem <- struct{}{}
	}

	s.config.deregisterDiagnostics(s.Monitor)

	return nil
}

// startServerReporting starts periodic server reporting.
func (s *Server) startServerReporting(ctx context.Context) {
	s.reportServer()

	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
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
		n, err := s.TSDBStore.SeriesCardinality(name)
		if err != nil {
			s.Logger.Error(fmt.Sprintf("Unable to get series cardinality for database %s: %v", name, err))
		} else {
			numSeries += n
		}

		n, err = s.TSDBStore.MeasurementsCardinality(name)
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
	Run(context.Context, services.Registry) error
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
	return (*coordinator.PointsWriter)(pw).WritePointsPrivileged(database, retentionPolicy, models.ConsistencyLevelAny, points)
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
