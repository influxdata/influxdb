package run

import (
	"fmt"
	"net"
	"time"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/services/admin"
	"github.com/influxdb/influxdb/services/collectd"
	"github.com/influxdb/influxdb/services/continuous_querier"
	"github.com/influxdb/influxdb/services/graphite"
	"github.com/influxdb/influxdb/services/hh"
	"github.com/influxdb/influxdb/services/httpd"
	"github.com/influxdb/influxdb/services/opentsdb"
	"github.com/influxdb/influxdb/services/retention"
	"github.com/influxdb/influxdb/services/udp"
	"github.com/influxdb/influxdb/tcp"
	"github.com/influxdb/influxdb/tsdb"
)

// Server represents a container for the metadata and storage data and services.
// It is built using a Config and it manages the startup and shutdown of all
// services in the proper order.
type Server struct {
	err     chan error
	closing chan struct{}

	Hostname    string
	BindAddress string
	Listener    net.Listener

	MetaStore     *meta.Store
	TSDBStore     *tsdb.Store
	QueryExecutor *tsdb.QueryExecutor
	PointsWriter  *cluster.PointsWriter
	ShardWriter   *cluster.ShardWriter
	HintedHandoff *hh.Service

	Services []Service
}

// NewServer returns a new instance of Server built from a config.
func NewServer(c *Config) *Server {
	// Construct base meta store and data store.
	s := &Server{
		err:     make(chan error),
		closing: make(chan struct{}),

		Hostname:    c.Meta.Hostname,
		BindAddress: c.Meta.BindAddress,

		MetaStore: meta.NewStore(c.Meta),
		TSDBStore: tsdb.NewStore(c.Data.Dir),
	}

	// Initialize query executor.
	s.QueryExecutor = tsdb.NewQueryExecutor(s.TSDBStore)
	s.QueryExecutor.MetaStore = s.MetaStore
	s.QueryExecutor.MetaStatementExecutor = &meta.StatementExecutor{Store: s.MetaStore}

	// Set the shard writer
	s.ShardWriter = cluster.NewShardWriter(time.Duration(c.Cluster.ShardWriterTimeout))
	s.ShardWriter.MetaStore = s.MetaStore

	// Create the hinted handoff service
	s.HintedHandoff = hh.NewService(c.HintedHandoff, s.ShardWriter)

	// Initialize points writer.
	s.PointsWriter = cluster.NewPointsWriter()
	s.PointsWriter.MetaStore = s.MetaStore
	s.PointsWriter.TSDBStore = s.TSDBStore
	s.PointsWriter.ShardWriter = s.ShardWriter
	s.PointsWriter.HintedHandoff = s.HintedHandoff

	// Append services.
	s.appendClusterService(c.Cluster)
	s.appendAdminService(c.Admin)
	s.appendContinuousQueryService(c.ContinuousQuery)
	s.appendHTTPDService(c.HTTPD)
	s.appendCollectdService(c.Collectd)
	s.appendOpenTSDBService(c.OpenTSDB)
	s.appendUDPService(c.UDP)
	s.appendRetentionPolicyService(c.Retention)
	for _, g := range c.Graphites {
		s.appendGraphiteService(g)
	}

	return s
}

func (s *Server) appendClusterService(c cluster.Config) {
	srv := cluster.NewService(c)
	srv.TSDBStore = s.TSDBStore
	s.Services = append(s.Services, srv)
}

func (s *Server) appendRetentionPolicyService(c retention.Config) {
	if !c.Enabled {
		return
	}
	srv := retention.NewService(c)
	srv.MetaStore = s.MetaStore
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
	srv.Handler.MetaStore = s.MetaStore
	srv.Handler.QueryExecutor = s.QueryExecutor
	srv.Handler.PointsWriter = s.PointsWriter

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
	s.Services = append(s.Services, srv)
}

func (s *Server) appendOpenTSDBService(c opentsdb.Config) {
	if !c.Enabled {
		return
	}
	srv := opentsdb.NewService(c)
	s.Services = append(s.Services, srv)
}

func (s *Server) appendGraphiteService(c graphite.Config) {
	if !c.Enabled {
		return
	}
	srv := graphite.NewService(c)
	s.Services = append(s.Services, srv)
}

func (s *Server) appendUDPService(c udp.Config) {
	if !c.Enabled {
		return
	}
	srv := udp.NewService(c)
	srv.PointsWriter = s.PointsWriter
}

func (s *Server) appendContinuousQueryService(c continuous_querier.Config) {
	if !c.Enabled {
		return
	}
	srv := continuous_querier.NewService(c)
	srv.MetaStore = s.MetaStore
	srv.QueryExecutor = s.QueryExecutor
	srv.PointsWriter = s.PointsWriter
	s.Services = append(s.Services, srv)
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return s.err }

// Open opens the meta and data store and all services.
func (s *Server) Open() error {
	if err := func() error {
		// Resolve host to address.
		_, port, err := net.SplitHostPort(s.BindAddress)
		if err != nil {
			return fmt.Errorf("split bind address: %s", err)
		}
		hostport := net.JoinHostPort(s.Hostname, port)
		addr, err := net.ResolveTCPAddr("tcp", hostport)
		if err != nil {
			return fmt.Errorf("resolve tcp: addr=%s, err=%s", hostport, err)
		}
		s.MetaStore.Addr = addr

		// Open shared TCP connection.
		ln, err := net.Listen("tcp", s.BindAddress)
		if err != nil {
			return fmt.Errorf("listen: %s", err)
		}
		s.Listener = ln

		// Multiplex listener.
		mux := tcp.NewMux()
		s.MetaStore.RaftListener = mux.Listen(meta.MuxRaftHeader)
		s.MetaStore.ExecListener = mux.Listen(meta.MuxExecHeader)
		s.Services[0].(*cluster.Service).Listener = mux.Listen(cluster.MuxHeader)
		go mux.Serve(ln)

		// Open meta store.
		if err := s.MetaStore.Open(); err != nil {
			return fmt.Errorf("open meta store: %s", err)
		}
		go s.monitorErrorChan(s.MetaStore.Err())

		// Wait for the store to initialize.
		<-s.MetaStore.Ready()

		// Open TSDB store.
		if err := s.TSDBStore.Open(); err != nil {
			return fmt.Errorf("open tsdb store: %s", err)
		}

		// Open the hinted handoff service
		if err := s.HintedHandoff.Open(); err != nil {
			return fmt.Errorf("open hinted handoff: %s", err)
		}

		for _, service := range s.Services {
			if err := service.Open(); err != nil {
				return fmt.Errorf("open service: %s", err)
			}
		}

		return nil

	}(); err != nil {
		s.Close()
		return err
	}

	return nil
}

// Close shuts down the meta and data stores and all services.
func (s *Server) Close() error {
	if s.Listener != nil {
		s.Listener.Close()
	}
	if s.MetaStore != nil {
		s.MetaStore.Close()
	}
	if s.TSDBStore != nil {
		s.TSDBStore.Close()
	}
	if s.HintedHandoff != nil {
		s.HintedHandoff.Close()
	}
	for _, service := range s.Services {
		service.Close()
	}
	close(s.closing)
	return nil
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

// Service represents a service attached to the server.
type Service interface {
	Open() error
	Close() error
}
