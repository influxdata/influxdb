// Package opentsdb provides a service for InfluxDB to ingest data via the opentsdb protocol.
package opentsdb // import "github.com/influxdata/influxdb/services/opentsdb"

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"log"
	"net"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

// statistics gathered by the openTSDB package.
const (
	statHTTPConnectionsHandled   = "httpConnsHandled"
	statTelnetConnectionsActive  = "tlConnsActive"
	statTelnetConnectionsHandled = "tlConnsHandled"
	statTelnetPointsReceived     = "tlPointsRx"
	statTelnetBytesReceived      = "tlBytesRx"
	statTelnetReadError          = "tlReadErr"
	statTelnetBadLine            = "tlBadLine"
	statTelnetBadTime            = "tlBadTime"
	statTelnetBadTag             = "tlBadTag"
	statTelnetBadFloat           = "tlBadFloat"
	statBatchesTransmitted       = "batchesTx"
	statPointsTransmitted        = "pointsTx"
	statBatchesTransmitFail      = "batchesTxFail"
	statConnectionsActive        = "connsActive"
	statConnectionsHandled       = "connsHandled"
	statDroppedPointsInvalid     = "droppedPointsInvalid"
)

// Service manages the listener and handler for an HTTP endpoint.
type Service struct {
	ln     net.Listener  // main listener
	httpln *chanListener // http channel-based listener

	wg        sync.WaitGroup
	tls       bool
	tlsConfig *tls.Config
	cert      string

	mu    sync.RWMutex
	ready bool          // Has the required database been created?
	done  chan struct{} // Is the service closing or closed?

	BindAddress     string
	Database        string
	RetentionPolicy string

	PointsWriter interface {
		WritePointsPrivileged(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}
	MetaClient interface {
		CreateDatabase(name string) (*meta.DatabaseInfo, error)
	}

	// Points received over the telnet protocol are batched.
	batchSize    int
	batchPending int
	batchTimeout time.Duration
	batcher      *tsdb.PointBatcher

	LogPointErrors bool
	Logger         *zap.Logger

	stats       *Statistics
	defaultTags models.StatisticTags

	// members used for testing
	ctx     context.Context
	cancel  context.CancelFunc
	errChan chan error
}

// NewService returns a new instance of Service.
func NewService(c Config) (*Service, error) {
	// Use defaults where necessary.
	d := c.WithDefaults()

	s := &Service{
		tls:             d.TLSEnabled,
		tlsConfig:       d.TLS,
		cert:            d.Certificate,
		BindAddress:     d.BindAddress,
		Database:        d.Database,
		RetentionPolicy: d.RetentionPolicy,
		batchSize:       d.BatchSize,
		batchPending:    d.BatchPending,
		batchTimeout:    time.Duration(d.BatchTimeout),
		Logger:          zap.NewNop(),
		LogPointErrors:  d.LogPointErrors,
		stats:           &Statistics{},
		defaultTags:     models.StatisticTags{"bind": d.BindAddress},
	}
	if s.tlsConfig == nil {
		s.tlsConfig = new(tls.Config)
	}

	return s, nil
}

func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errChan = make(chan error)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	ready := make(chan struct{})

	go func() { s.errChan <- s.RunWithReady(s.ctx, ready, services.NewRegistry()) }()

	select {
	case <-ready:
		return nil
	case err := <-s.errChan:
		return err
	}
}

func (s *Service) Close() error {
	s.cancel()
	return <-s.errChan
}

func (s *Service) Run(ctx context.Context, reg services.Registry) error {
	ready := make(chan struct{})
	errChan := make(chan error)
	go func() { errChan <- s.RunWithReady(ctx, ready, reg) }()

	select {
	case <-ready:
		log.Printf("READY!")
		return nil
	case err := <-errChan:
		return err
	}
}

// Open starts the service.
func (s *Service) RunWithReady(ctx context.Context, ready chan struct{}, reg services.Registry) error {
	if s.done != nil {
		return nil // Already open.
	}
	s.done = make(chan struct{})

	s.Logger.Info("Starting OpenTSDB service")

	s.batcher = tsdb.NewPointBatcher(s.batchSize, s.batchPending, s.batchTimeout)
	s.batcher.Start()

	// Start processing batches.
	s.wg.Add(1)
	go func() { defer s.wg.Done(); s.processBatches(s.batcher) }()

	// Open listener.
	if s.tls {
		cert, err := tls.LoadX509KeyPair(s.cert, s.cert)
		if err != nil {
			return err
		}

		tlsConfig := s.tlsConfig.Clone()
		tlsConfig.Certificates = []tls.Certificate{cert}

		listener, err := tls.Listen("tcp", s.BindAddress, tlsConfig)
		if err != nil {
			return err
		}

		s.ln = listener
	} else {
		listener, err := net.Listen("tcp", s.BindAddress)
		if err != nil {
			return err
		}

		s.ln = listener
	}
	s.Logger.Info("Listening on TCP",
		zap.Stringer("addr", s.ln.Addr()),
		zap.Bool("tls", s.tls))
	s.httpln = newChanListener(s.ln.Addr())

	// Begin listening for connections.
	s.wg.Add(2)
	go func() { defer s.wg.Done(); s.serve(ctx) }()
	go func() { defer s.wg.Done(); s.serveHTTP() }()

	close(ready)
	<-ctx.Done()

	return s.cleanup()
}

// Close closes the openTSDB service.
func (s *Service) cleanup() error {
	if wait, err := func() (bool, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		if s.closed() {
			return false, nil // Already closed.
		}
		close(s.done)

		// Close the listeners.
		if err := s.ln.Close(); err != nil {
			return false, err
		}
		if err := s.httpln.Close(); err != nil {
			return false, err
		}

		if s.batcher != nil {
			s.batcher.Stop()
		}
		return true, nil
	}(); err != nil {
		return err
	} else if !wait {
		return nil
	}
	s.wg.Wait()

	s.mu.Lock()
	s.done = nil
	s.mu.Unlock()

	return nil
}

// Closed returns true if the service is currently closed.
func (s *Service) Closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed()
}

func (s *Service) closed() bool {
	select {
	case <-s.done:
		// Service is closing.
		return true
	default:
		return s.done == nil
	}
}

// createInternalStorage ensures that the required database has been created.
func (s *Service) createInternalStorage() error {
	s.mu.RLock()
	ready := s.ready
	s.mu.RUnlock()
	if ready {
		return nil
	}

	if _, err := s.MetaClient.CreateDatabase(s.Database); err != nil {
		return err
	}

	// The service is now ready.
	s.mu.Lock()
	s.ready = true
	s.mu.Unlock()
	return nil
}

// WithLogger sets the logger for the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "opentsdb"))
}

// Statistics maintains statistics for the subscriber service.
type Statistics struct {
	HTTPConnectionsHandled   int64
	ActiveTelnetConnections  int64
	HandledTelnetConnections int64
	TelnetPointsReceived     int64
	TelnetBytesReceived      int64
	TelnetReadError          int64
	TelnetBadLine            int64
	TelnetBadTime            int64
	TelnetBadTag             int64
	TelnetBadFloat           int64
	BatchesTransmitted       int64
	PointsTransmitted        int64
	BatchesTransmitFail      int64
	ActiveConnections        int64
	HandledConnections       int64
	InvalidDroppedPoints     int64
}

// Statistics returns statistics for periodic monitoring.
func (s *Service) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "opentsdb",
		Tags: s.defaultTags.Merge(tags),
		Values: map[string]interface{}{
			statHTTPConnectionsHandled:   atomic.LoadInt64(&s.stats.HTTPConnectionsHandled),
			statTelnetConnectionsActive:  atomic.LoadInt64(&s.stats.ActiveTelnetConnections),
			statTelnetConnectionsHandled: atomic.LoadInt64(&s.stats.HandledTelnetConnections),
			statTelnetPointsReceived:     atomic.LoadInt64(&s.stats.TelnetPointsReceived),
			statTelnetBytesReceived:      atomic.LoadInt64(&s.stats.TelnetBytesReceived),
			statTelnetReadError:          atomic.LoadInt64(&s.stats.TelnetReadError),
			statTelnetBadLine:            atomic.LoadInt64(&s.stats.TelnetBadLine),
			statTelnetBadTime:            atomic.LoadInt64(&s.stats.TelnetBadTime),
			statTelnetBadTag:             atomic.LoadInt64(&s.stats.TelnetBadTag),
			statTelnetBadFloat:           atomic.LoadInt64(&s.stats.TelnetBadFloat),
			statBatchesTransmitted:       atomic.LoadInt64(&s.stats.BatchesTransmitted),
			statPointsTransmitted:        atomic.LoadInt64(&s.stats.PointsTransmitted),
			statBatchesTransmitFail:      atomic.LoadInt64(&s.stats.BatchesTransmitFail),
			statConnectionsActive:        atomic.LoadInt64(&s.stats.ActiveConnections),
			statConnectionsHandled:       atomic.LoadInt64(&s.stats.HandledConnections),
			statDroppedPointsInvalid:     atomic.LoadInt64(&s.stats.InvalidDroppedPoints),
		},
	}}
}

// Addr returns the listener's address. Returns nil if listener is closed.
func (s *Service) Addr() net.Addr {
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// serve serves the handler from the listener.
func (s *Service) serve(ctx context.Context) error {
	conChan := make(chan net.Conn)
	errChan := make(chan error)

	// send accepted connections to our conChan so that we can multiplex over
	// conChand and ctx.Done() in the next for loop.
	//
	// this go routine is cancelled once the s.ln listener is closed.
	//
	// FIXME: this logic needs to be looked at closely.
	go func() {
		for {
			// Wait for next connection.
			c, err := s.ln.Accept()
			if opErr, ok := err.(*net.OpError); ok && !opErr.Temporary() {
				s.Logger.Info("OpenTSDB TCP listener closed")
				errChan <- err
				return
			} else if err != nil {
				s.Logger.Info("Error accepting OpenTSDB", zap.Error(err))
				continue
			}
			conChan <- c
		}
	}()

	// handle new incoming connections, errors, and cancellation signal.
	for {
		select {
		case <-ctx.Done():
			// we're done.  close our listener immediately so we don't accept new connections.
			s.ln.Close()
			return nil

		case conn := <-conChan:
			// Handle connection in separate goroutine.
			go s.handleConn(conn)

		case err := <-errChan:
			s.ln.Close()
			return err
		}
	}

	return nil
}

// handleConn processes conn. This is run in a separate goroutine.
func (s *Service) handleConn(conn net.Conn) {
	defer atomic.AddInt64(&s.stats.ActiveConnections, -1)
	atomic.AddInt64(&s.stats.ActiveConnections, 1)
	atomic.AddInt64(&s.stats.HandledConnections, 1)

	// Read header into buffer to check if it's HTTP.
	var buf bytes.Buffer
	r := bufio.NewReader(io.TeeReader(conn, &buf))

	// Attempt to parse connection as HTTP.
	_, err := http.ReadRequest(r)

	// Rebuild connection from buffer and remaining connection data.
	bufr := bufio.NewReader(io.MultiReader(&buf, conn))
	conn = &readerConn{Conn: conn, r: bufr}

	// If no HTTP parsing error occurred then process as HTTP.
	if err == nil {
		atomic.AddInt64(&s.stats.HTTPConnectionsHandled, 1)
		s.httpln.ch <- conn
		return
	}

	// Otherwise handle in telnet format.
	s.wg.Add(1)
	s.handleTelnetConn(conn)
	s.wg.Done()
}

// handleTelnetConn accepts OpenTSDB's telnet protocol.
// Each telnet command consists of a line of the form:
//   put sys.cpu.user 1356998400 42.5 host=webserver01 cpu=0
func (s *Service) handleTelnetConn(conn net.Conn) {
	defer conn.Close()
	defer atomic.AddInt64(&s.stats.ActiveTelnetConnections, -1)
	atomic.AddInt64(&s.stats.ActiveTelnetConnections, 1)
	atomic.AddInt64(&s.stats.HandledTelnetConnections, 1)

	// Get connection details.
	remoteAddr := conn.RemoteAddr().String()

	// Wrap connection in a text protocol reader.
	r := textproto.NewReader(bufio.NewReader(conn))
	for {
		line, err := r.ReadLine()
		if err != nil {
			if err != io.EOF {
				atomic.AddInt64(&s.stats.TelnetReadError, 1)
				s.Logger.Info("Error reading from OpenTSDB connection", zap.Error(err))
			}
			return
		}
		atomic.AddInt64(&s.stats.TelnetPointsReceived, 1)
		atomic.AddInt64(&s.stats.TelnetBytesReceived, int64(len(line)))

		inputStrs := strings.Fields(line)

		if len(inputStrs) == 1 && inputStrs[0] == "version" {
			conn.Write([]byte("InfluxDB TSDB proxy"))
			continue
		}

		if len(inputStrs) < 4 || inputStrs[0] != "put" {
			atomic.AddInt64(&s.stats.TelnetBadLine, 1)
			if s.LogPointErrors {
				s.Logger.Info("Malformed line", zap.String("line", line), zap.String("remote_addr", remoteAddr))
			}
			continue
		}

		measurement := inputStrs[1]
		tsStr := inputStrs[2]
		valueStr := inputStrs[3]
		tagStrs := inputStrs[4:]

		var t time.Time
		ts, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			atomic.AddInt64(&s.stats.TelnetBadTime, 1)
			if s.LogPointErrors {
				s.Logger.Info("Malformed time", zap.String("time", tsStr), zap.String("remote_addr", remoteAddr))
			}
		}

		switch len(tsStr) {
		case 10:
			t = time.Unix(ts, 0)
		case 13:
			t = time.Unix(ts/1000, (ts%1000)*1000)
		default:
			atomic.AddInt64(&s.stats.TelnetBadTime, 1)
			if s.LogPointErrors {
				s.Logger.Info("Time must be 10 or 13 chars", zap.String("time", tsStr), zap.String("remote_addr", remoteAddr))
			}
			continue
		}

		tags := make(map[string]string)
		for t := range tagStrs {
			parts := strings.SplitN(tagStrs[t], "=", 2)
			if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
				atomic.AddInt64(&s.stats.TelnetBadTag, 1)
				if s.LogPointErrors {
					s.Logger.Info("Malformed tag data", zap.String("tag", tagStrs[t]), zap.String("remote_addr", remoteAddr))
				}
				continue
			}
			k := parts[0]

			tags[k] = parts[1]
		}

		fields := make(map[string]interface{})
		fv, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			atomic.AddInt64(&s.stats.TelnetBadFloat, 1)
			if s.LogPointErrors {
				s.Logger.Info("Bad float", zap.String("value", valueStr), zap.String("remote_addr", remoteAddr))
			}
			continue
		}
		fields["value"] = fv

		pt, err := models.NewPoint(measurement, models.NewTags(tags), fields, t)
		if err != nil {
			atomic.AddInt64(&s.stats.TelnetBadFloat, 1)
			if s.LogPointErrors {
				s.Logger.Info("Bad float", zap.String("value", valueStr), zap.String("remote_addr", remoteAddr))
			}
			continue
		}
		s.batcher.In() <- pt
	}
}

// serveHTTP handles connections in HTTP format.
func (s *Service) serveHTTP() {
	handler := &Handler{
		Database:        s.Database,
		RetentionPolicy: s.RetentionPolicy,
		PointsWriter:    s.PointsWriter,
		Logger:          s.Logger,
		stats:           s.stats,
	}
	srv := &http.Server{Handler: handler}
	srv.Serve(s.httpln)
}

// processBatches continually drains the given batcher and writes the batches to the database.
func (s *Service) processBatches(batcher *tsdb.PointBatcher) {
	for {
		select {
		case <-s.done:
			return
		case batch := <-batcher.Out():
			// Will attempt to create database if not yet created.
			if err := s.createInternalStorage(); err != nil {
				s.Logger.Info("Required database does not yet exist", logger.Database(s.Database), zap.Error(err))
				continue
			}

			if err := s.PointsWriter.WritePointsPrivileged(s.Database, s.RetentionPolicy, models.ConsistencyLevelAny, batch); err == nil {
				atomic.AddInt64(&s.stats.BatchesTransmitted, 1)
				atomic.AddInt64(&s.stats.PointsTransmitted, int64(len(batch)))
			} else {
				s.Logger.Info("Failed to write point batch to database",
					logger.Database(s.Database), zap.Error(err))
				atomic.AddInt64(&s.stats.BatchesTransmitFail, 1)
			}
		}
	}
}
