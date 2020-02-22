// Package udp provides the UDP input service for InfluxDB.
package udp // import "github.com/influxdata/influxdb/services/udp"

import (
	"context"
	"errors"
	"net"
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

const (
	// Arbitrary, testing indicated that this doesn't typically get over 10
	parserChanLen = 1000

	// MaxUDPPayload is largest payload size the UDP service will accept.
	MaxUDPPayload = 64 * 1024
)

// statistics gathered by the UDP package.
const (
	statPointsReceived      = "pointsRx"
	statBytesReceived       = "bytesRx"
	statPointsParseFail     = "pointsParseFail"
	statReadFail            = "readFail"
	statBatchesTransmitted  = "batchesTx"
	statPointsTransmitted   = "pointsTx"
	statBatchesTransmitFail = "batchesTxFail"
)

// Service is a UDP service that will listen for incoming packets of line protocol.
type Service struct {
	conn *net.UDPConn
	addr *net.UDPAddr
	wg   sync.WaitGroup

	mu    sync.RWMutex
	ready bool // Has the required database been created?

	parserChan chan []byte
	batcher    *tsdb.PointBatcher
	config     Config

	PointsWriter interface {
		WritePointsPrivileged(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}

	MetaClient interface {
		CreateDatabase(name string) (*meta.DatabaseInfo, error)
	}

	Logger      *zap.Logger
	stats       *Statistics
	defaultTags models.StatisticTags

	// members used for testing
	ctx     context.Context
	cancel  context.CancelFunc
	errChan chan error
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	d := *c.WithDefaults()
	return &Service{
		config:      d,
		parserChan:  make(chan []byte, parserChanLen),
		Logger:      zap.NewNop(),
		stats:       &Statistics{},
		errChan:     make(chan error),
		defaultTags: models.StatisticTags{"bind": d.BindAddress},
	}
}

func (s *Service) Open() error {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	ready := make(chan struct{})
	go func() { s.errChan <- s.RunWithReady(s.ctx, ready, services.NewRegistry()) }()
	<-ready
	return nil
}

func (s *Service) Close() error {
	s.cancel()
	return <-s.errChan
}

// Open starts the service.
func (s *Service) Run(ctx context.Context, reg services.Registrar) error {
	ready := make(chan struct{})
	return s.RunWithReady(ctx, ready, reg)
}

func (s *Service) RunWithReady(ctx context.Context, ready chan struct{}, reg services.Registrar) (err error) {
	if s.config.BindAddress == "" {
		return errors.New("bind address has to be specified in config")
	}
	if s.config.Database == "" {
		return errors.New("database has to be specified in config")
	}

	s.addr, err = net.ResolveUDPAddr("udp", s.config.BindAddress)
	if err != nil {
		s.Logger.Info("Failed to resolve UDP address",
			zap.String("bind_address", s.config.BindAddress), zap.Error(err))
		return err
	}

	s.conn, err = net.ListenUDP("udp", s.addr)
	if err != nil {
		s.Logger.Info("Failed to set up UDP listener",
			zap.Stringer("addr", s.addr), zap.Error(err))
		return err
	}

	if s.config.ReadBuffer != 0 {
		err = s.conn.SetReadBuffer(s.config.ReadBuffer)
		if err != nil {
			s.Logger.Info("Failed to set UDP read buffer",
				zap.Int("buffer_size", s.config.ReadBuffer), zap.Error(err))
			return err
		}
	}
	s.batcher = tsdb.NewPointBatcher(s.config.BatchSize, s.config.BatchPending, time.Duration(s.config.BatchTimeout))
	s.batcher.Start()

	s.Logger.Info("Started listening on UDP", zap.String("addr", s.config.BindAddress))

	s.wg.Add(3)
	go s.serve(ctx)
	go s.parser(ctx)
	go s.writer(ctx)

	close(ready)
	s.wg.Wait()

	s.Logger.Info("Service closed")
	return nil
}

// Statistics maintains statistics for the UDP service.
type Statistics struct {
	PointsReceived      int64
	BytesReceived       int64
	PointsParseFail     int64
	ReadFail            int64
	BatchesTransmitted  int64
	PointsTransmitted   int64
	BatchesTransmitFail int64
}

// Statistics returns statistics for periodic monitoring.
func (s *Service) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "udp",
		Tags: s.defaultTags.Merge(tags),
		Values: map[string]interface{}{
			statPointsReceived:      atomic.LoadInt64(&s.stats.PointsReceived),
			statBytesReceived:       atomic.LoadInt64(&s.stats.BytesReceived),
			statPointsParseFail:     atomic.LoadInt64(&s.stats.PointsParseFail),
			statReadFail:            atomic.LoadInt64(&s.stats.ReadFail),
			statBatchesTransmitted:  atomic.LoadInt64(&s.stats.BatchesTransmitted),
			statPointsTransmitted:   atomic.LoadInt64(&s.stats.PointsTransmitted),
			statBatchesTransmitFail: atomic.LoadInt64(&s.stats.BatchesTransmitFail),
		},
	}}
}

func (s *Service) writer(ctx context.Context) {
	defer s.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case batch := <-s.batcher.Out():
			// Will attempt to create database if not yet created.
			if err := s.createInternalStorage(); err != nil {
				s.Logger.Info("Required database does not yet exist",
					logger.Database(s.config.Database), zap.Error(err))
				continue
			}

			if err := s.PointsWriter.WritePointsPrivileged(s.config.Database, s.config.RetentionPolicy, models.ConsistencyLevelAny, batch); err == nil {
				atomic.AddInt64(&s.stats.BatchesTransmitted, 1)
				atomic.AddInt64(&s.stats.PointsTransmitted, int64(len(batch)))
			} else {
				s.Logger.Info("Failed to write point batch to database",
					logger.Database(s.config.Database), zap.Error(err))
				atomic.AddInt64(&s.stats.BatchesTransmitFail, 1)
			}
		}
	}
}

func (s *Service) serve(ctx context.Context) {
	defer s.wg.Done()

	// close our listner on cancel
	//
	// closing s.conn triggers the conn.Read() loop below to unblock and return
	// an error immediately.
	//
	// there doesn't appear to be a cross platform way of detecting a "read on a
	// closed connection" type of error so instead we simply check if ctx.Done()
	// has been triggered when Read() unblocks.
	//
	// if ctx.Done() is triggered, then the loop terminates.
	//
	go func() {
		<-ctx.Done()
		s.conn.Close()
	}()

	// simple utility function that returns a copy of a byte slice.
	cpybuf := func(b []byte) []byte {
		rc := make([]byte, len(b))
		copy(rc, b)
		return rc
	}

	for buf := make([]byte, MaxUDPPayload); ; {
		nbytes, err := s.conn.Read(buf)

		// test if we've been cancelled
		select {
		case <-ctx.Done():
			return
		default:
			// proceed to regular error handling
		}

		// if there is an error, account for it and log it.
		if err != nil {
			atomic.AddInt64(&s.stats.ReadFail, 1)
			s.Logger.Info("Failed to read UDP message", zap.Error(err))
			continue
		}

		// at this point, we've whittled away the undesired stated and we can pass
		// data to our parser.
		atomic.AddInt64(&s.stats.BytesReceived, int64(nbytes))
		s.parserChan <- cpybuf(buf[:nbytes])
	}
}

func (s *Service) parser(ctx context.Context) {
	defer s.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case buf := <-s.parserChan:
			points, err := models.ParsePointsWithPrecision(buf, time.Now().UTC(), s.config.Precision)
			if err != nil {
				atomic.AddInt64(&s.stats.PointsParseFail, 1)
				s.Logger.Info("Failed to parse points", zap.Error(err))
				continue
			}

			for _, point := range points {
				s.batcher.In() <- point
			}
			atomic.AddInt64(&s.stats.PointsReceived, int64(len(points)))
		}
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

	if _, err := s.MetaClient.CreateDatabase(s.config.Database); err != nil {
		return err
	}

	// The service is now ready.
	s.mu.Lock()
	s.ready = true
	s.mu.Unlock()
	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "udp"))
}

// Addr returns the listener's address.
func (s *Service) Addr() net.Addr {
	return s.addr
}
