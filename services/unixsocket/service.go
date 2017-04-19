package unixsocket // import "github.com/influxdata/influxdb/services/unixsocket"

import (
	"errors"
	"io"
	"log"
	"net"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
)

const (
	// Arbitrary, testing indicated that this doesn't typically get over 10
	parserChanLen = 1000

	MAX_UNIX_SOCKET_PAYLOAD = 64 * 1024
)

// statistics gathered by the unix socket package.
const (
	statPointsReceived      = "pointsRx"
	statBytesReceived       = "bytesRx"
	statPointsParseFail     = "pointsParseFail"
	statReadFail            = "readFail"
	statBatchesTransmitted  = "batchesTx"
	statPointsTransmitted   = "pointsTx"
	statBatchesTransmitFail = "batchesTxFail"
)

//
// Service represents here an unix socket service
// that will listen for incoming packets
// formatted with the inline protocol
//
type Service struct {
	conn *net.UnixConn
	addr *net.UnixAddr
	wg   sync.WaitGroup
	done chan struct{}

	parserChan chan []byte
	batcher    *tsdb.PointBatcher
	config     Config

	PointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}

	MetaClient interface {
		CreateDatabase(name string) (*meta.DatabaseInfo, error)
	}

	Logger   *log.Logger
	stats    *Statistics
	statTags models.Tags
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	d := *c.WithDefaults()
	return &Service{
		config:     d,
		done:       make(chan struct{}),
		parserChan: make(chan []byte, parserChanLen),
		batcher:    tsdb.NewPointBatcher(d.BatchSize, d.BatchPending, time.Duration(d.BatchTimeout)),
		Logger:     log.New(os.Stderr, "[unixsocket] ", log.LstdFlags),
		stats:      &Statistics{},
		statTags:   map[string]string{"unixsocket": d.BindSocket},
	}
}

// Open starts the service
func (s *Service) Open() (err error) {
	if s.conn != nil {
		s.conn.Close()
	}

	if s.config.BindSocket == "" {
		return errors.New("bind socket has to be specified in config")
	}
	if s.config.Database == "" {
		return errors.New("database has to be specified in config")
	}

	if _, err := s.MetaClient.CreateDatabase(s.config.Database); err != nil {
		return errors.New("Failed to ensure target database exists")
	}

	if err := os.MkdirAll(path.Dir(s.config.BindSocket), 0777); err != nil {
		return err
	}

	s.addr, err = net.ResolveUnixAddr("unix", s.config.BindSocket)
	if err != nil {
		s.Logger.Printf("Failed to resolve unix socket address %s: %s", s.config.BindSocket, err)
		return err
	}

	s.conn, err = net.ListenUnixgram("unixgram", s.addr)
	if err != nil {
		s.Logger.Printf("Failed to set up unixgram listener at address %s: %s", s.addr, err)
		return err
	}

	if s.config.ReadBuffer != 0 {
		err = s.conn.SetReadBuffer(s.config.ReadBuffer)
		if err != nil {
			s.Logger.Printf("Failed to set unix socket read buffer to %d: %s",
				s.config.ReadBuffer, err)
			return err
		}
	}

	s.Logger.Printf("Started listening on unix socket: %s", s.config.BindSocket)

	s.wg.Add(3)
	go s.serve()
	go s.parser()
	go s.writer()

	return nil
}

// Statistics maintains statistics for the unix socket service.
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
		Name: "unixsocket",
		Tags: s.statTags,
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

func (s *Service) writer() {
	defer s.wg.Done()

	for {
		select {
		case batch := <-s.batcher.Out():
			if err := s.PointsWriter.WritePoints(s.config.Database, s.config.RetentionPolicy, models.ConsistencyLevelAny, batch); err == nil {
				atomic.AddInt64(&s.stats.BatchesTransmitted, 1)
				atomic.AddInt64(&s.stats.PointsTransmitted, int64(len(batch)))
			} else {
				s.Logger.Printf("failed to write point batch to database %q: %s", s.config.Database, err)
				atomic.AddInt64(&s.stats.BatchesTransmitFail, 1)
			}

		case <-s.done:
			return
		}
	}
}

func (s *Service) serve() {
	defer s.wg.Done()

	buf := make([]byte, MAX_UNIX_SOCKET_PAYLOAD)
	s.batcher.Start()
	for {

		select {
		case <-s.done:
			// We closed the connection, time to go.
			return
		default:
			// Keep processing.
			n, _, err := s.conn.ReadFromUnix(buf)
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && !opErr.Temporary() {
					// Receive singal graceful
					s.Logger.Printf("unix socket listener closed")
					continue
				}
				atomic.AddInt64(&s.stats.ReadFail, 1)
				s.Logger.Printf("Failed to read unix socket message: %s", err)
				continue
			}
			atomic.AddInt64(&s.stats.BytesReceived, int64(n))

			bufCopy := make([]byte, n)
			copy(bufCopy, buf[:n])
			s.parserChan <- bufCopy
		}
	}
}

func (s *Service) parser() {
	defer s.wg.Done()

	for {
		select {
		case <-s.done:
			return
		case buf := <-s.parserChan:
			points, err := models.ParsePointsWithPrecision(buf, time.Now().UTC(), s.config.Precision)
			if err != nil {
				atomic.AddInt64(&s.stats.PointsParseFail, 1)
				s.Logger.Printf("Failed to parse points: %s", err)
				continue
			}

			for _, point := range points {
				s.batcher.In() <- point
			}
			atomic.AddInt64(&s.stats.PointsReceived, int64(len(points)))
		}
	}
}

// Close closes the underlying listener.
func (s *Service) Close() error {
	if s.conn == nil {
		return errors.New("Service already closed")
	}

	s.conn.Close()
	s.batcher.Flush()
	close(s.done)
	s.wg.Wait()

	// Release all remaining resources.
	s.done = nil
	s.conn = nil
	err := os.Remove(s.config.BindSocket)
	if err != nil {
		return err
	}

	s.Logger.Print("Service closed")

	return nil
}

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (s *Service) SetLogOutput(w io.Writer) {
	s.Logger = log.New(w, "[unixsocket] ", log.LstdFlags)
}

// Addr returns the listener's address
func (s *Service) Addr() net.Addr {
	return s.addr
}
