package udp

import (
	"errors"
	"expvar"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/tsdb"
)

const (
	UDPBufferSize = 65536
)

// statistics gathered by the UDP package.
const (
	statPointsReceived      = "points_rx"
	statBytesReceived       = "bytes_rx"
	statPointsParseFail     = "points_parse_fail"
	statReadFail            = "read_fail"
	statBatchesTrasmitted   = "batches_tx"
	statPointsTransmitted   = "points_tx"
	statBatchesTransmitFail = "batches_tx_fail"
)

//
// Service represents here an UDP service
// that will listen for incoming packets
// formatted with the inline protocol
//
type Service struct {
	conn *net.UDPConn
	addr *net.UDPAddr
	wg   sync.WaitGroup
	done chan struct{}

	batcher *tsdb.PointBatcher
	config  Config

	PointsWriter interface {
		WritePoints(p *cluster.WritePointsRequest) error
	}

	Logger  *log.Logger
	statMap *expvar.Map
}

func NewService(c Config) *Service {
	d := *c.WithDefaults()
	return &Service{
		config:  d,
		done:    make(chan struct{}),
		batcher: tsdb.NewPointBatcher(d.BatchSize, d.BatchPending, time.Duration(d.BatchTimeout)),
		Logger:  log.New(os.Stderr, "[udp] ", log.LstdFlags),
	}
}

func (s *Service) Open() (err error) {
	// Configure expvar monitoring. It's OK to do this even if the service fails to open and
	// should be done before any data could arrive for the service.
	key := strings.Join([]string{"udp", s.config.BindAddress}, ":")
	tags := map[string]string{"bind": s.config.BindAddress}
	s.statMap = influxdb.NewStatistics(key, "udp", tags)

	if s.config.BindAddress == "" {
		return errors.New("bind address has to be specified in config")
	}
	if s.config.Database == "" {
		return errors.New("database has to be specified in config")
	}

	s.addr, err = net.ResolveUDPAddr("udp", s.config.BindAddress)
	if err != nil {
		s.Logger.Printf("Failed to resolve UDP address %s: %s", s.config.BindAddress, err)
		return err
	}

	s.conn, err = net.ListenUDP("udp", s.addr)
	if err != nil {
		s.Logger.Printf("Failed to set up UDP listener at address %s: %s", s.addr, err)
		return err
	}

	s.Logger.Printf("Started listening on UDP: %s", s.config.BindAddress)

	s.wg.Add(2)
	go s.serve()
	go s.writePoints()

	return nil
}

func (s *Service) writePoints() {
	defer s.wg.Done()

	for {
		select {
		case batch := <-s.batcher.Out():
			if err := s.PointsWriter.WritePoints(&cluster.WritePointsRequest{
				Database:         s.config.Database,
				RetentionPolicy:  s.config.RetentionPolicy,
				ConsistencyLevel: cluster.ConsistencyLevelOne,
				Points:           batch,
			}); err == nil {
				s.statMap.Add(statBatchesTrasmitted, 1)
				s.statMap.Add(statPointsTransmitted, int64(len(batch)))
			} else {
				s.Logger.Printf("failed to write point batch to database %q: %s", s.config.Database, err)
				s.statMap.Add(statBatchesTransmitFail, 1)
			}

		case <-s.done:
			return
		}
	}
}

func (s *Service) serve() {
	defer s.wg.Done()

	s.batcher.Start()
	for {
		buf := make([]byte, UDPBufferSize)

		select {
		case <-s.done:
			// We closed the connection, time to go.
			return
		default:
			// Keep processing.
		}

		n, _, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			s.statMap.Add(statReadFail, 1)
			s.Logger.Printf("Failed to read UDP message: %s", err)
			continue
		}
		s.statMap.Add(statBytesReceived, int64(n))

		points, err := models.ParsePoints(buf[:n])
		if err != nil {
			s.statMap.Add(statPointsParseFail, 1)
			s.Logger.Printf("Failed to parse points: %s", err)
			continue
		}

		for _, point := range points {
			s.batcher.In() <- point
		}
		s.statMap.Add(statPointsReceived, int64(len(points)))
	}
}

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

	s.Logger.Print("Service closed")

	return nil
}

// SetLogger sets the internal logger to the logger passed in.
func (s *Service) SetLogger(l *log.Logger) {
	s.Logger = l
}

func (s *Service) Addr() net.Addr {
	return s.addr
}
