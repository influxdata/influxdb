package udp

import (
	"errors"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/tsdb"
)

const (
	UDPBufferSize = 65536
)

type Server struct {
	conn *net.UDPConn
	addr *net.UDPAddr
	wg   sync.WaitGroup
	done chan struct{}

	batcher *tsdb.PointBatcher

	batchSize    int
	batchTimeout time.Duration
	database     string

	PointsWriter interface {
		WritePoints(p *cluster.WritePointsRequest) error
	}

	Logger *log.Logger
}

func NewServer(db string) *Server {
	return &Server{
		done:     make(chan struct{}),
		database: db,
		Logger:   log.New(os.Stderr, "[udp] ", log.LstdFlags),
	}
}

func (s *Server) SetBatchSize(sz int)             { s.batchSize = sz }
func (s *Server) SetBatchTimeout(d time.Duration) { s.batchTimeout = d }

func (s *Server) ListenAndServe(iface string) (err error) {
	if iface == "" {
		return errors.New("bind address has to be specified in config")
	}
	if s.database == "" {
		return errors.New("database has to be specified in config")
	}

	s.addr, err = net.ResolveUDPAddr("udp", iface)
	if err != nil {
		s.Logger.Printf("Failed to resolve UDP address %s: %s", iface, err)
		return err
	}

	s.conn, err = net.ListenUDP("udp", s.addr)
	if err != nil {
		s.Logger.Printf("Failed to set up UDP listener at address %s: %s", s.addr, err)
		return err
	}

	s.Logger.Printf("Started listening on %s", iface)

	s.batcher = tsdb.NewPointBatcher(s.batchSize, s.batchTimeout)

	s.wg.Add(2)
	go s.serve()
	go s.writePoints()

	return nil
}

func (s *Server) writePoints() {
	defer s.wg.Done()

	for {
		select {
		case batch := <-s.batcher.Out():
			err := s.PointsWriter.WritePoints(&cluster.WritePointsRequest{
				Database:         s.database,
				RetentionPolicy:  "",
				ConsistencyLevel: cluster.ConsistencyLevelOne,
				Points:           batch,
			})
			if err != nil {
				s.Logger.Printf("Failed to write point batch to database %q: %s\n", s.database, err)
			} else {
				s.Logger.Printf("Wrote a batch of %d points to %s", len(batch), s.database)
			}
		case <-s.done:
			return
		}
	}
}

func (s *Server) serve() {
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
			s.Logger.Printf("Failed to read UDP message: %s", err)
			continue
		}

		points, err := tsdb.ParsePoints(buf[:n])
		if err != nil {
			s.Logger.Printf("Failed to parse points: %s", err)
			continue
		}

		s.Logger.Printf("Received write for %d points on database %s", len(points), s.database)

		for _, point := range points {
			s.batcher.In() <- point
		}
	}
}

func (s *Server) Close() error {
	if s.conn == nil {
		return errors.New("Server already closed")
	}

	s.conn.Close()
	s.batcher.Flush()
	close(s.done)
	s.wg.Wait()

	// Release all remaining resources.
	s.done = nil
	s.conn = nil

	s.Logger.Print("Server closed")

	return nil
}

func (s *Server) Addr() net.Addr {
	return s.addr
}
