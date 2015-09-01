package graphite

import (
	"bufio"
	"expvar"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/monitor"
	"github.com/influxdb/influxdb/tsdb"
)

const (
	udpBufferSize     = 65536
	leaderWaitTimeout = 30 * time.Second
)

// expvar stats maintained by the Graphite package.
var monitorOnce sync.Once
var statMap = expvar.NewMap("graphite")
var statMapTCP = expvar.NewMap("tcp")
var statMapUDP = expvar.NewMap("udp")

// Build the graphite expvar hierarchy.
func init() {
	statMap.Set("tcp", statMapTCP)
	statMap.Set("udp", statMapUDP)
}

// statistics gathered by the graphite package.
const (
	statPointsReceived      = "points_rx"
	statBytesReceived       = "bytes_rx"
	statPointsParseFail     = "points_parse_fail"
	statPointsUnsupported   = "points_unsupported_fail"
	statBatchesTrasmitted   = "batches_tx"
	statPointsTransmitted   = "points_tx"
	statBatchesTransmitFail = "batches_tx_fail"
	statConnectionsActive   = "connections_active"
	statConnectionsHandled  = "connections_handled"
)

type Service struct {
	bindAddress      string
	database         string
	protocol         string
	batchSize        int
	batchTimeout     time.Duration
	consistencyLevel cluster.ConsistencyLevel

	batcher *tsdb.PointBatcher
	parser  *Parser

	logger  *log.Logger
	statMap *expvar.Map

	ln   net.Listener
	addr net.Addr

	wg   sync.WaitGroup
	done chan struct{}

	MonitorService interface {
		Register(name string, tags map[string]string, client monitor.Client) error
	}
	PointsWriter interface {
		WritePoints(p *cluster.WritePointsRequest) error
	}
	MetaStore interface {
		WaitForLeader(d time.Duration) error
		CreateDatabaseIfNotExists(name string) (*meta.DatabaseInfo, error)
	}
}

// NewService returns an instance of the Graphite service.
func NewService(c Config) (*Service, error) {
	// Use defaults where necessary.
	d := c.WithDefaults()

	s := Service{
		bindAddress:  d.BindAddress,
		database:     d.Database,
		protocol:     d.Protocol,
		batchSize:    d.BatchSize,
		batchTimeout: time.Duration(d.BatchTimeout),
		logger:       log.New(os.Stderr, "[graphite] ", log.LstdFlags),
		done:         make(chan struct{}),
	}

	consistencyLevel, err := cluster.ParseConsistencyLevel(d.ConsistencyLevel)
	if err != nil {
		return nil, err
	}
	s.consistencyLevel = consistencyLevel

	parser, err := NewParserWithOptions(Options{
		Templates:   d.Templates,
		DefaultTags: d.DefaultTags(),
		Separator:   d.Separator})

	if err != nil {
		return nil, err
	}
	s.parser = parser

	return &s, nil
}

// Open starts the Graphite input processing data.
func (s *Service) Open() error {
	s.logger.Printf("Starting graphite service, batch size %d, batch timeout %s", s.batchSize, s.batchTimeout)

	// One Graphite service hooks up monitoring for all Graphite functionality.
	monitorOnce.Do(func() {
		if s.MonitorService == nil {
			s.logger.Println("no monitor service available, no monitoring will be performed")
			return
		}

		t := monitor.NewMonitorClient(statMapTCP)
		s.MonitorService.Register("graphite", map[string]string{"proto": "tcp"}, t)

		u := monitor.NewMonitorClient(statMapUDP)
		s.MonitorService.Register("graphite", map[string]string{"proto": "udp"}, u)
	})

	if err := s.MetaStore.WaitForLeader(leaderWaitTimeout); err != nil {
		s.logger.Printf("Failed to detect a cluster leader: %s", err.Error())
		return err
	}

	if _, err := s.MetaStore.CreateDatabaseIfNotExists(s.database); err != nil {
		s.logger.Printf("Failed to ensure target database %s exists: %s", s.database, err.Error())
		return err
	}

	s.batcher = tsdb.NewPointBatcher(s.batchSize, s.batchTimeout)
	s.batcher.Start()

	// Start processing batches.
	s.wg.Add(1)
	go s.processBatches(s.batcher)

	var err error
	if strings.ToLower(s.protocol) == "tcp" {
		s.addr, err = s.openTCPServer()
	} else if strings.ToLower(s.protocol) == "udp" {
		s.addr, err = s.openUDPServer()
	} else {
		return fmt.Errorf("unrecognized Graphite input protocol %s", s.protocol)
	}
	if err != nil {
		return err
	}

	s.logger.Printf("Listening on %s: %s", strings.ToUpper(s.protocol), s.addr.String())
	return nil
}

// Close stops all data processing on the Graphite input.
func (s *Service) Close() error {
	if s.ln != nil {
		s.ln.Close()
	}

	s.batcher.Stop()
	close(s.done)
	s.wg.Wait()
	s.done = nil

	return nil
}

// SetLogger sets the internal logger to the logger passed in.
func (s *Service) SetLogger(l *log.Logger) {
	s.logger = l
}

func (s *Service) Addr() net.Addr {
	return s.addr
}

// openTCPServer opens the Graphite input in TCP mode and starts processing data.
func (s *Service) openTCPServer() (net.Addr, error) {
	ln, err := net.Listen("tcp", s.bindAddress)
	if err != nil {
		return nil, err
	}
	s.ln = ln

	// Point at the TCP stats.
	s.statMap = statMapTCP

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.ln.Accept()
			if opErr, ok := err.(*net.OpError); ok && !opErr.Temporary() {
				s.logger.Println("graphite TCP listener closed")
				return
			}
			if err != nil {
				s.logger.Println("error accepting TCP connection", err.Error())
				continue
			}

			s.wg.Add(1)
			go s.handleTCPConnection(conn)
		}
	}()
	return ln.Addr(), nil
}

// handleTCPConnection services an individual TCP connection for the Graphite input.
func (s *Service) handleTCPConnection(conn net.Conn) {
	defer conn.Close()
	defer s.wg.Done()
	defer s.statMap.Add(statConnectionsActive, -1)
	s.statMap.Add(statConnectionsActive, 1)
	s.statMap.Add(statConnectionsHandled, 1)

	reader := bufio.NewReader(conn)

	for {
		// Read up to the next newline.
		buf, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}

		// Trim the buffer, even though there should be no padding
		line := strings.TrimSpace(string(buf))

		s.statMap.Add(statPointsReceived, 1)
		s.statMap.Add(statBytesReceived, int64(len(buf)))
		s.handleLine(line)
	}
}

// openUDPServer opens the Graphite input in UDP mode and starts processing incoming data.
func (s *Service) openUDPServer() (net.Addr, error) {
	addr, err := net.ResolveUDPAddr("udp", s.bindAddress)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	// Point at the UDP stats.
	s.statMap = statMapUDP

	buf := make([]byte, udpBufferSize)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				conn.Close()
				return
			}

			lines := strings.Split(string(buf[:n]), "\n")
			for _, line := range lines {
				s.handleLine(line)
			}
			s.statMap.Add(statPointsReceived, int64(len(lines)))
			s.statMap.Add(statBytesReceived, int64(n))
		}
	}()
	return conn.LocalAddr(), nil
}

func (s *Service) handleLine(line string) {
	if line == "" {
		return
	}

	// Parse it.
	point, err := s.parser.Parse(line)
	if err != nil {
		s.logger.Printf("unable to parse line: %s", err)
		s.statMap.Add(statPointsParseFail, 1)
		return
	}

	f, ok := point.Fields()["value"].(float64)
	if ok {
		// Drop NaN and +/-Inf data points since they are not supported values
		if math.IsNaN(f) || math.IsInf(f, 0) {
			s.logger.Printf("dropping unsupported value: '%v'", line)
			s.statMap.Add(statPointsUnsupported, 1)
			return
		}
	}

	s.batcher.In() <- point
}

// processBatches continually drains the given batcher and writes the batches to the database.
func (s *Service) processBatches(batcher *tsdb.PointBatcher) {
	defer s.wg.Done()
	for {
		select {
		case batch := <-batcher.Out():
			if err := s.PointsWriter.WritePoints(&cluster.WritePointsRequest{
				Database:         s.database,
				RetentionPolicy:  "",
				ConsistencyLevel: s.consistencyLevel,
				Points:           batch,
			}); err == nil {
				s.statMap.Add(statBatchesTrasmitted, 1)
				s.statMap.Add(statPointsTransmitted, int64(len(batch)))
			} else {
				s.logger.Printf("failed to write point batch to database %q: %s", s.database, err)
				s.statMap.Add(statBatchesTransmitFail, 1)
			}

		case <-s.done:
			return
		}
	}
}
