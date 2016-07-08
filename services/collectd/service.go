package collectd // import "github.com/influxdata/influxdb/services/collectd"

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/kimor79/gollectd"
)

// statistics gathered by the collectd service.
const (
	statPointsReceived       = "pointsRx"
	statBytesReceived        = "bytesRx"
	statPointsParseFail      = "pointsParseFail"
	statReadFail             = "readFail"
	statBatchesTransmitted   = "batchesTx"
	statPointsTransmitted    = "pointsTx"
	statBatchesTransmitFail  = "batchesTxFail"
	statDroppedPointsInvalid = "droppedPointsInvalid"
)

// pointsWriter is an internal interface to make testing easier.
type pointsWriter interface {
	WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
}

// metaStore is an internal interface to make testing easier.
type metaClient interface {
	CreateDatabase(name string) (*meta.DatabaseInfo, error)
}

// Service represents a UDP server which receives metrics in collectd's binary
// protocol and stores them in InfluxDB.
type Service struct {
	Config       *Config
	MetaClient   metaClient
	PointsWriter pointsWriter
	Logger       *log.Logger

	wg      sync.WaitGroup
	err     chan error
	stop    chan struct{}
	conn    *net.UDPConn
	batcher *tsdb.PointBatcher
	typesdb gollectd.Types
	addr    net.Addr

	// expvar-based stats.
	stats    *Statistics
	statTags models.Tags
}

// NewService returns a new instance of the collectd service.
func NewService(c Config) *Service {
	s := Service{
		// Use defaults where necessary.
		Config: c.WithDefaults(),

		Logger:   log.New(os.Stderr, "[collectd] ", log.LstdFlags),
		err:      make(chan error),
		stats:    &Statistics{},
		statTags: map[string]string{"bind": c.BindAddress},
	}

	return &s
}

// Open starts the service.
func (s *Service) Open() error {
	s.Logger.Printf("Starting collectd service")

	if s.Config.BindAddress == "" {
		return fmt.Errorf("bind address is blank")
	} else if s.Config.Database == "" {
		return fmt.Errorf("database name is blank")
	} else if s.PointsWriter == nil {
		return fmt.Errorf("PointsWriter is nil")
	}

	if _, err := s.MetaClient.CreateDatabase(s.Config.Database); err != nil {
		s.Logger.Printf("Failed to ensure target database %s exists: %s", s.Config.Database, err.Error())
		return err
	}

	if s.typesdb == nil {
		// Open collectd types.
		if stat, err := os.Stat(s.Config.TypesDB); err != nil {
			return fmt.Errorf("Stat(): %s", err)
		} else if stat.IsDir() {
			alltypesdb := make(gollectd.Types)
			var readdir func(path string)
			readdir = func(path string) {
				files, err := ioutil.ReadDir(path)
				if err != nil {
					s.Logger.Printf("Unable to read directory %s: %s\n", path, err)
					return
				}

				for _, f := range files {
					fullpath := filepath.Join(path, f.Name())
					if f.IsDir() {
						readdir(fullpath)
						continue
					}

					s.Logger.Printf("Loading %s\n", fullpath)
					types, err := gollectd.TypesDBFile(fullpath)
					if err != nil {
						s.Logger.Printf("Unable to parse collectd types file: %s\n", f.Name())
						continue
					}

					for k, t := range types {
						a, ok := alltypesdb[k]
						if ok {
							alltypesdb[k] = t
						} else {
							alltypesdb[k] = append(a, t...)
						}
					}
				}
			}
			readdir(s.Config.TypesDB)
			s.typesdb = alltypesdb
		} else {
			s.Logger.Printf("Loading %s\n", s.Config.TypesDB)
			typesdb, err := gollectd.TypesDBFile(s.Config.TypesDB)
			if err != nil {
				return fmt.Errorf("Open(): %s", err)
			}
			s.typesdb = typesdb
		}
	}

	// Resolve our address.
	addr, err := net.ResolveUDPAddr("udp", s.Config.BindAddress)
	if err != nil {
		return fmt.Errorf("unable to resolve UDP address: %s", err)
	}
	s.addr = addr

	// Start listening
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("unable to listen on UDP: %s", err)
	}

	if s.Config.ReadBuffer != 0 {
		err = conn.SetReadBuffer(s.Config.ReadBuffer)
		if err != nil {
			return fmt.Errorf("unable to set UDP read buffer to %d: %s",
				s.Config.ReadBuffer, err)
		}
	}
	s.conn = conn

	s.Logger.Println("Listening on UDP: ", conn.LocalAddr().String())

	// Start the points batcher.
	s.batcher = tsdb.NewPointBatcher(s.Config.BatchSize, s.Config.BatchPending, time.Duration(s.Config.BatchDuration))
	s.batcher.Start()

	// Create channel and wait group for signalling goroutines to stop.
	s.stop = make(chan struct{})
	s.wg.Add(2)

	// Start goroutines that process collectd packets.
	go s.serve()
	go s.writePoints()

	return nil
}

// Close stops the service.
func (s *Service) Close() error {
	// Close the connection, and wait for the goroutine to exit.
	if s.stop != nil {
		close(s.stop)
	}
	if s.conn != nil {
		s.conn.Close()
	}
	if s.batcher != nil {
		s.batcher.Stop()
	}
	s.wg.Wait()

	// Release all remaining resources.
	s.stop = nil
	s.conn = nil
	s.batcher = nil
	s.Logger.Println("collectd UDP closed")
	return nil
}

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (s *Service) SetLogOutput(w io.Writer) {
	s.Logger = log.New(w, "[collectd] ", log.LstdFlags)
}

// Statistics maintains statistics for the collectd service.
type Statistics struct {
	PointsReceived       int64
	BytesReceived        int64
	PointsParseFail      int64
	ReadFail             int64
	BatchesTransmitted   int64
	PointsTransmitted    int64
	BatchesTransmitFail  int64
	InvalidDroppedPoints int64
}

// Statistics returns statistics for periodic monitoring.
func (s *Service) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "collectd",
		Tags: s.statTags.Merge(tags),
		Values: map[string]interface{}{
			statPointsReceived:       atomic.LoadInt64(&s.stats.PointsReceived),
			statBytesReceived:        atomic.LoadInt64(&s.stats.BytesReceived),
			statPointsParseFail:      atomic.LoadInt64(&s.stats.PointsParseFail),
			statReadFail:             atomic.LoadInt64(&s.stats.ReadFail),
			statBatchesTransmitted:   atomic.LoadInt64(&s.stats.BatchesTransmitted),
			statPointsTransmitted:    atomic.LoadInt64(&s.stats.PointsTransmitted),
			statBatchesTransmitFail:  atomic.LoadInt64(&s.stats.BatchesTransmitFail),
			statDroppedPointsInvalid: atomic.LoadInt64(&s.stats.InvalidDroppedPoints),
		},
	}}
}

// SetTypes sets collectd types db.
func (s *Service) SetTypes(types string) (err error) {
	s.typesdb, err = gollectd.TypesDB([]byte(types))
	return
}

// Err returns a channel for fatal errors that occur on go routines.
func (s *Service) Err() chan error { return s.err }

// Addr returns the listener's address. Returns nil if listener is closed.
func (s *Service) Addr() net.Addr {
	return s.conn.LocalAddr()
}

func (s *Service) serve() {
	defer s.wg.Done()

	// From https://collectd.org/wiki/index.php/Binary_protocol
	//   1024 bytes (payload only, not including UDP / IP headers)
	//   In versions 4.0 through 4.7, the receive buffer has a fixed size
	//   of 1024 bytes. When longer packets are received, the trailing data
	//   is simply ignored. Since version 4.8, the buffer size can be
	//   configured. Version 5.0 will increase the default buffer size to
	//   1452 bytes (the maximum payload size when using UDP/IPv6 over
	//   Ethernet).
	buffer := make([]byte, 1452)

	for {
		select {
		case <-s.stop:
			// We closed the connection, time to go.
			return
		default:
			// Keep processing.
		}

		n, _, err := s.conn.ReadFromUDP(buffer)
		if err != nil {
			atomic.AddInt64(&s.stats.ReadFail, 1)
			s.Logger.Printf("collectd ReadFromUDP error: %s", err)
			continue
		}
		if n > 0 {
			atomic.AddInt64(&s.stats.BytesReceived, int64(n))
			s.handleMessage(buffer[:n])
		}
	}
}

func (s *Service) handleMessage(buffer []byte) {
	packets, err := gollectd.Packets(buffer, s.typesdb)
	if err != nil {
		atomic.AddInt64(&s.stats.PointsParseFail, 1)
		s.Logger.Printf("Collectd parse error: %s", err)
		return
	}
	for _, packet := range *packets {
		points := s.UnmarshalCollectd(&packet)
		for _, p := range points {
			s.batcher.In() <- p
		}
		atomic.AddInt64(&s.stats.PointsReceived, int64(len(points)))
	}
}

func (s *Service) writePoints() {
	defer s.wg.Done()

	for {
		select {
		case <-s.stop:
			return
		case batch := <-s.batcher.Out():
			if err := s.PointsWriter.WritePoints(s.Config.Database, s.Config.RetentionPolicy, models.ConsistencyLevelAny, batch); err == nil {
				atomic.AddInt64(&s.stats.BatchesTransmitted, 1)
				atomic.AddInt64(&s.stats.PointsTransmitted, int64(len(batch)))
			} else {
				s.Logger.Printf("failed to write point batch to database %q: %s", s.Config.Database, err)
				atomic.AddInt64(&s.stats.BatchesTransmitFail, 1)
			}
		}
	}
}

// Unmarshal translates a collectd packet into InfluxDB data points.
func (s *Service) UnmarshalCollectd(packet *gollectd.Packet) []models.Point {
	// Prefer high resolution timestamp.
	var timestamp time.Time
	if packet.TimeHR > 0 {
		// TimeHR is "near" nanosecond measurement, but not exactly nanasecond time
		// Since we store time in microseconds, we round here (mostly so tests will work easier)
		sec := packet.TimeHR >> 30
		// Shifting, masking, and dividing by 1 billion to get nanoseconds.
		nsec := ((packet.TimeHR & 0x3FFFFFFF) << 30) / 1000 / 1000 / 1000
		timestamp = time.Unix(int64(sec), int64(nsec)).UTC().Round(time.Microsecond)
	} else {
		// If we don't have high resolution time, fall back to basic unix time
		timestamp = time.Unix(int64(packet.Time), 0).UTC()
	}

	var points []models.Point
	for i := range packet.Values {
		name := fmt.Sprintf("%s_%s", packet.Plugin, packet.Values[i].Name)
		tags := make(map[string]string)
		fields := make(map[string]interface{})

		fields["value"] = packet.Values[i].Value

		if packet.Hostname != "" {
			tags["host"] = packet.Hostname
		}
		if packet.PluginInstance != "" {
			tags["instance"] = packet.PluginInstance
		}
		if packet.Type != "" {
			tags["type"] = packet.Type
		}
		if packet.TypeInstance != "" {
			tags["type_instance"] = packet.TypeInstance
		}
		p, err := models.NewPoint(name, tags, fields, timestamp)
		// Drop invalid points
		if err != nil {
			s.Logger.Printf("Dropping point %v: %v", name, err)
			atomic.AddInt64(&s.stats.InvalidDroppedPoints, 1)
			continue
		}

		points = append(points, p)
	}
	return points
}
