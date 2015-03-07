package collectd

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/kimor79/gollectd"
)

// DefaultPort for collectd is 25826
const DefaultPort = 25826

// SeriesWriter defines the interface for the destination of the data.
type SeriesWriter interface {
	WriteSeries(database, retentionPolicy string, points []influxdb.Point) (uint64, error)
}

type Server struct {
	mu sync.Mutex
	wg sync.WaitGroup

	conn *net.UDPConn

	writer      SeriesWriter
	Database    string
	typesdb     gollectd.Types
	typesdbpath string
}

func NewServer(w SeriesWriter, typesDBPath string) *Server {
	s := Server{
		writer:      w,
		typesdbpath: typesDBPath,
		typesdb:     make(gollectd.Types),
	}

	return &s
}

func ListenAndServe(s *Server, iface string) error {
	if iface == "" { // Make sure we have an address
		return errors.New("bind address required")
	} else if s.Database == "" { // Make sure they have a database
		return errors.New("database was not specified in config")
	}

	addr, err := net.ResolveUDPAddr("udp", iface)
	if err != nil {
		return fmt.Errorf("unable to resolve UDP address: %v", err)
	}

	s.typesdb, err = gollectd.TypesDBFile(s.typesdbpath)
	if err != nil {
		return fmt.Errorf("unable to parse typesDBFile: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("unable to listen on UDP: %v", err)
	}
	s.conn = conn

	s.wg.Add(1)
	go s.serve(conn)

	return nil
}

func (s *Server) serve(conn *net.UDPConn) {
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
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil && s.conn != nil {
			log.Printf("Collectd ReadFromUDP error: %s", err)
			continue
		}
		if n > 0 {
			s.handleMessage(buffer[:n])
		}
		if s.conn == nil {
			// we closed the connection, time to go
			return
		}
	}
}

func (s *Server) handleMessage(buffer []byte) {
	packets, err := gollectd.Packets(buffer, s.typesdb)
	if err != nil {
		log.Printf("Collectd parse error: %s", err)
		return
	}

	for _, packet := range *packets {
		points := Unmarshal(&packet)
		for _, p := range points {
			_, err := s.writer.WriteSeries(s.Database, "", []influxdb.Point{p})
			if err != nil {
				log.Printf("Collectd cannot write data: %s", err)
				continue
			}
		}
	}
}

// Close shuts down the server's listeners.
func (s *Server) Close() error {
	// Notify other goroutines of shutdown.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return errors.New("server already closed")
	}
	s.conn.Close()
	s.conn = nil

	// Wait for all goroutines to shutdown.
	s.wg.Wait()
	log.Printf("all waitgroups finished")

	return nil
}

func Unmarshal(data *gollectd.Packet) []influxdb.Point {
	// Prefer high resolution timestamp.
	var timestamp time.Time
	if data.TimeHR > 0 {
		// TimeHR is "near" nanosecond measurement, but not exactly nanasecond time
		// Since we store time in microseconds, we round here (mostly so tests will work easier)
		sec := data.TimeHR >> 30
		// Shifting, masking, and dividing by 1 billion to get nanoseconds.
		nsec := ((data.TimeHR & 0x3FFFFFFF) << 30) / 1000 / 1000 / 1000
		timestamp = time.Unix(int64(sec), int64(nsec)).UTC().Round(time.Microsecond)
	} else {
		// If we don't have high resolution time, fall back to basic unix time
		timestamp = time.Unix(int64(data.Time), 0).UTC()
	}

	var points []influxdb.Point
	for i := range data.Values {
		name := fmt.Sprintf("%s_%s", data.Plugin, data.Values[i].Name)
		tags := make(map[string]string)
		fields := make(map[string]interface{})

		fields[name] = data.Values[i].Value

		if data.Hostname != "" {
			tags["host"] = data.Hostname
		}
		if data.PluginInstance != "" {
			tags["instance"] = data.PluginInstance
		}
		if data.Type != "" {
			tags["type"] = data.Type
		}
		if data.TypeInstance != "" {
			tags["type_instance"] = data.TypeInstance
		}
		p := influxdb.Point{
			Name:      name,
			Tags:      tags,
			Timestamp: timestamp,
			Fields:    fields,
		}

		points = append(points, p)
	}
	return points
}
