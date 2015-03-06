package udp

import (
	"bytes"
	"encoding/json"
	"log"
	"net"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/client"
)

const (
	udpBufferSize = 65536
)

// SeriesWriter defines the interface for the destination of the data.
type SeriesWriter interface {
	WriteSeries(database, retentionPolicy string, points []influxdb.Point) (uint64, error)
}

// UDPServer
type UDPServer struct {
	writer SeriesWriter
}

// NewUDPServer returns a new instance of a UDPServer
func NewUDPServer(w SeriesWriter) *UDPServer {
	u := UDPServer{
		writer: w,
	}
	return &u
}

// ListenAndServe binds the server to the given UDP interface.
func (u *UDPServer) ListenAndServe(iface string) error {
	addr, err := net.ResolveUDPAddr("udp", iface)
	if err != nil {
		log.Printf("Failed resolve UDP address %s: %s", iface, err)
		return err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Printf("Failed set up UDP listener at address %s: %s", addr, err)
		return err
	}

	var bp client.BatchPoints
	buf := make([]byte, udpBufferSize)

	go func() {
		for {
			_, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				log.Printf("Failed read UDP message: %s.", err)
				continue
			}

			dec := json.NewDecoder(bytes.NewReader(buf))
			if err := dec.Decode(&bp); err != nil {
				log.Printf("Failed decode JSON UDP message")
				continue
			}

			points, err := influxdb.NormalizeBatchPoints(bp)
			if err != nil {
				log.Printf("Failed normalize batch points")
				continue
			}

			if msgIndex, err := u.writer.WriteSeries(bp.Database, bp.RetentionPolicy, points); err != nil {
				log.Printf("Server write failed. Message index was %d: %s", msgIndex, err)
			}
		}
	}()
	return nil
}
