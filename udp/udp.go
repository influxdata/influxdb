package udp

import (
	"bytes"
	"encoding/json"
	"github.com/influxdb/influxdb"
	"log"
	"net"
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
		log.Printf("Failed resolve UDP address %s. Error is %s", iface, err)
		return err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Printf("Failed set up UDP listener at address %s. Error is %s", addr, err)
		return err
	}

	var bp influxdb.BatchPoints
	buf := make([]byte, udpBufferSize)

	go func() {
		for {
			_, remote, err := conn.ReadFromUDP(buf)
			if err != nil {
				log.Printf("Failed read UDP message. Error is %s.", err)
				continue
			}

			dec := json.NewDecoder(bytes.NewReader(buf))
			if err := dec.Decode(&bp); err != nil {
				log.Printf("Failed decode JSON UDP message")
				msgUDP := []byte("Failed to decode your message")
				conn.WriteToUDP(msgUDP, remote)
				continue
			}

			points, err := influxdb.NormalizeBatchPoints(bp)
			if err != nil {
				log.Printf("Failed normalize batch points")
				msgUDP := []byte("Failed find points in your message")
				conn.WriteToUDP(msgUDP, remote)
				continue
			}

			if msgIndex, err := u.writer.WriteSeries(bp.Database, bp.RetentionPolicy, points); err != nil {
				log.Printf("Server write failed. Message index was %d. Error is %s.", msgIndex, err)
				msgUDP := []byte("Failed to write series to the database")
				conn.WriteToUDP(msgUDP, remote)
			} else {
				msgUDP := []byte("Write OK")
				conn.WriteToUDP(msgUDP, remote)
			}
		}
	}()
	return nil
}
