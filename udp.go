package influxdb

import (
	"bytes"
	"encoding/json"
	"net"
	"sync"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/protocol"
)

// UDPServer
type UDPServer struct {
	server *Server

	mu   sync.Mutex
	wg   sync.WaitGroup
	done chan struct{} // close notification

	// The UDP address to listen on.
	Addr *net.UDPAddr

	// The name of the database to insert data into.
	Database string

	// The cluster admin authorized to insert the data.
	User *ClusterAdmin
}

// NewUDPServer returns an instance of UDPServer attached to a Server.
func NewUDPServer(server *Server) *UDPServer {
	return &UDPServer{server: server}
}

// ListenAndServe opens a UDP socket to listen for messages.
func (s *UDPServer) ListenAndServe() {
	// Validate that server has a UDP address.
	if s.UDPAddr == nil {
		return ErrBindAddressRequired
	}

	// Open UDP connection.
	conn, err := net.ListenUDP("udp", s.UDPAddr)
	if err != nil {
		return err
	}
	defer self.conn.Close()

	// Read messages off the connection and handle them.
	buffer := make([]byte, 2048)
	for {
		n, _, err := socket.ReadFromUDP(buffer)
		if err != nil || n == 0 {
			log.Error("UDP ReadFromUDP error: %s", err)
			continue
		}

		// Create a JSON decoder.
		dec := json.NewDecoder(bytes.NewBuffer(buffer[0:n]))
		dec.UseNumber()

		// Deserialize data into series.
		var a []*serializedSeries
		if err := dec.Decode(&serializedSeries); err != nil {
			log.Error("UDP json error: %s", err)
			continue
		}

		// Write data points to the data store.
		for _, s := range a {
			if len(s.Points) == 0 {
				continue
			}

			// Convert to the internal series format.
			series, err := s.Series(s, SecondPrecision)
			if err != nil {
				log.Error("udp cannot convert received data: %s", err)
				continue
			}

			// Write series data to server.
			err = s.server.WriteSeriesData(self.user, self.database, []*protocol.Series{series})
			if err != nil {
				log.Error("udp cannot write data: %s", err)
				continue
			}
		}

	}

}
