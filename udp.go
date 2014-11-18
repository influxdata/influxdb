package influxdb

import (
	"net"
	"sync"
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
func (s *UDPServer) ListenAndServe() error {
	panic("not yet implemented: UDPServer.ListenAndServe()")

	/* TEMPORARILY REMOVED FOR PROTOBUFS.
	// Validate that server has a UDP address.
	if s.Addr == nil {
		return ErrBindAddressRequired
	}

	// Open UDP connection.
	conn, err := net.ListenUDP("udp", s.Addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Read messages off the connection and handle them.
	buffer := make([]byte, 2048)
	for {
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil || n == 0 {
			log.Error("UDP ReadFromUDP error: %s", err)
			continue
		}

		// Create a JSON decoder.
		dec := json.NewDecoder(bytes.NewBuffer(buffer[0:n]))
		dec.UseNumber()

		// Deserialize data into series.
		var a []*serializedSeries
		if err := dec.Decode(&a); err != nil {
			log.Error("UDP json error: %s", err)
			continue
		}

		// Write data points to the data store.
		for _, ss := range a {
			if len(ss.Points) == 0 {
				continue
			}

			// Convert to the internal series format.
			series, err := ss.series(SecondPrecision)
			if err != nil {
				log.Error("udp cannot convert received data: %s", err)
				continue
			}

			// TODO: Authorization.

			// Lookup database.
			db := s.server.Database(s.Database)
			if db == nil {
				log.Error("udp: %s", ErrDatabaseNotFound)
				continue
			}

			// Write series data to server.
			if err := db.WriteSeries(series); err != nil {
				log.Error("udp: write data error: %s", err)
				continue
			}
		}

	}
	*/
}
