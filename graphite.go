package influxdb

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strings"
	"sync"

	log "code.google.com/p/log4go"
)

var (
	// ErrBindAddressRequired is returned when starting the GraphiteServer
	// without a TCP or UDP listening address.
	ErrBindAddressRequired = errors.New("bind address required")

	// ErrGraphiteServerClosed return when closing an already closed graphite server.
	ErrGraphiteServerClosed = errors.New("graphite server already closed")
)

// GraphiteListener provides a tcp and/or udp listener that you can
// use to ingest metrics into influxdb via the graphite protocol.  it
// behaves as a carbon daemon, except:
//
// no rounding of timestamps to the nearest interval.  Upon ingestion
// of multiple datapoints for a given key within the same interval
// (possibly but not necessarily the same timestamp), graphite would
// use one (the latest received) value with a rounded timestamp
// representing that interval.  We store values for every timestamp we
// receive (only the latest value for a given metric-timestamp pair)
// so it's up to the user to feed the data in proper intervals (and
// use round intervals if you plan to rely on that)
type GraphiteServer struct {
	server *Server

	mu   sync.Mutex
	wg   sync.WaitGroup
	done chan struct{} // close notification

	// The TCP address to listen on.
	TCPAddr *net.TCPAddr

	// The UDP address to listen on.
	UDPAddr *net.UDPAddr

	// The name of the database to insert data into.
	Database string

	// The cluster admin authorized to insert the data.
	User *User
}

// NewGraphiteServer returns an instance of GraphiteServer attached to a Server.
func NewGraphiteServer(server *Server) *GraphiteServer {
	return &GraphiteServer{server: server}
}

// ListenAndServe opens TCP (and optionally a UDP) socket to listen for messages.
func (s *GraphiteServer) ListenAndServe() error {
	// Make sure we have either a TCP address or a UDP address.
	// Also validate that there is an admin user to insert data as.
	if s.TCPAddr == nil && s.UDPAddr == nil {
		return ErrBindAddressRequired
	} else if s.User != nil {
		return ErrUserNotFound
	}

	// Create a new close notification channel.
	done := make(chan struct{}, 0)
	s.done = done

	// Open the TCP connection.
	if s.TCPAddr != nil {
		l, err := net.ListenTCP("tcp", s.TCPAddr)
		if err != nil {
			return err
		}
		defer func() { _ = l.Close() }()

		s.wg.Add(1)
		go s.serveTCP(l, done)
	}

	// Open the UDP connection.
	if s.UDPAddr != nil {
		l, err := net.ListenUDP("udp", s.UDPAddr)
		if err != nil {
			return err
		}
		defer func() { _ = l.Close() }()

		s.wg.Add(1)
		go s.serveUDP(l, done)
	}

	return nil
}

// serveTCP handles incoming TCP connection requests.
func (s *GraphiteServer) serveTCP(l *net.TCPListener, done chan struct{}) {
	defer s.wg.Done()

	// Listen for server close.
	go func() {
		<-done
		l.Close()
	}()

	// Listen for new TCP connections.
	for {
		c, err := l.Accept()
		if err != nil {
			// TODO(benbjohnson): Check for connection closed.
			log.Error("GraphiteServer: Accept: ", err)
			continue
		}

		s.wg.Add(1)
		go s.handleTCPConn(c)
	}
}

func (s *GraphiteServer) handleTCPConn(conn net.Conn) {
	defer conn.Close()
	defer s.wg.Done()

	reader := bufio.NewReader(conn)
	for {
		err := s.handleMessage(reader)
		if err != nil {
			if io.EOF == err {
				log.Debug("GraphiteServer: Client closed graphite connection")
				return
			}
			log.Error("GraphiteServer:", err)
		}
	}
}

// serveUDP handles incoming UDP messages.
func (s *GraphiteServer) serveUDP(conn *net.UDPConn, done chan struct{}) {
	defer s.wg.Done()

	// Listen for server close.
	go func() {
		<-done
		conn.Close()
	}()

	buf := make([]byte, 65536)
	for {
		// Read from connection.
		n, _, err := conn.ReadFromUDP(buf)
		if err == io.EOF {
			return
		} else if err != nil {
			log.Warn("GraphiteServer: Error when reading from UDP connection %s", err.Error())
		}

		// Read in data in a separate goroutine.
		s.wg.Add(1)
		go s.handleUDPMessage(string(buf[:n]))
	}
}

// handleUDPMessage splits a UDP packet by newlines and processes each message.
func (s *GraphiteServer) handleUDPMessage(msg string) {
	defer s.wg.Done()
	for _, metric := range strings.Split(msg, "\n") {
		s.handleMessage(bufio.NewReader(strings.NewReader(metric + "\n")))
	}
}

// Close shuts down the server's listeners.
func (s *GraphiteServer) Close() error {
	// Notify other goroutines of shutdown.
	s.mu.Lock()
	if s.done == nil {
		s.mu.Unlock()
		return ErrGraphiteServerClosed
	}
	close(s.done)
	s.done = nil
	s.mu.Unlock()

	// Wait for all goroutines to shutdown.
	s.wg.Wait()
	return nil
}

// handleMessage decodes a graphite message from the reader and sends it to the
// committer goroutine.
func (s *GraphiteServer) handleMessage(r *bufio.Reader) error {
	panic("not yet implemented: GraphiteServer.handleMessage()")
	/* TEMPORARILY REMOVED FOR PROTOBUFS.
		// Decode graphic metric.
		m, err := decodeGraphiteMetric(r)
		if err != nil {
			return err
		}


		// Convert metric to a field value.
		v := &protocol.FieldValue{}
		if m.isInt {
			v.Int64Value = &m.integerValue
		} else {
			v.DoubleValue = &m.floatValue
		}

		// Use a single sequence number to make sure last write wins.
		sn := uint64(1)

		// Send data point to committer.
		p := &protocol.Point{
			Timestamp:      &m.timestamp,
			Values:         []*protocol.FieldValue{v},
			SequenceNumber: &sn,
		}

		// Write data to server.
		series := &protocol.Series{
			Name:   &m.name,
			Fields: []string{"value"},
			Points: []*protocol.Point{p},
		}

		// TODO: Validate user.

		// Look up database.
		db := s.server.Database(s.Database)
		if db == nil {
			return ErrDatabaseNotFound
		}

		// Write series data to database.
		if err := db.WriteSeries(series); err != nil {
			return fmt.Errorf("write series data: %s", err)
		}

		return nil
	}

	type graphiteMetric struct {
		name         string
		isInt        bool
		integerValue int64
		floatValue   float64
		timestamp    int64
	}

	// returns err == io.EOF when we hit EOF without any further data
	func decodeGraphiteMetric(r *bufio.Reader) (*graphiteMetric, error) {
		// Read up to the next newline.
		buf, err := r.ReadBytes('\n')
		str := strings.TrimSpace(string(buf))
		if err != nil {
			if err != io.EOF {
				return nil, fmt.Errorf("connection closed uncleanly/broken: %s\n", err.Error())
			}
			if str == "" {
				return nil, err
			}
			// else we got EOF but also data, so just try to process it as valid data
		}

		// Break into 3 fields (name, value, timestamp).
		fields := strings.Fields(str)
		if len(fields) != 3 {
			return nil, fmt.Errorf("received '%s' which doesn't have three fields", str)
		}

		// Create a metric.
		m := &graphiteMetric{name: fields[0]}

		// Parse value.
		v, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return nil, err
		}

		// Determine if value is a float or an int.
		if i := int64(v); float64(i) == v {
			m.integerValue, m.isInt = int64(v), true
		} else {
			m.floatValue = v
		}

		// Parse timestamp.
		timestamp, err := strconv.ParseUint(fields[2], 10, 32)
		if err != nil {
			return nil, err
		}
		m.timestamp = int64(timestamp) * int64(time.Millisecond)

		return m, nil
	*/
}
