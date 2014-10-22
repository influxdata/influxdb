package influxdb

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/protocol"
)

var (
	// ErrBindAddressRequired is returned when starting the GraphiteServer
	// without a TCP or UDP listening address.
	ErrBindAddressRequired = errors.New("bind address required")

	// ErrClusterAdminNotFound is returned when starting the GraphiteServer
	// and a cluster admin is not found in the configuration.
	ErrClusterAdminNotFound = errors.New("cluster admin not found")
)

// the ingest could in theory be anything from 1 series(datapoint) every few minutes, upto millions of datapoints every second.
// and there might be multiple connections, each delivering a certain (not necessarily equal) fraction of the total.
// we want ingested points to be ingested (flushed) quickly (let's say at least within 100ms), but since coordinator.WriteSeriesData
// comes with a cost, we also want to buffer up the data, the more the better (up to a limit).
// So basically we need to trade these concepts off against each other, by committing

// a batch of series every commit_max_wait ms or every commit_capacity datapoints, whichever is first.

// upto how many points/series to commit in 1 go?
// let's say buffer up to about 1MiB of data, at 24B per record -> 43690 records, let's make it an even 40k
const commitCapacity = 40000

// how long to wait max before flushing a commit payload
const commitMaxWait = 100 * time.Millisecond

// the write commit payload should get written in a timely fashion.
// if not, the channel that feeds the committer will queue up to max_queue series, and then
// block, creating backpressure to the client.  This allows to keep receiving metrics while
// a flush is ongoing, but not much more than we can actually handle.
const maxQueue = 20000

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
	User *ClusterAdmin
}

// NewGraphiteServer returns an instance of GraphiteServer attached to a Server.
func NewGraphiteServer(server *Server) *GraphiteServer {
	return &GraphiteServer{server: server}
}

// getAuth assures that the user property is a user with access to the graphite database
// only call this function after everything (i.e. Raft) is initialized, so that there's at least 1 admin user
/* TODO: move to runner
func (s *GraphiteServer) getAuth() {
	// just use any (the first) of the list of admins.
	names := s.clusterConfig.GetClusterAdmins()
	s.user = s.clusterConfig.GetClusterAdmin(names[0])
}
*/

// ListenAndServe opens TCP (and optionally a UDP) socket to listen for messages.
func (s *GraphiteServer) ListenAndServe() error {
	// Make sure we have either a TCP address or a UDP address.
	// Also validate that there is an admin user to insert data as.
	if s.TCPAddr == nil && s.UDPAddr == nil {
		return ErrBindAddressRequired
	} else if s.User != nil {
		return ErrClusterAdminNotFound
	}

	// Create a new close notification channel.
	done := make(chan struct{}, 0)
	s.done = done

	// Open the TCP connection.
	if s.TCPAddr != nil {
		conn, err := net.ListenTCP("tcp", s.TCPAddr)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		go s.serveTCP(conn, done)
	}

	// Open the UDP connection.
	if s.UDPAddr != nil {
		conn, err := net.ListenUDP("udp", s.UDPAddr)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		go s.serveUDP(conn, done)
	}

	// Start separate goroutine to commit data.
	go s.committer(done)

	return nil
}

// serveTCP handles incoming TCP connection requests.
func (s *GraphiteServer) serveTCP(l *net.TCPListener, done chan struct{}) {
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
	buf := make([]byte, 65536)
	for {
		// Read from connection.
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
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
	s.records <- graphiteRecord{m.name, point}

	return nil
}
func (s *GraphiteServer) committer(done chan struct{}) {
	defer func() { s.done <- true }()

	commit := func(toCommit map[string]*protocol.Series) {
		if len(toCommit) == 0 {
			return
		}
		commitPayload := make([]*protocol.Series, len(toCommit))
		i := 0
		for _, serie := range toCommit {
			commitPayload[i] = serie
			i++
		}
		log.Debug("GraphiteServer committing %d series", len(toCommit))
		err := s.coordinator.WriteSeriesData(s.user, s.database, commitPayload)
		if err != nil {
			switch err.(type) {
			case AuthorizationError:
				// user information got stale, get a fresh one (this should happen rarely)
				s.getAuth()
				err = s.coordinator.WriteSeriesData(s.user, s.database, commitPayload)
				if err != nil {
					log.Warn("GraphiteServer: failed to write %d series after getting new auth: %s\n", len(toCommit), err.Error())
				}
			default:
				log.Warn("GraphiteServer: failed write %d series: %s\n", len(toCommit), err.Error())
			}
		}
	}

	timer := time.NewTimer(commit_max_wait)
	toCommit := make(map[string]*protocol.Series)
	pointsPending := 0

CommitLoop:
	for {
		select {
		case record, ok := <-s.records:
			if ok {
				pointsPending += 1
				if series, seen := toCommit[record.name]; seen {
					series.Points = append(series.Points, record.point)
				} else {
					points := make([]*protocol.Point, 1, 1)
					points[0] = record.point
					toCommit[record.name] = &protocol.Series{
						Name:   &record.name,
						Fields: []string{"value"},
						Points: points,
					}
				}
			} else {
				// no more input, commit whatever we have and break
				commit(toCommit)
				break CommitLoop
			}
			// if capacity reached, commit
			if pointsPending == commit_capacity {
				commit(toCommit)
				toCommit = make(map[string]*protocol.Series)
				pointsPending = 0
				timer.Reset(commit_max_wait)
			}
		case <-timer.C:
			commit(toCommit)
			toCommit = make(map[string]*protocol.Series)
			pointsPending = 0
			timer.Reset(commit_max_wait)
		}
	}
}

// holds a point to be added into series by Name
type graphiteRecord struct {
	name  string
	point *protocol.Point
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
	buf, err := reader.ReadBytes('\n')
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
	v, err = strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return nil, err
	}

	// Determine if value is a float or an int.
	if i := int64(v); float64(i) == v {
		m.integerValue, m.isInt = int64(v), true
	} else {
		m.floatValue = floatValue
	}

	// Parse timestamp.
	timestamp, err := strconv.ParseUint(fields[2], 10, 32)
	if err != nil {
		return err
	}
	m.timestamp = int64(timestamp) * int64(time.Millisecond)

	return m, nil
}
