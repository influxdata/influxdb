package influxdb

import (
	"bufio"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb/api"
	"github.com/influxdb/influxdb/cluster"
	. "github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/protocol"

	log "code.google.com/p/log4go"
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
type Server struct {
	listenAddress string
	database      string
	coordinator   api.Coordinator
	clusterConfig *cluster.ClusterConfiguration
	conn          net.Listener
	udpConn       *net.UDPConn
	user          *cluster.ClusterAdmin
	writeSeries   chan Record
	allCommitted  chan bool
	connClosed    chan bool // lets us break from the Accept() loop, see http://zhen.org/blog/graceful-shutdown-of-go-net-dot-listeners/ (quit channel)
	udpEnabled    bool
	writers       sync.WaitGroup // allows us to make sure things writing to writeSeries are done (they do blocking calls to handleMessage()) whether udp or tcp
}

// holds a point to be added into series by Name
type Record struct {
	Name string
	*protocol.Point
}

// the ingest could in theory be anything from 1 series(datapoint) every few minutes, upto millions of datapoints every second.
// and there might be multiple connections, each delivering a certain (not necessarily equal) fraction of the total.
// we want ingested points to be ingested (flushed) quickly (let's say at least within 100ms), but since coordinator.WriteSeriesData
// comes with a cost, we also want to buffer up the data, the more the better (up to a limit).
// So basically we need to trade these concepts off against each other, by committing

// a batch of series every commit_max_wait ms or every commit_capacity datapoints, whichever is first.

// upto how many points/series to commit in 1 go?
// let's say buffer up to about 1MiB of data, at 24B per record -> 43690 records, let's make it an even 40k
const commit_capacity = 40000

// how long to wait max before flushing a commit payload
const commit_max_wait = 100 * time.Millisecond

// the write commit payload should get written in a timely fashion.
// if not, the channel that feeds the committer will queue up to max_queue series, and then
// block, creating backpressure to the client.  This allows to keep receiving metrics while
// a flush is ongoing, but not much more than we can actually handle.
const max_queue = 20000

// TODO: check that database exists and create it if not
func NewServer(config *configuration.Configuration, coord api.Coordinator, clusterConfig *cluster.ClusterConfiguration) *Server {
	s := &Server{}
	s.listenAddress = config.GraphitePortString()
	s.database = config.GraphiteDatabase
	s.coordinator = coord
	s.writeSeries = make(chan Record, max_queue)
	s.allCommitted = make(chan bool, 1)
	s.connClosed = make(chan bool, 1)
	s.clusterConfig = clusterConfig
	s.udpEnabled = config.GraphiteUdpEnabled
	return s
}

// getAuth assures that the user property is a user with access to the graphite database
// only call this function after everything (i.e. Raft) is initialized, so that there's at least 1 admin user
func (s *Server) getAuth() {
	// just use any (the first) of the list of admins.
	names := s.clusterConfig.GetClusterAdmins()
	s.user = s.clusterConfig.GetClusterAdmin(names[0])
}

func (s *Server) ListenAndServe() {
	s.getAuth()
	var err error
	if s.listenAddress != "" {
		s.conn, err = net.Listen("tcp", s.listenAddress)
		if err != nil {
			log.Error("GraphiteServer: Listen: ", err)
			return
		}
	}
	if s.udpEnabled {
		udpAddress, _ := net.ResolveUDPAddr("udp", s.listenAddress)
		s.udpConn, _ = net.ListenUDP("udp", udpAddress)
		go s.ServeUdp(s.udpConn)
	}
	go s.committer()
	s.Serve(s.conn)
}

func (s *Server) Serve(listener net.Listener) {
	for {
		conn_in, err := listener.Accept()
		if err != nil {
			log.Error("GraphiteServer: Accept: ", err)
			select {
			case <-s.connClosed:
				break
			default:
			}
			continue
		}
		s.writers.Add(1)
		go s.handleClient(conn_in)
	}
	s.writers.Wait()
	close(s.writeSeries)
}

func (s *Server) ServeUdp(conn *net.UDPConn) {
	var buf []byte = make([]byte, 65536)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Warn("GraphiteServer: Error when reading from UDP connection %s", err.Error())
		}
		s.writers.Add(1)
		go s.handleUdpMessage(string(buf[:n]))
	}
}

func (s *Server) handleUdpMessage(msg string) {
	defer s.writers.Done()
	metrics := strings.Split(msg, "\n")
	for _, metric := range metrics {
		reader := bufio.NewReader(strings.NewReader(metric + "\n"))
		s.handleMessage(reader)
	}
}

func (s *Server) Close() {
	if s.udpConn != nil {
		log.Info("GraphiteServer: Closing graphite UDP listener")
		s.udpConn.Close()
	}
	if s.conn != nil {
		log.Info("GraphiteServer: Closing graphite server")
		close(s.connClosed)
		s.conn.Close()
		log.Info("GraphiteServer: Waiting for all graphite requests to finish before killing the process")
		select {
		case <-time.After(time.Second * 5):
			log.Error("GraphiteServer: There seems to be a hanging graphite request or data flush. Closing anyway")
		case <-s.allCommitted:
			log.Info("GraphiteServer shut down cleanly")
		}
	}
}

func (s *Server) committer() {
	defer func() { s.allCommitted <- true }()

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
		case record, ok := <-s.writeSeries:
			if ok {
				pointsPending += 1
				if series, seen := toCommit[record.Name]; seen {
					series.Points = append(series.Points, record.Point)
				} else {
					points := make([]*protocol.Point, 1, 1)
					points[0] = record.Point
					toCommit[record.Name] = &protocol.Series{
						Name:   &record.Name,
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

func (s *Server) handleClient(conn net.Conn) {
	defer conn.Close()
	defer s.writers.Done()
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

func (s *Server) handleMessage(reader *bufio.Reader) (err error) {
	graphiteMetric := &GraphiteMetric{}
	err = graphiteMetric.Read(reader)
	if err != nil {
		return
	}
	values := []*protocol.FieldValue{}
	if graphiteMetric.isInt {
		values = append(values, &protocol.FieldValue{Int64Value: &graphiteMetric.integerValue})
	} else {
		values = append(values, &protocol.FieldValue{DoubleValue: &graphiteMetric.floatValue})
	}
	sn := uint64(1) // use same SN makes sure that we'll only keep the latest value for a given metric_id-timestamp pair
	point := &protocol.Point{
		Timestamp:      &graphiteMetric.timestamp,
		Values:         values,
		SequenceNumber: &sn,
	}
	record := Record{graphiteMetric.name, point}
	s.writeSeries <- record
	return
}

type GraphiteMetric struct {
	name         string
	isInt        bool
	integerValue int64
	floatValue   float64
	timestamp    int64
}

// returns err == io.EOF when we hit EOF without any further data
func (m *GraphiteMetric) Read(reader *bufio.Reader) error {
	buf, err := reader.ReadBytes('\n')
	str := strings.TrimSpace(string(buf))
	if err != nil {
		if err != io.EOF {
			return fmt.Errorf("connection closed uncleanly/broken: %s\n", err.Error())
		}
		if str == "" {
			return err
		}
		// else we got EOF but also data, so just try to process it as valid data
	}
	elements := strings.Fields(str)
	if len(elements) != 3 {
		return fmt.Errorf("Received '%s' which doesn't have three fields", str)
	}
	m.name = elements[0]
	m.floatValue, err = strconv.ParseFloat(elements[1], 64)
	if err != nil {
		return err
	}
	if i := int64(m.floatValue); float64(i) == m.floatValue {
		m.isInt = true
		m.integerValue = int64(m.floatValue)
	}
	timestamp, err := strconv.ParseUint(elements[2], 10, 32)
	if err != nil {
		return err
	}
	m.timestamp = int64(timestamp * 1000000)
	return nil
}
