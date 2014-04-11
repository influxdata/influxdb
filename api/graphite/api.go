// package Graphite provides a tcp and/or udp listener that you can
// use to ingest metrics into influxdb via the graphite protocol.  it
// behaves as a carbon daemon, except:

// no rounding of timestamps to the nearest interval.  Upon ingestion
// of multiple datapoints for a given key within the same interval
// (possibly but not necessarily the same timestamp), graphite would
// use one (the latest received) value with a rounded timestamp
// representing that interval.  We store values for every timestamp we
// receive (only the latest value for a given metric-timestamp pair)
// so it's up to the user to feed the data in proper intervals (and
// use round intervals if you plan to rely on that)
package graphite

import (
	"bufio"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb/cluster"
	. "github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/coordinator"
	"github.com/influxdb/influxdb/protocol"

	log "code.google.com/p/log4go"
)

type Server struct {
	listenAddress string
	database      string
	coordinator   coordinator.Coordinator
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
func NewServer(config *configuration.Configuration, coord coordinator.Coordinator, clusterConfig *cluster.ClusterConfiguration) *Server {
	self := &Server{}
	self.listenAddress = config.GraphitePortString()
	self.database = config.GraphiteDatabase
	self.coordinator = coord
	self.writeSeries = make(chan Record, max_queue)
	self.allCommitted = make(chan bool, 1)
	self.connClosed = make(chan bool, 1)
	self.clusterConfig = clusterConfig
	self.udpEnabled = config.GraphiteUdpEnabled

	return self
}

// getAuth assures that the user property is a user with access to the graphite database
// only call this function after everything (i.e. Raft) is initialized, so that there's at least 1 admin user
func (self *Server) getAuth() {
	// just use any (the first) of the list of admins.
	names := self.clusterConfig.GetClusterAdmins()
	self.user = self.clusterConfig.GetClusterAdmin(names[0])
}

func (self *Server) ListenAndServe() {
	self.getAuth()
	var err error
	if self.listenAddress != "" {
		self.conn, err = net.Listen("tcp", self.listenAddress)
		if err != nil {
			log.Error("GraphiteServer: Listen: ", err)
			return
		}
	}
	if self.udpEnabled {
		udpAddress, _ := net.ResolveUDPAddr("udp", self.listenAddress)
		self.udpConn, _ = net.ListenUDP("udp", udpAddress)
		go self.ServeUdp(self.udpConn)
	}
	go self.committer()
	self.Serve(self.conn)
}

func (self *Server) Serve(listener net.Listener) {
	for {
		conn_in, err := listener.Accept()
		if err != nil {
			log.Error("GraphiteServer: Accept: ", err)
			select {
			case <-self.connClosed:
				break
			default:
			}
			continue
		}
		self.writers.Add(1)
		go self.handleClient(conn_in)
	}
	self.writers.Wait()
	close(self.writeSeries)
}

func (self *Server) ServeUdp(conn *net.UDPConn) {
	var buf []byte = make([]byte, 65536)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Warn("GraphiteServer: Error when reading from UDP connection %s", err.Error())
		}
		self.writers.Add(1)
		go self.handleUdpMessage(string(buf[:n]))
	}
}

func (self *Server) handleUdpMessage(msg string) {
	defer self.writers.Done()
	metrics := strings.Split(msg, "\n")
	for _, metric := range metrics {
		reader := bufio.NewReader(strings.NewReader(metric + "\n"))
		self.handleMessage(reader)
	}
}

func (self *Server) Close() {
	if self.udpConn != nil {
		log.Info("GraphiteServer: Closing graphite UDP listener")
		self.udpConn.Close()
	}
	if self.conn != nil {
		log.Info("GraphiteServer: Closing graphite server")
		close(self.connClosed)
		self.conn.Close()
		log.Info("GraphiteServer: Waiting for all graphite requests to finish before killing the process")
		select {
		case <-time.After(time.Second * 5):
			log.Error("GraphiteServer: There seems to be a hanging graphite request or data flush. Closing anyway")
		case <-self.allCommitted:
			log.Info("GraphiteServer shut down cleanly")
		}
	}
}

func (self *Server) committer() {
	defer func() { self.allCommitted <- true }()

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
		err := self.coordinator.WriteSeriesData(self.user, self.database, commitPayload)
		if err != nil {
			switch err.(type) {
			case AuthorizationError:
				// user information got stale, get a fresh one (this should happen rarely)
				self.getAuth()
				err = self.coordinator.WriteSeriesData(self.user, self.database, commitPayload)
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
		case record, ok := <-self.writeSeries:
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

func (self *Server) handleClient(conn net.Conn) {
	defer conn.Close()
	defer self.writers.Done()
	reader := bufio.NewReader(conn)
	for {
		err := self.handleMessage(reader)
		if err != nil {
			if io.EOF == err {
				log.Debug("GraphiteServer: Client closed graphite connection")
				return
			}
			log.Error("GraphiteServer:", err)
		}
	}
}

func (self *Server) handleMessage(reader *bufio.Reader) (err error) {
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
	self.writeSeries <- record
	return
}
