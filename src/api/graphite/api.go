// package Graphite provides a tcp listener that you can use to ingest metrics into influxdb
// via the graphite protocol.
// it behaves as a carbon daemon, except:

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
	"cluster"
	. "common"
	"configuration"
	"coordinator"
	"net"
	"protocol"
	"sync"
	"time"

	log "code.google.com/p/log4go"
)

type Server struct {
	listenAddress string
	database      string
	coordinator   coordinator.Coordinator
	clusterConfig *cluster.ClusterConfiguration
	conn          net.Listener
	user          *cluster.ClusterAdmin
	writeSeries   chan protocol.Series
	shutdown      chan bool
}

// will commit a batch of series every commit_max_wait ms or every commit_capacity datapoints,
// whichever is first
// upto how many points/series to commit in 1 go?
const commit_capacity = 40000

// how long to wait max before flushing a commit payload
// basically trade off the efficiency of a high commit_capacity with the expectation
// to get your metrics stored in a short timeframe.
const commit_max_wait = 100 * time.Millisecond

// the write commit payload should get written in a timely fashion.
// if not, the channel that feeds the committer will queue up to max_queue series, and then
// block, creating backpressure to the client.
const max_queue = 20000

// TODO: check that database exists and create it if not
func NewServer(config *configuration.Configuration, coord coordinator.Coordinator, clusterConfig *cluster.ClusterConfiguration) *Server {
	self := &Server{}
	self.listenAddress = config.GraphitePortString()
	self.database = config.GraphiteDatabase
	self.coordinator = coord
	self.writeSeries = make(chan protocol.Series, max_queue)
	self.shutdown = make(chan bool, 1)
	self.clusterConfig = clusterConfig
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
	go self.committer()
	self.Serve(self.conn)
}

func (self *Server) Serve(listener net.Listener) {
	var wg sync.WaitGroup

	for {
		conn_in, err := listener.Accept()
		if err != nil {
			log.Error("GraphiteServer: Accept: ", err)
			continue
		}
		wg.Add(1)
		go self.handleClient(conn_in, wg)
	}
	wg.Wait()
	close(self.writeSeries)
}

func (self *Server) Close() {
	if self.conn != nil {
		log.Info("GraphiteServer: Closing graphite server")
		self.conn.Close()
		log.Info("GraphiteServer: Waiting for all graphite requests to finish before killing the process")
		select {
		case <-time.After(time.Second * 5):
			log.Error("GraphiteServer: There seems to be a hanging graphite request. Closing anyway")
		case <-self.shutdown:
		}
	}
}

func (self *Server) committer() {
	defer func() { self.shutdown <- true }()

	// the ingest could in theory be anything from 1 series(datapoint) every few minutes, upto millions of datapoints every second.
	// and there might be multiple connections, each delivering a certain (not necessarily equal) fraction of the total.
	// we want ingested points to be ingested quickly (let's say at least within 100ms), but since coordinator.WriteSeriesData
	// has a bunch of overhead, we also want to buffer up the data, let's say
	// up to about 1MiB of data, at 24B per record -> 43690 records, let's make it an even 40k

	commit := func(commit_payload []*protocol.Series) {
		err := self.coordinator.WriteSeriesData(self.user, self.database, commit_payload)
		if err != nil {
			switch err.(type) {
			case AuthorizationError:
				// user information got stale, get a fresh one (this should happen rarely)
				self.getAuth()
				err = self.coordinator.WriteSeriesData(self.user, self.database, commit_payload)
				if err != nil {
					log.Warn("GraphiteServer: failed to write series after getting new auth: %s\n", err.Error())
				}
			default:
				log.Warn("GraphiteServer: failed write series: %s\n", err.Error())
			}
		}
	}
	commit_payload := make([]*protocol.Series, 0, commit_capacity)
	timer := time.NewTimer(commit_max_wait)

CommitLoop:
	for {
		select {
		case serie, ok := <-self.writeSeries:
			if len(commit_payload) == commit_capacity {
				commit(commit_payload)
				commit_payload = make([]*protocol.Series, 0, commit_capacity)
				timer.Reset(commit_max_wait)
				if !ok {
					break CommitLoop
				}
			} else {
				commit_payload = append(commit_payload, &serie)
				if !ok {
					commit(commit_payload)
					break CommitLoop
				}
			}
		case <-timer.C:
			commit(commit_payload)
			commit_payload = make([]*protocol.Series, 0, commit_capacity)
		}
	}
}

func (self *Server) handleClient(conn net.Conn, wg sync.WaitGroup) {
	defer conn.Close()
	defer wg.Done()
	reader := bufio.NewReader(conn)
	for {
		graphiteMetric := &GraphiteMetric{}
		err := graphiteMetric.Read(reader)
		if err != nil {
			log.Error(err)
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
		series := protocol.Series{
			Name:   &graphiteMetric.name,
			Fields: []string{"value"},
			Points: []*protocol.Point{point},
		}
		self.writeSeries <- series
	}
}
