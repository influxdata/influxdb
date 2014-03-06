// package Graphite provides a tcp listener that you can use to ingest metrics into influxdb
// via the graphite protocol.
// it behaves as a carbon daemon, with these exceptions:
// * no rounding of timestamps to the nearest interval.  Upon ingestion of multiple datapoints
//   for a given key within the same interval, graphite will use the latest value.
//   We store everything we receive so it's up to the user to feed the data in proper intervals
//   and use round intervals if you plan to rely on that.
package graphite

import (
	"bufio"
	"cluster"
	log "code.google.com/p/log4go"
	. "common"
	"coordinator"
	"io"
	"net"
	"protocol"
	"strconv"
	"strings"
	"time"
)

// TODO log prefixes so that you know the lines originate from this package?

type Server struct {
	listen_addr   string
	database      string
	coordinator   coordinator.Coordinator
	clusterConfig *cluster.ClusterConfiguration
	conn          net.Listener
	user          *cluster.ClusterAdmin
	shutdown      chan bool
}

type GraphiteListener interface {
	Close()
	getAuth()
	ListenAndServe()
	writePoints(protocol.Series) error
}

// TODO: check that database exists and create it if not
func NewServer(listen_addr, database string, coord coordinator.Coordinator, clusterConfig *cluster.ClusterConfiguration) *Server {
	self := &Server{}
	self.listen_addr = listen_addr
	self.database = database
	self.coordinator = coord
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
	if self.listen_addr != "" {
		self.conn, err = net.Listen("tcp", self.listen_addr)
		if err != nil {
			log.Error("Listen: ", err)
		}
	}
	self.Serve(self.conn)
}

func (self *Server) Serve(listener net.Listener) {
	// not really sure of the use of this shutdown channel,
	// as all handling is done through goroutines. maybe we should use a waitgroup
	defer func() { self.shutdown <- true }()

	for {
		conn_in, err := listener.Accept()
		if err != nil {
			log.Error("Accept: ", err)
			continue
		}
		go self.handleClient(conn_in)
	}
}

func (self *Server) Close() {
	if self.conn != nil {
		log.Info("Closing graphite server")
		self.conn.Close()
		log.Info("Waiting for all graphite requests to finish before killing the process")
		select {
		case <-time.After(time.Second * 5):
			log.Error("There seems to be a hanging graphite request. Closing anyway")
		case <-self.shutdown:
		}
	}
}

func (self *Server) writePoints(series *protocol.Series) error {
	err := self.coordinator.WriteSeriesData(self.user, self.database, series)
	if err != nil {
		switch err.(type) {
		case AuthorizationError:
			// user information got stale, get a fresh one (this should happen rarely)
			self.getAuth()
			err = self.coordinator.WriteSeriesData(self.user, self.database, series)
			if err != nil {
				log.Warn("failed to write series after getting new auth: %s\n", err.Error())
			}
		default:
			log.Warn("failed write series: %s\n", err.Error())
		}
	}
	return err
}

func (self *Server) handleClient(conn_in net.Conn) {
	defer conn_in.Close()
	reader := bufio.NewReader(conn_in)
	for {
		buf, err := reader.ReadBytes('\n')
		if err != nil {
			str := strings.TrimSpace(string(buf))
			if err != io.EOF {
				log.Warn("connection closed uncleanly/broken: %s\n", err.Error())
			}
			if len(str) > 0 {
				log.Warn("incomplete read, line read: '%s'. neglecting line because connection closed because of %s\n", str, err.Error())
			}
			return
		}
		str := strings.TrimSpace(string(buf))
		elements := strings.Split(str, " ")
		if len(elements) != 3 {
			continue // invalid line
		}
		val, err := strconv.ParseFloat(elements[1], 64)
		if err != nil {
			continue // invalid line
		}
		timestamp, err := strconv.ParseUint(elements[2], 10, 32)
		if err != nil {
			continue // invalid line
		}
		values := []*protocol.FieldValue{}
		if i := int64(val); float64(i) == val {
			values = append(values, &protocol.FieldValue{Int64Value: &i})
		} else {
			values = append(values, &protocol.FieldValue{DoubleValue: &val})
		}
		ts := int64(timestamp * 1000000)
		sn := uint64(1) // use same SN makes sure that we'll only keep the latest value for a given metric_id-timestamp pair
		point := &protocol.Point{
			Timestamp:      &ts,
			Values:         values,
			SequenceNumber: &sn,
		}
		series := &protocol.Series{
			Name:   &elements[0],
			Fields: []string{"value"},
			Points: []*protocol.Point{point},
		}
		// little inefficient for now, later we might want to add multiple series in 1 writePoints request
		self.writePoints(series)
	}
}
