// package Graphite provides a tcp listener that you can use to ingest metrics into influxdb
// via the graphite protocol.
// it behaves as a carbon daemon, with these exceptions:
// * no rounding of timestamps to the nearest interval.  Upon ingestion of multiple datapoints
//   for a given key within the same interval, graphite will use the latest value.
//   We store everything we receive so it's up to the user to feed the data in proper intervals
// TODO: what happens if you send a new value for a given metric key with an already-seen timestamp?
// that should be a replace (as in graphite), will we store 2 values? maybe we should do a delete first?
package graphite

import (
	log "code.google.com/p/log4go"
	"coordinator"
	. "common"
    "cluster"
	"net"
	"strconv"
	"strings"
	"time"
    "protocol"
    "io"
    "bufio"
)

// TODO log prefixes so that you know the lines originate from this package?

type GraphiteServer struct {
	listen_addr string
	database    string
	coordinator coordinator.Coordinator
    clusterConfig *cluster.ClusterConfiguration
    conn        net.Listener
    user        *cluster.ClusterAdmin
    shutdown    chan bool
}

type GraphiteListener interface {
	Close()
    getAuth()
    ListenAndServe()
	writePoints(protocol.Series) error
}

func NewGraphiteServer(listen_addr, database string, coord coordinator.Coordinator, clusterConfig *cluster.ClusterConfiguration) *GraphiteServer {
	self := &GraphiteServer{}
    self.listen_addr = listen_addr
    self.database = database
    self.coordinator = coord
    self.shutdown = make(chan bool, 1)
    self.clusterConfig = clusterConfig
    self.getAuth()
	return self
}

func (self *GraphiteServer) getAuth() {
    // just use any (the first) of the list of admins.
    // can we assume there's always at least 1 ?
    names := self.clusterConfig.GetClusterAdmins()
    self.user = self.clusterConfig.GetClusterAdmin(names[0])
}


func (self *GraphiteServer) ListenAndServe() {
	var err error
	if self.listen_addr != "" {
		self.conn, err = net.Listen("tcp", self.listen_addr)
		if err != nil {
			log.Error("Listen: ", err)
		}
	}
	self.Serve(self.conn)
}

func (self *GraphiteServer) Serve(listener net.Listener) {
    // not really sure of the use of this shutdown channel,
    // as all handling is done through goroutines. maybe we should use a waitgroup
	defer func() { self.shutdown <- true }()

	for {
		conn_in, err := listener.Accept()
		if err != nil {
			log.Error("Accept: ", err)
			continue
		}
		go handleClient(conn_in)
	}
}

func (self *GraphiteServer) Close() {
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

func (self *GraphiteServer) writePoints(series *protocol.Series) error {
    err := self.coordinator.WriteSeriesData(self.user, self.database, series)
    if err != nil {
        switch t := err.(type) {
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

func handleClient(conn_in net.Conn) {
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
            continue  // invalid line
        }
        val, err := strconv.ParseFloat(elements[1], 64)
        if err != nil {
            continue  // invalid line
        }
        timestamp, err := strconv.ParseUint(elements[2], 10, 32)
        if err != nil {
            continue  // invalid line
        }
        // this doesn't work yet.  should i use the types in protocol/types.go ?
        // they don't seem to match with what's in protocol.pb.go
        // and the latter seems a little lowlevel and has attributes that don't apply in this context
        // also, not sure if i still have to reserve a column for time or sequence number, or whether
        // that will be handled automatically for me
        point := &protocol.Point{int64(timestamp), [1]interface{}{val}}
        series := &protocol.Series{
            &elements[0],
            [2]*string{"time", "value"},
            [2]*protocol.ColumnType{protocol.IntType, protocol.FloatType},
            [1]*protocol.Point{point},
        }
        // little inefficient for now, later we might want to add multiple series in 1 writePoints request
        self.writePoints(series)
	}
}
