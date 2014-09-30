package collectd

import (
	"net"
	"strconv"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/api"
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/protocol"

	collectd "github.com/kimor79/gollectd"
)

type Server struct {
	listenAddress string
	database      string
	coordinator   api.Coordinator
	clusterConfig *cluster.ClusterConfiguration
	conn          *net.UDPConn
	shutdown      chan bool
	user          *cluster.ClusterAdmin
	typesdb       collectd.Types
	typesdbpath   string
}

func NewServer(config *configuration.Configuration, coord api.Coordinator, clusterConfig *cluster.ClusterConfiguration) *Server {
	self := &Server{}

	self.listenAddress = config.CollectdPortString()
	self.database = config.CollectdDatabase
	self.coordinator = coord
	self.shutdown = make(chan bool, 1)
	self.clusterConfig = clusterConfig
	self.typesdbpath = config.CollectdTypesDB
	self.typesdb = make(collectd.Types)

	return self
}

func (self *Server) getAuth() {
	// just use any (the first) of the list of admins.
	names := self.clusterConfig.GetClusterAdmins()
	self.user = self.clusterConfig.GetClusterAdmin(names[0])
}

func (self *Server) ListenAndServe() {
	var err error

	self.getAuth()

	self.typesdb, err = collectd.TypesDB(self.typesdbpath)
	if err != nil {
		log.Error("CollectdServer: TypesDB: ", err)
		return
	}

	addr, err := net.ResolveUDPAddr("udp4", self.listenAddress)
	if err != nil {
		log.Error("CollectdServer: ResolveUDPAddr: ", err)
		return
	}

	if self.listenAddress != "" {
		self.conn, err = net.ListenUDP("udp", addr)
		if err != nil {
			log.Error("CollectdServer: Listen: ", err)
			return
		}
	}
	defer self.conn.Close()
	self.HandleSocket(self.conn)
}

func (self *Server) HandleSocket(socket *net.UDPConn) {
	// From https://collectd.org/wiki/index.php/Binary_protocol
	//   1024 bytes (payload only, not including UDP / IP headers)
	//   In versions 4.0 through 4.7, the receive buffer has a fixed size
	//   of 1024 bytes. When longer packets are received, the trailing data
	//   is simply ignored. Since version 4.8, the buffer size can be
	//   configured. Version 5.0 will increase the default buffer size to
	//   1452 bytes (the maximum payload size when using UDP/IPv6 over
	//   Ethernet).
	buffer := make([]byte, 1452)

	for {
		n, _, err := socket.ReadFromUDP(buffer)
		if err != nil || n == 0 {
			log.Error("Collectd ReadFromUDP error: %s", err)
			continue
		}

		packets, err := collectd.Packets(buffer[:n], self.typesdb)
		if err != nil {
			log.Error("Collectd parse error: %s", err)
			continue
		}

		for _, packet := range *packets {
			// TimeHR is 2^-30 seconds, influx expects milliseconds
			uts := (packet.TimeHR >> 30) * 1000
			// TimeHR is also uint64 but influx expects int64
			sts := strconv.FormatUint(uts, 10)
			ts, _ := strconv.ParseInt(sts, 10, 64)

			for _, dataSet := range packet.Values {
				metricName := packet.FormatName()

				values := []*protocol.FieldValue{}
				values = append(values, &protocol.FieldValue{
					StringValue: &packet.Hostname,
				})
				values = append(values, &protocol.FieldValue{
					StringValue: &packet.Plugin,
				})
				values = append(values, &protocol.FieldValue{
					StringValue: &packet.PluginInstance,
				})
				values = append(values, &protocol.FieldValue{
					StringValue: &packet.Type,
				})
				values = append(values, &protocol.FieldValue{
					StringValue: &packet.TypeInstance,
				})
				values = append(values, &protocol.FieldValue{
					StringValue: &dataSet.Name,
				})
				values = append(values, &protocol.FieldValue{
					StringValue: &dataSet.TypeName,
				})
				values = append(values, &protocol.FieldValue{
					DoubleValue: &dataSet.Value,
				})

				points := make([]*protocol.Point, 1, 1)
				points[0] = &protocol.Point{
					Timestamp: &ts,
					Values:    values,
				}

				series := make([]*protocol.Series, 1, 1)
				series[0] = &protocol.Series{
					Name: &metricName,
					Fields: []string{
						"host",
						"plugin",
						"plugin_instance",
						"type",
						"type_instance",
						"dsname",
						"dstype",
						"value",
					},
					Points: points,
				}

				err = self.coordinator.WriteSeriesData(self.user, self.database, series)
				if err != nil {
					log.Error("Collectd cannot write data: %s", err)
					continue
				}
			}
		}
	}
}
