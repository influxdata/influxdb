package collectd

import (
	"math"
	"net"
	"sync"

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
	s := &Server{}

	s.listenAddress = config.CollectdBindString()
	s.database = config.CollectdDatabase
	s.coordinator = coord
	s.shutdown = make(chan bool, 1)
	s.clusterConfig = clusterConfig
	s.typesdbpath = config.CollectdTypesDB
	s.typesdb = make(collectd.Types)

	return s
}

func (s *Server) getAuth() {
	// just use any (the first) of the list of admins.
	names := s.clusterConfig.GetClusterAdmins()
	if len(names) == 0 {
		panic("Collectd Plugin: No cluster admins found - couldn't initialize.")
	}
	s.user = s.clusterConfig.GetClusterAdmin(names[0])
}

func (s *Server) ListenAndServe() {
	var err error

	s.getAuth()

	s.typesdb, err = collectd.TypesDBFile(s.typesdbpath)
	if err != nil {
		log.Error("CollectdServer: TypesDB: ", err)
		return
	}

	addr, err := net.ResolveUDPAddr("udp4", s.listenAddress)
	if err != nil {
		log.Error("CollectdServer: ResolveUDPAddr: ", err)
		return
	}

	if s.listenAddress != "" {
		s.conn, err = net.ListenUDP("udp", addr)
		if err != nil {
			log.Error("CollectdServer: Listen: ", err)
			return
		}
	}
	defer s.conn.Close()
	s.HandleSocket(s.conn)
}

func (s *Server) HandleSocket(socket *net.UDPConn) {
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

		packets, err := collectd.Packets(buffer[:n], s.typesdb)
		if err != nil {
			log.Error("Collectd parse error: %s", err)
			continue
		}

		for _, packet := range *packets {
			series := packetToSeries(&packet)
			err = s.coordinator.WriteSeriesData(s.user, s.database, series)
			if err != nil {
				log.Error("Collectd cannot write data: %s", err)
				continue
			}
		}
	}
}

func packetToSeries(p *collectd.Packet) []*protocol.Series {
	// Prefer high resolution timestamp.  TimeHR is 2^-30 seconds, so shift
	// right 30 to get seconds then convert to microseconds for InfluxDB
	uts := (p.TimeHR >> 30) * 1000 * 1000

	// Fallback on unix timestamp if high res is 0
	if uts == 0 {
		uts = p.Time * 1000 * 1000
	}

	// Collectd time is uint64 but influxdb expects int64
	ts := int64(uts % math.MaxInt64)
	if uts > math.MaxInt64 {
		logTimerWrapOnce()
	}

	series := make([]*protocol.Series, len(p.Values))

	for i, _ := range p.Values {
		metricName := p.FormatName()

		values := []*protocol.FieldValue{}
		values = append(values, &protocol.FieldValue{
			StringValue: &p.Hostname,
		})
		values = append(values, &protocol.FieldValue{
			StringValue: &p.Plugin,
		})
		values = append(values, &protocol.FieldValue{
			StringValue: &p.PluginInstance,
		})
		values = append(values, &protocol.FieldValue{
			StringValue: &p.Type,
		})
		values = append(values, &protocol.FieldValue{
			StringValue: &p.TypeInstance,
		})
		values = append(values, &protocol.FieldValue{
			StringValue: &p.Values[i].Name,
		})
		values = append(values, &protocol.FieldValue{
			StringValue: &p.Values[i].TypeName,
		})
		values = append(values, &protocol.FieldValue{
			DoubleValue: &p.Values[i].Value,
		})

		points := []*protocol.Point{
			{
				Timestamp: &ts,
				Values:    values,
			},
		}

		series[i] = &protocol.Series{
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
	}
	return series
}

var wrapOnce sync.Once

func logTimerWrapOnce() {
	wrapOnce.Do(func() {
		log.Error("Collectd timestamp too large for InfluxDB.  Wrapping around to 0.")
	})
}
