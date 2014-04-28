package udp

import (
	"cluster"
	. "common"
	"configuration"
	"coordinator"
	"encoding/json"
	"net"
	"protocol"

	log "code.google.com/p/log4go"
)

type Server struct {
	listenAddress string
	database      string
	coordinator   coordinator.Coordinator
	clusterConfig *cluster.ClusterConfiguration
	conn          *net.UDPConn
	user          *cluster.ClusterAdmin
	shutdown      chan bool
}

func NewServer(config *configuration.Configuration, coord coordinator.Coordinator, clusterConfig *cluster.ClusterConfiguration) *Server {
	self := &Server{}

	self.listenAddress = config.UdpInputPortString()
	self.database = config.UdpInputDatabase
	self.coordinator = coord
	self.shutdown = make(chan bool, 1)
	self.clusterConfig = clusterConfig

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

	addr, err := net.ResolveUDPAddr("udp4", self.listenAddress)
	if err != nil {
		log.Error("UDPServer: ResolveUDPAddr: ", err)
		return
	}

	if self.listenAddress != "" {
		self.conn, err = net.ListenUDP("udp", addr)
		if err != nil {
			log.Error("UDPServer: Listen: ", err)
			return
		}
	}
	defer self.conn.Close()
	self.HandleSocket(self.conn)
}

func (self *Server) HandleSocket(socket *net.UDPConn) {
	buffer := make([]byte, 2048)

	for {
		n, _, err := socket.ReadFromUDP(buffer)
		if err != nil || n == 0 {
			log.Error("UDP ReadFromUDP error: %s", err)
			continue
		}

		serializedSeries := []*SerializedSeries{}
		err = json.Unmarshal(buffer[0:n], &serializedSeries)
		if err != nil {
			log.Error("UDP json error: %s", err)
			continue
		}

		for _, s := range serializedSeries {
			if len(s.Points) == 0 {
				continue
			}

			series, err := ConvertToDataStoreSeries(s, SecondPrecision)
			if err != nil {
				log.Error("UDP cannot convert received data: %s", err)
				continue
			}

			serie := []*protocol.Series{series}
			err = self.coordinator.WriteSeriesData(self.user, self.database, serie)
			if err != nil {
				log.Error("UDP cannot write data: %s", err)
				continue
			}
		}

	}

}
