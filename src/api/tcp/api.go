package tcp

import (
	"cluster"
	. "common"
	"configuration"
	"coordinator"
	"net"
	"fmt"

	api "api/http"
	log "code.google.com/p/log4go"
	"os"

	"time"
	"code.google.com/p/goprotobuf/proto"
)

const KILOBYTE = 1024
const MEGABYTE = 1024 * KILOBYTE
const MAX_REQUEST_SIZE = MEGABYTE * 2

type Server struct {
	listenAddress string
	listenSocket string
	database      string
	Coordinator   coordinator.Coordinator
	UserManager   api.UserManager
	clusterConfig *cluster.ClusterConfiguration
	shutdown      chan bool
	RequestHandler *RequestHandler
}

func NewServer(config *configuration.Configuration, coord coordinator.Coordinator, um api.UserManager, clusterConfig *cluster.ClusterConfiguration) *Server {
	self := &Server{}

	self.listenAddress = config.TcpInputPortString()
	self.listenSocket = config.TcpInputSocketString()
	self.Coordinator = coord
	self.UserManager = um
	self.shutdown = make(chan bool, 1)
	self.clusterConfig = clusterConfig
	self.RequestHandler = &RequestHandler{
		Server: self,
	}

	return self
}

func (self *Server) SendErrorMessage(conn *Connection, t Command_CommandType, message string) error {
	result := Command_FAIL
	response := &Command{
		Type: &t,
		Result: &result,
		Reason: []byte(message),
	}

	conn.WriteRequest(response)
	return nil
}

func (self *Server) handleRequest(conn *Connection) error {
	err := self.RequestHandler.HandleRequest(conn)
	conn.IncrementSequence()
	return err
}

func getVersion() []byte {
	// TODO: how do I get current InfluxDB version
	version := fmt.Sprintf("InfluxDB v%s (git: %s)", "dev", "0000")
	return []byte(version)
}

func (self *Server) authenticate(conn *Connection, rhelo *Greeting) error {
	var u User
	var e error
	var t Account_AccountType

	db   := string(rhelo.GetDatabase())
	user := string(rhelo.GetAccount().GetName())
	pass := string(rhelo.GetAccount().GetPassword())

	if db != "" {
		u, e = self.UserManager.AuthenticateDbUser(db, user, pass)
		if e != nil {
			u, e = self.UserManager.AuthenticateClusterAdmin(user, pass)
			if e != nil {
				return e
			}
			t = Account_CLUSTER_ADMIN
		} else {
			t = Account_DB_USER
		}
		conn.State = STATE_AUTHENTICATED
	} else {
		u, e = self.UserManager.AuthenticateClusterAdmin(user, pass)
		if e != nil {
			return e
		}

		t = Account_CLUSTER_ADMIN
		conn.State = STATE_AUTHENTICATED
	}

	conn.User = u
	conn.AccountType = t
	conn.SetSequenceFromMessage(rhelo)

	return nil
}

func (self *Server) handshake(conn *Connection) error {
	var err error

	gtype := Greeting_HELO
	req := &Greeting{
		Type: &gtype,
		ProtocolVersion: proto.Int32(1),
		Agent: getVersion(),
		Sequence: proto.Uint32(conn.Sequence),
	}
	conn.WriteRequest(req)
	conn.IncrementSequence()

	// Wait Response
	rhelo := &Greeting{}
	err = conn.ReadMessage(rhelo)
	if err != nil {
		return err
	}

	err = self.authenticate(conn, rhelo)
	if err != nil {
		return err
	}

	// Ack Response
	gtype = Greeting_ACK
	conn.WriteRequest(&Greeting{
		Type: &gtype,
		Account: &Account{
			Type: &conn.AccountType,
		},
		Sequence: proto.Uint32(conn.Sequence),
	})
	conn.IncrementSequence()

	return nil
}

func (self *Server) HandleConnection(conn *Connection) {
	log.Info("Experimental ProtobufServer: client connected: %s", conn.Address.String())

	for {
		err := self.handshake(conn)
		if err != nil || conn.State == STATE_INITIALIZED {
			gtype := Greeting_DENY
			conn.WriteRequest(&Greeting{
				Type: &gtype,
				Sequence: proto.Uint32(conn.Sequence),
			})
			log.Error("handshake Failed: ", err)
			conn.Close()
			return;
		}

		for {
			log.Debug("handle Request: %+v", *conn)

			err := self.handleRequest(conn)
			if err != nil {
				log.Debug("Error: %s", err)
				if _, ok := err.(*ConnectionError); ok {
					log.Debug("ConnectionError")
					// okay, connection was finished by client.
					return
				} else if _, ok := err.(*ConnectionResetError); ok {
					// break current loop.
					log.Debug("Reset Request")
					break;
				}

				log.Debug("Error: closing connection: %s", err)
				conn.Close()
				return
			}

			if !conn.IsAlived() {
				return
			}
		}
	}
}

func (self *Server) tcpListenAndServe() {
	var err error
	var server net.Listener

	log.Debug("TcpServer: Listen at: ",self.listenAddress)
	addr, err := net.ResolveTCPAddr("tcp4", self.listenAddress)
	if err != nil {
		log.Error("TCPServer: ResolveTCPAddr: ", err)
		return
	}

	if self.listenAddress != "" {
		server, err = net.ListenTCP("tcp", addr)
		if err != nil {
			log.Error("TCPServer: Listen: ", err)
			return
		}
	}
	self.acceptLoop(server, nil)
}

func (self *Server) acceptLoop(listener net.Listener, yield func()) {
	defer func() {
		listener.Close()
		if yield != nil {
			yield()
		}
	}()

	for {
		client, err := listener.Accept()
		if err != nil {
			log.Error("Accept Failed: ", err)
			continue
		}

		conn := NewConnection(client, func(conn *Connection, time time.Time) {
			//log.Debug("Closing Connection")
			//conn.Close()
		})

		go self.HandleConnection(conn)
	}
}


func (self *Server) unixListenAndServe() {
	var err error
	var server net.Listener

	log.Debug("UnixServer: Listen at: ",self.listenSocket)

	if self.listenSocket != "" {
		server, err = net.Listen("unix", self.listenSocket)
		if err != nil {
			log.Error("UnixServer: Listen: ", err)
			return
		}
	}
	self.acceptLoop(server, func() {
		os.Remove(self.listenSocket)
	})
}

func (self *Server) ListenAndServe() {
	if self.listenAddress != "" {
		go self.tcpListenAndServe()
	}
	if self.listenSocket != "" {
		go self.unixListenAndServe()
	}
}
