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
	"errors"

	"crypto/rand"
	"crypto/tls"
)

const KILOBYTE = 1024
const MEGABYTE = 1024 * KILOBYTE
const MAX_REQUEST_SIZE = MEGABYTE * 2

type HandshakeState int32

const (
	HandshakeState_INITIALIZED HandshakeState = 0
	HandshakeState_WAIT_STARTUP_RESPONSE HandshakeState = 1
	HandshakeState_AUTHENTICATION_REQUEST HandshakeState = 2
	HandshakeState_WAIT_AUTHENTICATION_RESPONSE HandshakeState = 3
	HandshakeState_PROCESS_READY HandshakeState = 4
	HandshakeState_SEND_AUTHENTICATION HandshakeState = 5
	HandshakeState_FINISHED HandshakeState = 6
	HandshakeState_UPGRADE_SSL HandshakeState = 7
	HandshakeState_WAIT_STARTUP_REQUEST = 8
	HandshakeState_RECV_AUTHENTICATION = 9
	HandshakeState_ERROR HandshakeState = 999
)

var G_STARTUP_MESSAGE = Greeting_STARTUP_MESSAGE
var G_AUTHENTICATION = Greeting_AUTHENTICATION
var G_AUTHENTICATION_OK = Greeting_AUTHENTICATION_OK
var G_Configuration_PLAIN = Greeting_Configuration_PLAIN
var G_Authentication_CLEARTEXT_PASSWORD = Greeting_Authentication_CLEARTEXT_PASSWORD
var G_MESSAGE_OPTION = Greeting_MESSAGE_OPTION
var G_SSL_UPGRADE = Greeting_SSL_UPGRADE
var G_Configuration_REQUIRED = Greeting_Configuration_REQUIRED
var G_COMMAND_READY = Greeting_COMMAND_READY
var G_STARTUP_RESPONSE = Greeting_STARTUP_RESPONSE
var G_CONFIGURATION_PLAIN = Greeting_Configuration_PLAIN
var G_CONFIGURATION_NONE = Greeting_Configuration_NONE
var G_AUTHENTICATION_CLEARTEXT_PASSWORD = Greeting_Authentication_CLEARTEXT_PASSWORD
var G_ERROR = Greeting_ERROR

// Commands
var C_LISTDATABASE = Command_LISTDATABASE
var C_WRITESERIES = Command_WRITESERIES
var C_QUERY = Command_QUERY
var C_CLOSE = Command_CLOSE
var C_PING = Command_PING
var C_CREATEDATABASE = Command_CREATEDATABASE
var C_DROPDATABASE = Command_DROPDATABASE

type Server struct {
	listenAddress string
	listenSocket string
	database      string
	Coordinator   coordinator.Coordinator
	UserManager   api.UserManager
	clusterConfig *cluster.ClusterConfiguration
	shutdown      chan bool
	RequestHandler *RequestHandler
	tlsConfig *tls.Config
}

func NewServer(config *configuration.Configuration, coord coordinator.Coordinator, um api.UserManager, clusterConfig *cluster.ClusterConfiguration) *Server {
	self := &Server{}

	self.listenAddress = config.TcpInputPortString()
	self.listenSocket = config.TcpInputSocketString()

	if config.TcpInputUseSSL {
		cert, err := tls.LoadX509KeyPair(config.TcpInputSSLCert(), config.TcpInputSSLKey())
		if err != nil {
			log.Error("tcp server: loadkeys failed. disable ssl feature: %s", err)
		} else {
			tslConfig := &tls.Config{Certificates: []tls.Certificate{cert}}
			tslConfig.Rand = rand.Reader

			self.tlsConfig = tslConfig
			log.Debug("SSL Config loaded")
		}
	}

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
	v := Error_PERMISSION_DENIED
	response := &Command{
		Type: &t,
		Result: &result,
		Error: &Error{
			Code: &v,
			Reason: []byte(message),
		},
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

func (self *Server) authenticate(conn *Connection, info *Greeting_Authentication) error {
	var u User
	var e error
	var t Greeting_Authentication_AccountType

	db   := string(info.GetDatabase())
	user := string(info.GetName())
	pass := string(info.GetPassword())

	if db != "" {
		u, e = self.UserManager.AuthenticateDbUser(db, user, pass)
		if e != nil {
			u, e = self.UserManager.AuthenticateClusterAdmin(user, pass)
			if e != nil {
				return e
			}
			t = Greeting_Authentication_CLUSTER_ADMIN
		} else {
			t = Greeting_Authentication_DB_USER
		}
		conn.State = STATE_AUTHENTICATED
	} else {
		u, e = self.UserManager.AuthenticateClusterAdmin(user, pass)
		if e != nil {
			return e
		}

		t = Greeting_Authentication_CLUSTER_ADMIN
		conn.State = STATE_AUTHENTICATED
	}

	conn.User = u
	conn.AccountType = t
	conn.Database = db

	return nil
}

func (self *Server) beginSSLHandshake(conn *Connection) error {
	var tlsConn *tls.Conn

	tlsConn = tls.Server(conn.Socket, self.tlsConfig)
	// NOTE(chobie): Assume ssl handshake.
	if err := tlsConn.Handshake(); err != nil {
		log.Debug("SSL HANDSHAKE FAILED: %+v", err)
		return err
	}

	conn.Socket = tlsConn
	return nil
}

func (self *Server) handshake(conn *Connection) error {
	var err error
	var state HandshakeState
	auth := &Greeting_Authentication{}

	for {
		switch (state) {
		case HandshakeState_INITIALIZED:
			// Initialize here
			state = HandshakeState_WAIT_STARTUP_REQUEST
			break
		case HandshakeState_WAIT_STARTUP_REQUEST:
			// waiting startup request
			startup := &Greeting{}
			err = conn.ReadMessage(startup)
			if err != nil {
				return err
			}
			if startup.GetType() != Greeting_STARTUP_MESSAGE {
				return errors.New("Illegal handshake")
			}

			database := startup.GetAuthentication().GetDatabase()
			name     := startup.GetAuthentication().GetName()
			auth.Database = database
			auth.Name = name

			// TODO(chobie): CHECK SSL OPTION
			ssl := G_CONFIGURATION_NONE
			req := &Greeting{
				Type: &G_STARTUP_RESPONSE,
				ProtocolVersion: proto.Int32(1),
				Agent: getVersion(),
				Sequence: proto.Uint32(conn.Sequence),
				Authentication: &Greeting_Authentication{
					Method: &G_AUTHENTICATION_CLEARTEXT_PASSWORD,
				},
				Config: &Greeting_Configuration{
					CompressType: &G_CONFIGURATION_PLAIN,
					Ssl: &ssl,
				},
			}
			conn.WriteRequest(req)
			conn.IncrementSequence()

			if ssl == Greeting_Configuration_REQUIRED {
				state = HandshakeState_UPGRADE_SSL
			} else {
				state = HandshakeState_RECV_AUTHENTICATION
			}
			break
		case HandshakeState_UPGRADE_SSL:
			message := &Greeting{}
			err = conn.ReadMessage(message)
			if err != nil {
				return err
			}
			if message.GetType() != G_SSL_UPGRADE {
				return errors.New("Illegal handshake")
			}

			conn.Buffer.ReadBuffer.Reset()
			conn.Buffer.WriteBuffer.Reset()
			if e := self.beginSSLHandshake(conn); e != nil {
				return e
			}
			state = HandshakeState_RECV_AUTHENTICATION
			break
		case HandshakeState_RECV_AUTHENTICATION:
			// waiting request
			message := &Greeting{}
			err = conn.ReadMessage(message)
			if err != nil {
				return err
			}
			auth.Password = message.GetAuthentication().GetPassword()
			err = self.authenticate(conn, auth)
			if err != nil {
				return err
			}
			// Authentication OK
			conn.WriteRequest(&Greeting{
				Type: &G_AUTHENTICATION_OK,
				Authentication: &Greeting_Authentication{
					Type: &conn.AccountType,
				},
				Sequence: proto.Uint32(conn.Sequence),
			})
			conn.IncrementSequence()
			state = HandshakeState_PROCESS_READY
			break
		case HandshakeState_PROCESS_READY:
			// sends some options
			conn.WriteRequest(&Greeting{
				Type: &G_COMMAND_READY,
				Sequence: proto.Uint32(conn.Sequence),
			})
			conn.IncrementSequence()
			state = HandshakeState_FINISHED
			break
		default:
			return errors.New(fmt.Sprintf("%s not supported. Closing Connection", state))
		}

		if state == HandshakeState_FINISHED {
			break
		}
	}
	return nil
}

func (self *Server) HandleConnection(conn *Connection) {
	log.Info("Experimental ProtobufServer: client connected: %s", conn.Address.String())

	for {
		err := self.handshake(conn)
		if err != nil || conn.State == STATE_INITIALIZED {
			conn.WriteRequest(&Greeting{
				Type: &G_ERROR,
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
