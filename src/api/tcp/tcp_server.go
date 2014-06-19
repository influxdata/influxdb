package tcp

import (
	"cluster"
	. "common"
	"coordinator"
	"net"
	"fmt"

	api "api/http"
	log "code.google.com/p/log4go"
	"os"

	"time"
	"code.google.com/p/goprotobuf/proto"
	"crypto/tls"
	"errors"
)

type TcpServer struct {
	listenAddress  string
	listenSocket   string
	database       string
	Coordinator    coordinator.Coordinator
	UserManager    api.UserManager
	clusterConfig  *cluster.ClusterConfiguration
	shutdown       chan bool
	RequestHandler *RequestHandler
	tlsConfig      *tls.Config
	forceSSLUsers map[string]bool
	Connections map[string]Connection
	ConnectionCount int
}

func (self *TcpServer) GetCoordinator() coordinator.Coordinator {
	return self.Coordinator
}

func (self *TcpServer) IsForceSSLUser(name string) bool {
	if v, ok := self.forceSSLUsers[name]; ok && v == true {
		return true
	} else {
		return false
	}
}

func (self *TcpServer) SendErrorMessage(conn Connection, t Command_CommandType, message string) error {
	result := Command_FAIL
	v := Error_PERMISSION_DENIED
	response := &Command{
		Type:   &t,
		Result: &result,
		Error: &Error{
			Code:   &v,
			Reason: []byte(message),
		},
	}

	conn.WriteRequest(response)
	return errors.New(message)
}

func (self *TcpServer) handleRequest(conn Connection) error {
	err := self.RequestHandler.HandleRequest(conn)
	conn.IncrementSequence()
	return err
}

func getVersion() []byte {
	// TODO: how do I get current InfluxDB version
	version := fmt.Sprintf("InfluxDB v%s (git: %s)", "dev", "0000")
	return []byte(version)
}

func (self *TcpServer) Authenticate(conn Connection, info *Greeting_Authentication) error {
	var u User
	var e error
	var t Greeting_Authentication_AccountType

	db := string(info.GetDatabase())
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
		conn.SetState(STATE_AUTHENTICATED)
	} else {
		u, e = self.UserManager.AuthenticateClusterAdmin(user, pass)
		if e != nil {
			return e
		}

		t = Greeting_Authentication_CLUSTER_ADMIN
		conn.SetState(STATE_AUTHENTICATED)
	}

	conn.SetUser(u)
	conn.SetAccountType(t)
	conn.SetDatabase(db)

	return nil
}

func (self *TcpServer) SSLAvailable() bool {
	return true
}

func (self *TcpServer) handshake(conn Connection) error {
	processor := &HandshakeProcessorImpl{
		Server: self,
	}
	return processor.Handshake(conn)
}

func (self *TcpServer) HandleConnection(conn Connection) {
	log.Info("Experimental ProtobufServer: client connected: %s", conn.GetAddress().String())

	for {
		err := self.handshake(conn)
		if err != nil || conn.GetState() == STATE_INITIALIZED {
			conn.WriteRequest(&Greeting{
				Type:     &G_ERROR,
				Sequence: proto.Uint32(conn.GetSequence()),
			})
			log.Error("handshake Failed: ", err)
			conn.Close()
			return
		}

		for {
			log.Debug("handle Request: %+v", conn)

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
					break
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

func (self *TcpServer) tcpListenAndServe() {
	var err error
	var server net.Listener

	log.Debug("TcpServer: Listen at: ", self.listenAddress)
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

func (self *TcpServer) acceptLoop(listener net.Listener, yield func()) {
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
		if self.ConnectionCount > 100 {
			// TODO: Send Error message and close connection immediately.
		}

		conn := NewTcpConnection(client, self, func(c Connection, time time.Time) {
			//log.Debug("Closing Connection")
			//conn.Close()
		})
		self.Connections[client.RemoteAddr().String()] = conn
		self.ConnectionCount++

		go self.HandleConnection(conn)
	}
}

func (self *TcpServer) unixListenAndServe() {
	var err error
	var server net.Listener

	log.Debug("UnixServer: Listen at: ", self.listenSocket)

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

func (self *TcpServer) RemoveConnection(conn Connection) {
	delete(self.Connections, conn.GetSocket().RemoteAddr().String())
	self.ConnectionCount--
}

func (self *TcpServer) housekeeping() {
	ticker := time.NewTicker(time.Second)
	quit := make(chan struct{})
	// TODO: close this goroutine when server in shutodown state
	go func() {
		for {
			select {
			case <- ticker.C:
				// TODO: do manage task here.
				// * closing connection
				//   checks all connections and kill idle connection every 10 seconds.
				//   ConnectionState: Idle, Send, Receive, Handshake?
				// * set deadline
//				for _, con := range self.Connections {
//					// if conn.ShouldKill() {
//					// 	con.Close()
//					// }
//				}

			case <- quit:
				ticker.Stop()
				for _, con := range self.Connections {
					con.Close()
				}
				return
			}
		}
	}()
	select{}
}

// TODO: add stats support
func (self *TcpServer) ListenAndServe() {
	defer func() { self.shutdown <- true }()

	if self.listenAddress != "" {
		go self.tcpListenAndServe()
	}
	if self.listenSocket != "" {
		go self.unixListenAndServe()
	}
	go self.housekeeping()
	select {}
}
