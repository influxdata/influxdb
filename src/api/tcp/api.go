	package tcp

import (
	"cluster"
	. "common"
	"configuration"
	"coordinator"

	api "api/http"
	log "code.google.com/p/log4go"

	"crypto/rand"
	"crypto/tls"
)

const KILOBYTE = 1024
const MEGABYTE = 1024 * KILOBYTE
const MAX_REQUEST_SIZE = MEGABYTE * 2

type HandshakeState int32

const (
	HandshakeState_INITIALIZED                  HandshakeState = 0
	HandshakeState_WAIT_STARTUP_RESPONSE        HandshakeState = 1
	HandshakeState_AUTHENTICATION_REQUEST       HandshakeState = 2
	HandshakeState_WAIT_AUTHENTICATION_RESPONSE HandshakeState = 3
	HandshakeState_PROCESS_READY                HandshakeState = 4
	HandshakeState_SEND_AUTHENTICATION          HandshakeState = 5
	HandshakeState_FINISHED                     HandshakeState = 6
	HandshakeState_UPGRADE_SSL                  HandshakeState = 7
	HandshakeState_WAIT_STARTUP_REQUEST                        = 8
	HandshakeState_RECV_AUTHENTICATION                         = 9
	HandshakeState_ERROR                        HandshakeState = 999
)

// TODO: blush up
var G_STARTUP_MESSAGE = Greeting_STARTUP_MESSAGE
var G_AUTHENTICATION = Greeting_AUTHENTICATION
var G_AUTHENTICATION_OK = Greeting_AUTHENTICATION_OK
var G_Configuration_PLAIN = Greeting_Configuration_PLAIN
var G_Authentication_CLEARTEXT_PASSWORD = Greeting_Authentication_CLEARTEXT_PASSWORD
var G_MESSAGE_OPTION = Greeting_MESSAGE_OPTION
var G_SSL_UPGRADE = Greeting_SSL_UPGRADE
var G_Configuration_REQUIRED = Greeting_Configuration_REQUIRED
var G_Configuration_NONE = Greeting_Configuration_NONE
var G_COMMAND_READY = Greeting_COMMAND_READY
var G_STARTUP_RESPONSE = Greeting_STARTUP_RESPONSE
var G_CONFIGURATION_PLAIN = Greeting_Configuration_PLAIN
var G_CONFIGURATION_NONE = Greeting_Configuration_NONE
var G_AUTHENTICATION_CLEARTEXT_PASSWORD = Greeting_Authentication_CLEARTEXT_PASSWORD
var G_ERROR = Greeting_ERROR
var G_Authentication_CLUSTER_ADMIN = Greeting_Authentication_CLUSTER_ADMIN
var G_Authentication_DB_USER = Greeting_Authentication_DB_USER

// Commands
var C_OK = Command_OK
var C_FAIL = Command_FAIL
var C_CREATEDATABASE = Command_CREATEDATABASE
var C_DROPDATABASE = Command_DROPDATABASE
var C_CHANGEDATABASE = Command_CHANGEDATABASE
var C_LISTDATABASE = Command_LISTDATABASE
var C_WRITESERIES = Command_WRITESERIES
var C_QUERY = Command_QUERY
var C_CLOSE = Command_CLOSE
var C_PING = Command_PING


func NewServer(config *configuration.Configuration, coord coordinator.Coordinator, um api.UserManager, clusterConfig *cluster.ClusterConfiguration) *TcpServer {
	server := &TcpServer{
		forceSSLUsers: map[string]bool{},
		Connections: map[string]Connection{},
	}

	server.listenAddress = config.TcpInputPortString()
	server.listenSocket = config.TcpInputSocketString()

	if config.TcpInputUseSSL {
		cert, err := tls.LoadX509KeyPair(config.TcpInputSSLCert(), config.TcpInputSSLKey())
		if err != nil {
			log.Error("tcp server: loadkeys failed. disable ssl feature: %s", err)
		} else {
			tslConfig := &tls.Config{Certificates: []tls.Certificate{cert}}
			tslConfig.Rand = rand.Reader

			server.tlsConfig = tslConfig
			for _, name := range config.TcpInputForceSSL() {
				server.forceSSLUsers[name] = true
			}
			log.Debug("SSL Config loaded")
		}
	}

	server.Coordinator = coord
	server.UserManager = um
	server.shutdown = make(chan bool, 1)
	server.clusterConfig = clusterConfig
	server.RequestHandler = NewRequestHandler(server)

	return server
}
