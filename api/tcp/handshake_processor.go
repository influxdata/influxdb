package tcp

import(
	"fmt"
	"errors"

	log "code.google.com/p/log4go"
	"code.google.com/p/goprotobuf/proto"
	"crypto/tls"
)

type HandshakeProcessorImpl struct{
	Server Server
}

func (self *HandshakeProcessorImpl) BeginSSLHandshake(conn Connection) error {
	var tlsConn *tls.Conn

	// TODO: blush up this
	if ts, ok := self.Server.(*TcpServer); ok && ts.tlsConfig != nil {
		// NOTE(chobie): Assume ssl handshake.
		tlsConn = tls.Server(conn.GetSocket(), ts.tlsConfig)
		if err := tlsConn.Handshake(); err != nil {
			log.Debug("SSL HANDSHAKE FAILED: %+v", err)
			return err
		}
		conn.SetSocket(tlsConn)
		return nil
	} else {
		return errors.New("SSL NOT SUPPORTED")
	}
}

func (self *HandshakeProcessorImpl) Handshake(conn Connection) error {
	var err error
	var state HandshakeState = HandshakeState_INITIALIZED

	auth := &Greeting_Authentication{}
	for {
		switch state {
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
				return errors.New("illegal handshake")
			}

			database := startup.GetAuthentication().GetDatabase()
			name := startup.GetAuthentication().GetName()
			auth.Database = database
			auth.Name = name

			ssl := startup.GetConfig().GetSsl()
			if !self.Server.SSLAvailable() {
				// force overload
				ssl = G_CONFIGURATION_NONE
			} else {
				if self.Server.IsForceSSLUser(string(name)) {
					ssl = G_Configuration_REQUIRED
				}
			}

			req := &Greeting{
				Type:            &G_STARTUP_RESPONSE,
				ProtocolVersion: proto.Int32(1),
				Agent:           self.Server.GetInfluxDBVersions(),
				Sequence:        proto.Uint32(conn.GetSequence()),
				Authentication: &Greeting_Authentication{
					Method: &G_AUTHENTICATION_CLEARTEXT_PASSWORD,
				},
				Config: &Greeting_Configuration{
					// TODO: support gzip or snappy
					CompressType: &G_CONFIGURATION_PLAIN,
					Ssl:          &ssl,
				},
			}
			conn.WriteRequest(req)
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

			conn.ClearBuffer()
			if e := self.BeginSSLHandshake(conn); e != nil {
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
			if message.GetType() != G_AUTHENTICATION {
				return errors.New("Illegal handshake")
			}

			auth.Password = message.GetAuthentication().GetPassword()
			err = self.Server.Authenticate(conn, auth)
			if err != nil {
				return errors.New(fmt.Sprintf("%s", err))
			}
			// Authentication OK
			conn.WriteRequest(&Greeting{
				Type: &G_AUTHENTICATION_OK,
				Authentication: &Greeting_Authentication{
					Type: conn.GetAccountType(),
				},
				Sequence: proto.Uint32(conn.GetSequence()),
			})
			state = HandshakeState_PROCESS_READY
			break
		case HandshakeState_PROCESS_READY:
			// TODO: sends some options (timezone, field names...)
			conn.WriteRequest(&Greeting{
				Type:     &G_COMMAND_READY,
				Sequence: proto.Uint32(conn.GetSequence()),
			})
			state = HandshakeState_FINISHED
			break
		default:
			return errors.New(fmt.Sprintf("%s not supported. Closing Connection", state))
		}

		if state == HandshakeState_FINISHED {
			// TODO: do something here.
			break
		}
	}

	return nil
}
