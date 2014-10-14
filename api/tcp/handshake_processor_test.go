package tcp_test

import (
	. "launchpad.net/gocheck"

	"github.com/influxdb/influxdb/api/tcp"
//	"code.google.com/p/goprotobuf/proto"
)

type HandshakeSuite struct{}

var _ = Suite(&HandshakeSuite{})

func (s *HandshakeSuite) TestHandshake(c *C) {
	req, conn := getMock()
	p := &tcp.HandshakeProcessorImpl{
		Server: req.Server,
	}

	// Basic handshake flow
	conn.PrepareGreetingMessages([]*tcp.Greeting{
		// [1] send startup message
		&tcp.Greeting{
			Type:  &tcp.G_STARTUP_MESSAGE,
			// optional
			Agent: []byte("influx-test"),
			// requires
			Authentication: &tcp.Greeting_Authentication{
				Name:     []byte("chobie"),
				Database: []byte("chobie"),
			},
			// Config is optional parameter
			Config: &tcp.Greeting_Configuration{
				CompressType: &tcp.G_Configuration_PLAIN,
				Ssl: &tcp.G_Configuration_NONE,
			},
		},
		// [2] Recv startup response
		// [3] send authentication message
		&tcp.Greeting{
			Type:  &tcp.G_AUTHENTICATION,
			Authentication: &tcp.Greeting_Authentication{
				Password: []byte("chobie"),
			},
		},
		// [4] Recv authentication ok
		// [5] Recv options (optional)
		// [6] Recv ready (end of handshake)
	})

	err := p.Handshake(conn)

	//fmt.Printf("Error: %+v\n", err)
	//fmt.Printf("Messages: %+v\n", conn.ReceivedMessages)

	c.Assert(err, IsNil)
	c.Assert(conn.ReceivedMessages[0].GetType(), Equals, tcp.G_STARTUP_RESPONSE)
	c.Assert(conn.ReceivedMessages[0].GetAuthentication().GetMethod(), Equals, tcp.G_AUTHENTICATION_CLEARTEXT_PASSWORD)
	c.Assert(conn.ReceivedMessages[0].GetConfig().GetCompressType(), Equals, tcp.G_CONFIGURATION_PLAIN)
	c.Assert(conn.ReceivedMessages[0].GetConfig().GetSsl(), Equals, tcp.G_CONFIGURATION_NONE)

	c.Assert(conn.ReceivedMessages[1].GetType(), Equals, tcp.G_AUTHENTICATION_OK)
	c.Assert(conn.ReceivedMessages[1].GetAuthentication().GetType(), Equals, tcp.G_Authentication_CLUSTER_ADMIN)
	c.Assert(conn.ReceivedMessages[2].GetType(), Equals, tcp.G_COMMAND_READY)
}

func (s *HandshakeSuite) TestSSLHandshake(c *C) {
}
