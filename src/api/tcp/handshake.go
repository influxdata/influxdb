package tcp

type HandshakeProcessor interface{
	BeginSSLHandshake(conn Connection) error
	// begin handshake
	//   Sever will send error response when returns error
	Handshake(conn Connection) error
}
