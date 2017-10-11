package diagnostic

import "net"

type Context interface {
	// Service diagnostics.
	Starting(authEnabled bool)
	Listening(protocol string, addr net.Addr)

	// Handler diagnostics.
	UnauthorizedRequest(user string, query, database string)
	AsyncQueryError(query string, err error)
	WriteBodyReceived(body []byte)
	WriteBodyReadError()
	StatusDeprecated()
	PromWriteBodyReceived(body []byte)
	PromWriteBodyReadError()
	PromWriteError(err error)
	JWTClaimsAssertError()
	HttpError(status int, errStr string)
}
