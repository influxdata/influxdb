package diagnostic

import "net"

type Handler interface {
	ServiceHandler
	RouteHandler
}

type ServiceContext struct {
	Handler ServiceHandler
}

type ServiceHandler interface {
	Starting(authEnabled bool)
	Listening(protocol string, addr net.Addr)
}

func (c *ServiceContext) Starting(authEnabled bool) {
	if c.Handler != nil {
		c.Handler.Starting(authEnabled)
	}
}

func (c *ServiceContext) Listening(protocol string, addr net.Addr) {
	if c.Handler != nil {
		c.Handler.Listening(protocol, addr)
	}
}

type RouteContext struct {
	Handler RouteHandler
}

type RouteHandler interface {
	UnauthorizedRequest(user, query, database string)
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

func (c *RouteContext) UnauthorizedRequest(user, query, database string) {
	if c.Handler != nil {
		c.Handler.UnauthorizedRequest(user, query, database)
	}
}

func (c *RouteContext) AsyncQueryError(query string, err error) {
	if c.Handler != nil {
		c.Handler.AsyncQueryError(query, err)
	}
}

func (c *RouteContext) WriteBodyReceived(body []byte) {
	if c.Handler != nil {
		c.Handler.WriteBodyReceived(body)
	}
}

func (c *RouteContext) WriteBodyReadError() {
	if c.Handler != nil {
		c.Handler.WriteBodyReadError()
	}
}

func (c *RouteContext) StatusDeprecated() {
	if c.Handler != nil {
		c.Handler.StatusDeprecated()
	}
}

func (c *RouteContext) PromWriteBodyReceived(body []byte) {
	if c.Handler != nil {
		c.Handler.PromWriteBodyReceived(body)
	}
}

func (c *RouteContext) PromWriteBodyReadError() {
	if c.Handler != nil {
		c.Handler.PromWriteBodyReadError()
	}
}

func (c *RouteContext) PromWriteError(err error) {
	if c.Handler != nil {
		c.Handler.PromWriteError(err)
	}
}

func (c *RouteContext) JWTClaimsAssertError() {
	if c.Handler != nil {
		c.Handler.JWTClaimsAssertError()
	}
}

func (c *RouteContext) HttpError(status int, errStr string) {
	if c.Handler != nil {
		c.Handler.HttpError(status, errStr)
	}
}
