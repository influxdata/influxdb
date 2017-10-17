package diagnostic

import (
	"net"
	"time"
)

type HandlerBuilder interface {
	WithContext(bindAddress string) Handler
}

type Context struct {
	Handler Handler
}

type Handler interface {
	Starting(batchSize int, batchTimeout time.Duration)
	Listening(protocol string, addr net.Addr)
	TCPListenerClosed()
	TCPAcceptError(err error)
	LineParseError(line string, err error)
	InternalStorageCreateError(err error)
	PointWriterError(database string, err error)
}

func (c *Context) Starting(batchSize int, batchTimeout time.Duration) {
	if c.Handler != nil {
		c.Handler.Starting(batchSize, batchTimeout)
	}
}

func (c *Context) Listening(protocol string, addr net.Addr) {
	if c.Handler != nil {
		c.Handler.Listening(protocol, addr)
	}
}

func (c *Context) TCPListenerClosed() {
	if c.Handler != nil {
		c.Handler.TCPListenerClosed()
	}
}

func (c *Context) TCPAcceptError(err error) {
	if c.Handler != nil {
		c.Handler.TCPAcceptError(err)
	}
}

func (c *Context) LineParseError(line string, err error) {
	if c.Handler != nil {
		c.Handler.LineParseError(line, err)
	}
}

func (c *Context) InternalStorageCreateError(err error) {
	if c.Handler != nil {
		c.Handler.InternalStorageCreateError(err)
	}
}

func (c *Context) PointWriterError(database string, err error) {
	if c.Handler != nil {
		c.Handler.PointWriterError(database, err)
	}
}
