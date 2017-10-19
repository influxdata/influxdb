package diagnostic

import "net"

type Context struct {
	Handler Handler
}

type Handler interface {
	Starting()
	Listening(tls bool, addr net.Addr)
	Closed(err error)

	ConnReadError(err error)
	InternalStorageCreateError(database string, err error)
	PointWriterError(database string, err error)

	MalformedLine(line string, addr net.Addr)
	MalformedTime(time string, addr net.Addr, err error)
	MalformedTag(tag string, addr net.Addr)
	MalformedFloat(valueStr string, addr net.Addr)

	WriteHandler
}

func (c *Context) Starting() {
	if c.Handler != nil {
		c.Handler.Starting()
	}
}

func (c *Context) Listening(tls bool, addr net.Addr) {
	if c.Handler != nil {
		c.Handler.Listening(tls, addr)
	}
}

func (c *Context) Closed(err error) {
	if c.Handler != nil {
		c.Handler.Closed(err)
	}
}

func (c *Context) ConnReadError(err error) {
	if c.Handler != nil {
		c.Handler.ConnReadError(err)
	}
}

func (c *Context) InternalStorageCreateError(database string, err error) {
	if c.Handler != nil {
		c.Handler.InternalStorageCreateError(database, err)
	}
}

func (c *Context) PointWriterError(database string, err error) {
	if c.Handler != nil {
		c.Handler.PointWriterError(database, err)
	}
}

func (c *Context) MalformedLine(line string, addr net.Addr) {
	if c.Handler != nil {
		c.Handler.MalformedLine(line, addr)
	}
}

func (c *Context) MalformedTime(time string, addr net.Addr, err error) {
	if c.Handler != nil {
		c.Handler.MalformedTime(time, addr, err)
	}
}

func (c *Context) MalformedTag(tag string, addr net.Addr) {
	if c.Handler != nil {
		c.Handler.MalformedTag(tag, addr)
	}
}

func (c *Context) MalformedFloat(valueStr string, addr net.Addr) {
	if c.Handler != nil {
		c.Handler.MalformedFloat(valueStr, addr)
	}
}

func (c *Context) DroppingPoint(metric string, err error) {
	if c.Handler != nil {
		c.Handler.DroppingPoint(metric, err)
	}
}

func (c *Context) WriteError(err error) {
	if c.Handler != nil {
		c.Handler.WriteError(err)
	}
}

type WriteContext struct {
	Handler WriteHandler
}

type WriteHandler interface {
	DroppingPoint(metric string, err error)
	WriteError(err error)
}

func (c *WriteContext) DroppingPoint(metric string, err error) {
	if c.Handler != nil {
		c.Handler.DroppingPoint(metric, err)
	}
}

func (c *WriteContext) WriteError(err error) {
	if c.Handler != nil {
		c.Handler.WriteError(err)
	}
}
