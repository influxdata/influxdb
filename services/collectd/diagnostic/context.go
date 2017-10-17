package diagnostic

import "net"

type Context struct {
	Handler Handler
}

type Handler interface {
	Starting()
	Listening(addr net.Addr)
	Closed()
	UnableToReadDirectory(path string, err error)
	LoadingPath(path string)
	TypesParseError(path string, err error)

	ReadFromUDPError(err error)
	ParseError(err error)
	DroppingPoint(name string, err error)
	InternalStorageCreateError(err error)
	PointWriterError(database string, err error)
}

func (c *Context) Starting() {
	if c.Handler != nil {
		c.Handler.Starting()
	}
}

func (c *Context) Listening(addr net.Addr) {
	if c.Handler != nil {
		c.Handler.Listening(addr)
	}
}

func (c *Context) Closed() {
	if c.Handler != nil {
		c.Handler.Closed()
	}
}

func (c *Context) UnableToReadDirectory(path string, err error) {
	if c.Handler != nil {
		c.Handler.UnableToReadDirectory(path, err)
	}
}

func (c *Context) LoadingPath(path string) {
	if c.Handler != nil {
		c.Handler.LoadingPath(path)
	}
}

func (c *Context) TypesParseError(path string, err error) {
	if c.Handler != nil {
		c.Handler.TypesParseError(path, err)
	}
}

func (c *Context) ReadFromUDPError(err error) {
	if c.Handler != nil {
		c.Handler.ReadFromUDPError(err)
	}
}

func (c *Context) ParseError(err error) {
	if c.Handler != nil {
		c.Handler.ParseError(err)
	}
}

func (c *Context) DroppingPoint(name string, err error) {
	if c.Handler != nil {
		c.Handler.DroppingPoint(name, err)
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
