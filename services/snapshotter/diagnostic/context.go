package diagnostic

type Context struct {
	Handler Handler
}

type Handler interface {
	Starting()
	Closed()
	AcceptError(err error)
	Error(err error)
}

func (c *Context) Starting() {
	if c.Handler != nil {
		c.Handler.Starting()
	}
}

func (c *Context) Closed() {
	if c.Handler != nil {
		c.Handler.Closed()
	}
}

func (c *Context) AcceptError(err error) {
	if c.Handler != nil {
		c.Handler.AcceptError(err)
	}
}

func (c *Context) Error(err error) {
	if c.Handler != nil {
		c.Handler.Error(err)
	}
}
