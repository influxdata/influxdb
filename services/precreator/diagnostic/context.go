package diagnostic

import "time"

type Context struct {
	Handler Handler
}

type Handler interface {
	Starting(checkInterval, advancedPeriod time.Duration)
	Closing()
	PrecreateError(err error)
}

func (c *Context) Starting(checkInterval, advancedPeriod time.Duration) {
	if c.Handler != nil {
		c.Handler.Starting(checkInterval, advancedPeriod)
	}
}

func (c *Context) Closing() {
	if c.Handler != nil {
		c.Handler.Closing()
	}
}

func (c *Context) PrecreateError(err error) {
	if c.Handler != nil {
		c.Handler.PrecreateError(err)
	}
}
