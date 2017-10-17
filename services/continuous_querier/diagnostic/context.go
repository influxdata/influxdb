package diagnostic

import "time"

type Context struct {
	Handler Handler
}

type Handler interface {
	Starting()
	Closing()
	RunningByRequest(now time.Time)
	ExecuteContinuousQuery(name string, start, end time.Time)
	ExecuteContinuousQueryError(query string, err error)
	FinishContinuousQuery(name string, written int64, start, end time.Time, dur time.Duration)
}

func (c *Context) Starting() {
	if c.Handler != nil {
		c.Handler.Starting()
	}
}

func (c *Context) Closing() {
	if c.Handler != nil {
		c.Handler.Closing()
	}
}

func (c *Context) RunningByRequest(now time.Time) {
	if c.Handler != nil {
		c.Handler.RunningByRequest(now)
	}
}

func (c *Context) ExecuteContinuousQuery(name string, start, end time.Time) {
	if c.Handler != nil {
		c.Handler.ExecuteContinuousQuery(name, start, end)
	}
}

func (c *Context) ExecuteContinuousQueryError(query string, err error) {
	if c.Handler != nil {
		c.Handler.ExecuteContinuousQueryError(query, err)
	}
}

func (c *Context) FinishContinuousQuery(name string, written int64, start, end time.Time, dur time.Duration) {
	if c.Handler != nil {
		c.Handler.FinishContinuousQuery(name, written, start, end, dur)
	}
}
