package diagnostic

import "time"

type Context struct {
	Handler Handler
}

type Handler interface {
	Starting()
	AlreadyOpen()
	Closing()
	AlreadyClosed()
	DiagnosticRegistered(name string)

	CreateInternalStorageFailure(db string, err error)
	StoreStatistics(db, rp string, interval time.Duration)
	StoreStatisticsError(err error)
	StatisticsRetrievalFailure(err error)
	DroppingPoint(name string, err error)
	StoreStatisticsDone()
}

func (c *Context) Starting() {
	if c.Handler != nil {
		c.Handler.Starting()
	}
}

func (c *Context) AlreadyOpen() {
	if c.Handler != nil {
		c.Handler.AlreadyOpen()
	}
}

func (c *Context) Closing() {
	if c.Handler != nil {
		c.Handler.Closing()
	}
}

func (c *Context) AlreadyClosed() {
	if c.Handler != nil {
		c.Handler.AlreadyClosed()
	}
}

func (c *Context) DiagnosticRegistered(name string) {
	if c.Handler != nil {
		c.Handler.DiagnosticRegistered(name)
	}
}

func (c *Context) CreateInternalStorageFailure(db string, err error) {
	if c.Handler != nil {
		c.Handler.CreateInternalStorageFailure(db, err)
	}
}

func (c *Context) StoreStatistics(db, rp string, interval time.Duration) {
	if c.Handler != nil {
		c.Handler.StoreStatistics(db, rp, interval)
	}
}

func (c *Context) StoreStatisticsError(err error) {
	if c.Handler != nil {
		c.Handler.StoreStatisticsError(err)
	}
}

func (c *Context) StatisticsRetrievalFailure(err error) {
	if c.Handler != nil {
		c.Handler.StatisticsRetrievalFailure(err)
	}
}

func (c *Context) DroppingPoint(name string, err error) {
	if c.Handler != nil {
		c.Handler.DroppingPoint(name, err)
	}
}

func (c *Context) StoreStatisticsDone() {
	if c.Handler != nil {
		c.Handler.StoreStatisticsDone()
	}
}
