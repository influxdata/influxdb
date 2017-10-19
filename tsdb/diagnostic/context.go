package diagnostic

import "time"

type Context struct {
	Handler Handler
}

type Handler interface {
	ShardHandler
	SeriesCardinalityError(err error)
	MeasurementsCardinalityError(err error)
	UsingDataDir(path string)
	NotADatabaseDir(name string)
	SkippingRetentionPolicyDir(name string)
	ShardOpened(path string, dur time.Duration)
	ShardOpenError(err error)
	FreeColdShardError(err error)
	MeasurementNamesByExprError(err error)
	WarnMaxValuesPerTagLimitExceeded(perc, n, max int, db string, name, tag []byte)
}

func (c *Context) SeriesCardinalityError(err error) {
	if c.Handler != nil {
		c.Handler.SeriesCardinalityError(err)
	}
}

func (c *Context) MeasurementsCardinalityError(err error) {
	if c.Handler != nil {
		c.Handler.MeasurementsCardinalityError(err)
	}
}

func (c *Context) UsingDataDir(path string) {
	if c.Handler != nil {
		c.Handler.UsingDataDir(path)
	}
}

func (c *Context) NotADatabaseDir(name string) {
	if c.Handler != nil {
		c.Handler.NotADatabaseDir(name)
	}
}

func (c *Context) SkippingRetentionPolicyDir(name string) {
	if c.Handler != nil {
		c.Handler.SkippingRetentionPolicyDir(name)
	}
}

func (c *Context) ShardOpened(path string, dur time.Duration) {
	if c.Handler != nil {
		c.Handler.ShardOpened(path, dur)
	}
}

func (c *Context) ShardOpenError(err error) {
	if c.Handler != nil {
		c.Handler.ShardOpenError(err)
	}
}

func (c *Context) FreeColdShardError(err error) {
	if c.Handler != nil {
		c.Handler.FreeColdShardError(err)
	}
}

func (c *Context) MeasurementNamesByExprError(err error) {
	if c.Handler != nil {
		c.Handler.MeasurementNamesByExprError(err)
	}
}

func (c *Context) WarnMaxValuesPerTagLimitExceeded(perc, n, max int, db string, name, tag []byte) {
	if c.Handler != nil {
		c.Handler.WarnMaxValuesPerTagLimitExceeded(perc, n, max, db, name, tag)
	}
}

type ShardContext struct {
	Handler ShardHandler
}

type ShardHandler interface {
	AttachEngine(name string, engine interface{})
	AttachIndex(name string, index interface{})
}

func (c *ShardContext) AttachEngine(name string, engine interface{}) {
	if c.Handler != nil {
		c.Handler.AttachEngine(name, engine)
	}
}

func (c *ShardContext) AttachIndex(name string, index interface{}) {
	if c.Handler != nil {
		c.Handler.AttachIndex(name, index)
	}
}
