package diagnostic

type Context struct {
	Handler Handler
}

type Handler interface {
	Started(bindAddress string)
	Closed()
	CreateInternalStorageFailure(db string, err error)
	PointWriterError(database string, err error)
	ParseError(err error)
	ReadFromError(err error)
}

func (c *Context) Started(bindAddress string) {
	if c.Handler != nil {
		c.Handler.Started(bindAddress)
	}
}

func (c *Context) Closed() {
	if c.Handler != nil {
		c.Handler.Closed()
	}
}

func (c *Context) CreateInternalStorageFailure(db string, err error) {
	if c.Handler != nil {
		c.Handler.CreateInternalStorageFailure(db, err)
	}
}

func (c *Context) PointWriterError(database string, err error) {
	if c.Handler != nil {
		c.Handler.PointWriterError(database, err)
	}
}

func (c *Context) ParseError(err error) {
	if c.Handler != nil {
		c.Handler.ParseError(err)
	}
}

func (c *Context) ReadFromError(err error) {
	if c.Handler != nil {
		c.Handler.ReadFromError(err)
	}
}
