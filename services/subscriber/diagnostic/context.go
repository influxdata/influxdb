package diagnostic

type Context struct {
	Handler Handler
}

type Handler interface {
	Opened()
	Closed()
	UpdateSubscriptionError(err error)
	SubscriptionCreateError(name string, err error)
	AddedSubscription(db, rp string)
	DeletedSubscription(db, rp string)
	SkipInsecureVerify()
	Error(err error)
}

func (c *Context) Opened() {
	if c.Handler != nil {
		c.Handler.Opened()
	}
}

func (c *Context) Closed() {
	if c.Handler != nil {
		c.Handler.Closed()
	}
}

func (c *Context) UpdateSubscriptionError(err error) {
	if c.Handler != nil {
		c.Handler.UpdateSubscriptionError(err)
	}
}

func (c *Context) SubscriptionCreateError(name string, err error) {
	if c.Handler != nil {
		c.Handler.SubscriptionCreateError(name, err)
	}
}

func (c *Context) AddedSubscription(db, rp string) {
	if c.Handler != nil {
		c.Handler.AddedSubscription(db, rp)
	}
}

func (c *Context) DeletedSubscription(db, rp string) {
	if c.Handler != nil {
		c.Handler.DeletedSubscription(db, rp)
	}
}

func (c *Context) SkipInsecureVerify() {
	if c.Handler != nil {
		c.Handler.SkipInsecureVerify()
	}
}

func (c *Context) Error(err error) {
	if c.Handler != nil {
		c.Handler.Error(err)
	}
}
