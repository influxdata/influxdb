package diagnostic

type Context struct {
	Handler Handler
}

type Handler interface {
	ShardGroupExists(group uint64, db, rp string)
	PrecreateShardError(group uint64, err error)
	NewShardGroup(group uint64, db, rp string)
}

func (c *Context) ShardGroupExists(group uint64, db, rp string) {
	if c.Handler != nil {
		c.Handler.ShardGroupExists(group, db, rp)
	}
}

func (c *Context) PrecreateShardError(group uint64, err error) {
	if c.Handler != nil {
		c.Handler.PrecreateShardError(group, err)
	}
}

func (c *Context) NewShardGroup(group uint64, db, rp string) {
	if c.Handler != nil {
		c.Handler.NewShardGroup(group, db, rp)
	}
}
