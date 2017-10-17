package diagnostic

import "time"

type Context struct {
	Handler Handler
}

type Handler interface {
	Starting(checkInterval time.Duration)
	Closing()

	StartingCheck()
	DeletedShard(id uint64, db, rp string)
	DeleteShardError(id uint64, db, rp string, err error)
	DeletedShardGroup(id uint64, db, rp string)
	DeleteShardGroupError(id uint64, db, rp string, err error)
	PruneShardGroupsError(err error)
}

func (c *Context) Starting(checkInterval time.Duration) {
	if c.Handler != nil {
		c.Handler.Starting(checkInterval)
	}
}

func (c *Context) Closing() {
	if c.Handler != nil {
		c.Handler.Closing()
	}
}

func (c *Context) StartingCheck() {
	if c.Handler != nil {
		c.Handler.StartingCheck()
	}
}

func (c *Context) DeletedShard(id uint64, db, rp string) {
	if c.Handler != nil {
		c.Handler.DeletedShard(id, db, rp)
	}
}

func (c *Context) DeleteShardError(id uint64, db, rp string, err error) {
	if c.Handler != nil {
		c.Handler.DeleteShardError(id, db, rp, err)
	}
}

func (c *Context) DeletedShardGroup(id uint64, db, rp string) {
	if c.Handler != nil {
		c.Handler.DeletedShardGroup(id, db, rp)
	}
}

func (c *Context) DeleteShardGroupError(id uint64, db, rp string, err error) {
	if c.Handler != nil {
		c.Handler.DeleteShardGroupError(id, db, rp, err)
	}
}

func (c *Context) PruneShardGroupsError(err error) {
	if c.Handler != nil {
		c.Handler.PruneShardGroupsError(err)
	}
}
