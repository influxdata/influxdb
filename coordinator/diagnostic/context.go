package diagnostic

type PointsWriterContext struct {
	Handler PointsWriterHandler
}

type PointsWriterHandler interface {
	WriteFailed(shardID uint64, err error)
}

func (c *PointsWriterContext) WriteFailed(shardID uint64, err error) {
	if c.Handler != nil {
		c.Handler.WriteFailed(shardID, err)
	}
}
