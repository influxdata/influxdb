package diagnostic

type PointsWriterContext interface {
	WriteFailed(shardID uint64, err error)
}
