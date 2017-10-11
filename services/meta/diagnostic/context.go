package diagnostic

type Context interface {
	ShardGroupExists(group uint64, db, rp string)
	PrecreateShardError(group uint64, err error)
	NewShardGroup(group uint64, db, rp string)
}
