package diagnostic

import "time"

type Context interface {
	Starting(checkInterval time.Duration)
	Closing()

	StartingCheck()
	DeletedShard(id uint64, db, rp string)
	DeleteShardError(id uint64, db, rp string, err error)
	DeletedShardGroup(id uint64, db, rp string)
	DeleteShardGroupError(id uint64, db, rp string, err error)
	PruneShardGroupsError(err error)
}
