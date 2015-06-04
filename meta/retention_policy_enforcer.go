package meta

import (
	"time"
)

// RetentionPolicyEnforcer enforces retention policies.
type RetentionPolicyEnforcer struct {
	Store interface {
		Databases() (dis []DatabaseInfo, err error)
		DeleteShardGroup(database, policy string, id uint64) error
	}
}

// NewRetentionPolicyEnforcer returns a retention policy enforcer using the given store.
func NewRetentionPolicyEnforcer(store *Store) *RetentionPolicyEnforcer {
	return &RetentionPolicyEnforcer{Store: store}
}

// Enforce removes shard groups that represent data older than the retention policies
// allow. It then returns a slice of the shard IDs that must be deleted.
func (r *RetentionPolicyEnforcer) Enforce() ([]uint64, error) {
	dis, err := r.Store.Databases()
	if err != nil {
		return nil, err
	}

	// Check all shard groups.
	var ids []uint64
	for _, di := range dis {
		for _, rp := range di.RetentionPolicies {
			for _, g := range rp.ShardGroups {
				if rp.Duration != 0 && g.EndTime.Add(rp.Duration).Before(time.Now().UTC()) {
					for _, sh := range g.Shards {
						ids = append(ids, sh.ID)
					}

					if err := r.Store.DeleteShardGroup(di.Name, rp.Name, g.ID); err != nil {
						return nil, err
					}

				}
			}
		}
	}

	return ids, nil
}
