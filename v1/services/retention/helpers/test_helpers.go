package helpers

import (
	"fmt"
	"time"

	"golang.org/x/exp/slices"

	"github.com/influxdata/influxdb/v2/v1/services/meta"
)

// DataDeleteShardGroup deletes the shard group specified by database, policy, and id from targetData.
// It does this by setting the shard group's DeletedAt time to now. We have to reimplement DeleteShardGroup
// instead of using data's so that the DeletedAt time will be deterministic. We are also not testing
// the functionality of DeleteShardGroup. We are testing if DeleteShardGroup gets called correctly.
func DataDeleteShardGroup(targetData *meta.Data, now time.Time, database, policy string, id uint64) error {
	rpi, err := targetData.RetentionPolicy(database, policy)

	if err != nil {
		return err
	} else if rpi == nil {
		return meta.ErrRetentionPolicyNotFound
	}

	// Find shard group by ID and set its deletion timestamp.
	for i := range rpi.ShardGroups {
		if rpi.ShardGroups[i].ID == id {
			rpi.ShardGroups[i].DeletedAt = now
			return nil
		}
	}

	return meta.ErrShardGroupNotFound
}

// DataNukeShardGroup unconditionally removes the shard group identified by targetDB, targetRP, and targetID
// from targetData. There's no meta.Data method to directly remove a shard group, only to mark it deleted and
// then prune it. We can't use the functionality we're testing to generate the expected result.
func DataNukeShardGroup(targetData *meta.Data, targetDB, targetRP string, targetID uint64) error {
	rpi, err := targetData.RetentionPolicy(targetDB, targetRP)
	if err != nil {
		return err
	} else if rpi == nil {
		return fmt.Errorf("no retention policy found for %q, %q, %d", targetDB, targetRP, targetID)
	}
	isTargetShardGroup := func(sgi meta.ShardGroupInfo) bool {
		return sgi.ID == targetID
	}
	if !slices.ContainsFunc(rpi.ShardGroups, isTargetShardGroup) {
		return fmt.Errorf("shard not found for %q, %q, %d", targetDB, targetRP, targetID)
	}
	rpi.ShardGroups = slices.DeleteFunc(rpi.ShardGroups, isTargetShardGroup)
	return nil
}
