package export

import (
	"time"

	"github.com/influxdata/influxdb/services/meta"
)

func makeShardGroupsForDuration(min, max time.Time, d time.Duration) meta.ShardGroupInfos {
	start := min.Truncate(d).UTC()
	end := max.Truncate(d).Add(d).UTC()

	groups := make(meta.ShardGroupInfos, end.Sub(start)/d)
	var i uint64
	for start.Before(end) {
		groups[i] = meta.ShardGroupInfo{
			ID:        i,
			StartTime: start,
			EndTime:   start.Add(d),
		}
		i++
		start = start.Add(d)
	}
	return groups[:i]
}

// PlanShardGroups creates a new ShardGroup set using a shard group duration of d, for the time spanning min to max.
func planShardGroups(sourceShards []meta.ShardGroupInfo, min, max time.Time, d time.Duration) meta.ShardGroupInfos {
	groups := makeShardGroupsForDuration(min, max, d)
	var target []meta.ShardGroupInfo
	for i := 0; i < len(groups); i++ {
		g := groups[i]
		// NOTE: EndTime.Add(-1) matches the Contains interval of [start, end)
		if hasShardsGroupForTimeRange(sourceShards, g.StartTime, g.EndTime.Add(-1)) {
			target = append(target, g)
		}
	}
	return target
}

func hasShardsGroupForTimeRange(groups []meta.ShardGroupInfo, min, max time.Time) bool {
	for _, g := range groups {
		if g.Overlaps(min, max) {
			return true
		}
	}
	return false
}
