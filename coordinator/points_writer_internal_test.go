package coordinator

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/stretchr/testify/require"
)

func TestSgList_ShardGroupAt(t *testing.T) {
	base := time.Date(2016, 10, 19, 0, 0, 0, 0, time.UTC)
	day := func(n int) time.Time {
		return base.Add(time.Duration(24*n) * time.Hour)
	}

	list := sgList{
		items: meta.ShardGroupInfos{
			{ID: 1, StartTime: day(0), EndTime: day(1)},
			{ID: 2, StartTime: day(1), EndTime: day(2)},
			{ID: 3, StartTime: day(2), EndTime: day(3)},
			// SG day 3 to day 4 missing...
			{ID: 4, StartTime: day(4), EndTime: day(5)},
			{ID: 5, StartTime: day(5), EndTime: day(6)},
		},
		needsSort: true,
		earliest:  day(0),
		latest:    day(6),
	}

	examples := []struct {
		T            time.Time
		ShardGroupID uint64 // 0 will indicate we don't expect a shard group
	}{
		{T: base.Add(-time.Minute), ShardGroupID: 0}, // Before any SG
		{T: day(0), ShardGroupID: 1},
		{T: day(0).Add(time.Minute), ShardGroupID: 1},
		{T: day(1), ShardGroupID: 2},
		{T: day(3).Add(time.Minute), ShardGroupID: 0}, // No matching SG
		{T: day(5).Add(time.Hour), ShardGroupID: 5},
	}

	for i, example := range examples {
		sg := list.ShardGroupAt(example.T)
		var id uint64
		if sg != nil {
			id = sg.ID
		}

		if got, exp := id, example.ShardGroupID; got != exp {
			t.Errorf("[Example %d] got %v, expected %v", i+1, got, exp)
		}
	}
}

func TestSgList_ShardGroupAtOverlapping(t *testing.T) {
	base := time.Date(2016, 10, 19, 0, 0, 0, 0, time.UTC)
	hour := func(n int) time.Time {
		return base.Add(time.Duration(n) * time.Hour)
	}
	day := func(n int) time.Time {
		return base.Add(time.Duration(24*n) * time.Hour)
	}

	list := sgList{
		items: meta.ShardGroupInfos{
			{ID: 1, StartTime: hour(5), EndTime: hour(6)},
			{ID: 2, StartTime: hour(6), EndTime: hour(7)},
			// Day-long shard overlaps with the two hour-long shards.
			{ID: 3, StartTime: base, EndTime: day(1)},
		},
		needsSort: true,
		earliest:  base,
		latest:    day(1),
	}

	examples := []struct {
		T            time.Time
		ShardGroupID uint64 // 0 will indicate we don't expect a shard group
	}{
		{T: base.Add(-time.Minute), ShardGroupID: 0}, // Before any SG
		{T: base, ShardGroupID: 3},
		{T: hour(5), ShardGroupID: 1},
		{T: hour(7).Add(-time.Minute), ShardGroupID: 2},
		{T: hour(8), ShardGroupID: 3},
		{T: day(2), ShardGroupID: 0}, // No matching SG
	}

	for _, example := range examples {
		t.Run(example.T.String(), func(t *testing.T) {
			sg := list.ShardGroupAt(example.T)
			var id uint64
			if sg != nil {
				id = sg.ID
			}
			require.Equal(t, example.ShardGroupID, id)
		})
	}
}
