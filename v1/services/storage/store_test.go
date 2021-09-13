package storage

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/stretchr/testify/require"
)

func TestGroupShardsByTime(t *testing.T) {
	tests := []struct {
		name                        string
		shardGroups                 []meta.ShardGroupInfo
		start, end                  int64
		wantInRange, wantNotInRange []uint64
	}{
		{
			name: "all are within the time range",
			shardGroups: []meta.ShardGroupInfo{
				{
					ID:        1,
					StartTime: time.Unix(0, 10),
					EndTime:   time.Unix(0, 12),
					Shards: []meta.ShardInfo{
						{ID: 1},
					},
				},
				{
					ID:        2,
					StartTime: time.Unix(0, 11),
					EndTime:   time.Unix(0, 13),
					Shards: []meta.ShardInfo{
						{ID: 2},
					},
				},
			},
			start:          0,
			end:            15,
			wantInRange:    []uint64{1, 2},
			wantNotInRange: []uint64{},
		},
		{
			name: "none are within the time range",
			shardGroups: []meta.ShardGroupInfo{
				{
					ID:        1,
					StartTime: time.Unix(0, 10),
					EndTime:   time.Unix(0, 12),
					Shards: []meta.ShardInfo{
						{ID: 1},
					},
				},
				{
					ID:        2,
					StartTime: time.Unix(0, 11),
					EndTime:   time.Unix(0, 13),
					Shards: []meta.ShardInfo{
						{ID: 2},
					},
				},
			},
			start:          20,
			end:            25,
			wantInRange:    []uint64{},
			wantNotInRange: []uint64{1, 2},
		},
		{
			name: "some are in the time range; some are not",
			shardGroups: []meta.ShardGroupInfo{
				{
					ID:        1,
					StartTime: time.Unix(0, 10),
					EndTime:   time.Unix(0, 12),
					Shards: []meta.ShardInfo{
						{ID: 1},
					},
				},
				{
					ID:        2,
					StartTime: time.Unix(0, 12),
					EndTime:   time.Unix(0, 14),
					Shards: []meta.ShardInfo{
						{ID: 2},
					},
				},
			},
			start:          11,
			end:            15,
			wantInRange:    []uint64{2},
			wantNotInRange: []uint64{1},
		},
		{
			name: "time ranges are inclusive",
			shardGroups: []meta.ShardGroupInfo{
				{
					ID:        1,
					StartTime: time.Unix(0, 10),
					EndTime:   time.Unix(0, 12),
					Shards: []meta.ShardInfo{
						{ID: 1},
					},
				},
				{
					ID:        2,
					StartTime: time.Unix(0, 12),
					EndTime:   time.Unix(0, 14),
					Shards: []meta.ShardInfo{
						{ID: 2},
					},
				},
			},
			start:          10,
			end:            14,
			wantInRange:    []uint64{1, 2},
			wantNotInRange: []uint64{},
		},
	}

	for _, tt := range tests {
		gotInRange, gotNotInRange := groupShardsByTime(tt.shardGroups, tt.start, tt.end)
		require.Equal(t, tt.wantInRange, gotInRange)
		require.Equal(t, tt.wantNotInRange, gotNotInRange)
	}
}
