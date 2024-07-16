package storage

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/internal"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/stretchr/testify/require"
)

func TestValidateArgs(t *testing.T) {
	type inputParams struct {
		orgID    uint64
		bucketID uint64
		start    int64
		end      int64
	}

	type outputParams struct {
		database string
		rp       string
		start    int64
		end      int64
		err      error
	}

	testCases := []struct {
		desc     string
		store    *Store
		input    inputParams
		expected outputParams
	}{
		{
			desc: "start not < models.MinNanoTime and end not > models.MaxNanoTime",
			store: NewStore(nil, &internal.MetaClientMock{
				DatabaseFn: func(name string) *meta.DatabaseInfo {
					return &meta.DatabaseInfo{
						Name: name,
						RetentionPolicies: []meta.RetentionPolicyInfo{
							{
								Name: meta.DefaultRetentionPolicyName,
							},
						},
					}
				},
			}),
			input: inputParams{
				orgID:    1,
				bucketID: 2,
				start:    models.MinNanoTime - 1,
				end:      models.MaxNanoTime + 1,
			},
			expected: outputParams{
				database: "0000000000000002",
				rp:       meta.DefaultRetentionPolicyName,
				start:    models.MinNanoTime,
				end:      models.MaxNanoTime,
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			database, rp, start, end, err := tC.store.validateArgs(tC.input.orgID, tC.input.bucketID, tC.input.start, tC.input.end)

			require.Equal(t, tC.expected.database, database)
			require.Equal(t, tC.expected.rp, rp)
			require.Equal(t, tC.expected.start, start)
			require.Equal(t, tC.expected.end, end)
			require.Equal(t, tC.expected.err, err)
		})
	}
}

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
