package cluster_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/test"
	"github.com/influxdb/influxdb/tsdb"
)

type fakeShardWriter struct {
	ShardWriteFn func(shardID, nodeID uint64, points []tsdb.Point) error
}

func (f *fakeShardWriter) Write(shardID, nodeID uint64, points []tsdb.Point) error {
	return f.ShardWriteFn(shardID, nodeID, points)
}

type fakeStore struct {
	WriteFn func(shardID uint64, points []tsdb.Point) error
}

func (f *fakeStore) WriteToShard(shardID uint64, points []tsdb.Point) error {
	return f.WriteFn(shardID, points)
}

func newTestMetaStore() *test.MetaStore {
	ms := &test.MetaStore{}
	rp := test.NewRetentionPolicy("myp", time.Hour, 3)
	test.AttachShardGroupInfo(rp, []uint64{1, 2, 3})
	test.AttachShardGroupInfo(rp, []uint64{1, 2, 3})

	ms.RetentionPolicyFn = func(db, retentionPolicy string) (*meta.RetentionPolicyInfo, error) {
		return rp, nil
	}

	ms.CreateShardGroupIfNotExistsFn = func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
		for i, sg := range rp.ShardGroups {
			if timestamp.Equal(sg.StartTime) || timestamp.After(sg.StartTime) && timestamp.Before(sg.EndTime) {
				return &rp.ShardGroups[i], nil
			}
		}
		panic("should not get here")
	}
	return ms
}

// TestCoordinatorEnsureShardMappingOne tests that a single point maps to
// a single shard
func TestCoordinatorEnsureShardMappingOne(t *testing.T) {
	ms := test.MetaStore{}
	rp := test.NewRetentionPolicy("myp", time.Hour, 3)

	ms.RetentionPolicyFn = func(db, retentionPolicy string) (*meta.RetentionPolicyInfo, error) {
		return rp, nil
	}

	ms.CreateShardGroupIfNotExistsFn = func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
		return &rp.ShardGroups[0], nil
	}

	c := cluster.Coordinator{MetaStore: ms}
	pr := &cluster.WritePointsRequest{
		Database:         "mydb",
		RetentionPolicy:  "myrp",
		ConsistencyLevel: cluster.ConsistencyLevelOne,
	}
	pr.AddPoint("cpu", 1.0, time.Now(), nil)

	var (
		shardMappings *cluster.ShardMapping
		err           error
	)
	if shardMappings, err = c.MapShards(pr); err != nil {
		t.Fatalf("unexpected an error: %v", err)
	}

	if exp := 1; len(shardMappings.Points) != exp {
		t.Errorf("MapShards() len mismatch. got %v, exp %v", len(shardMappings.Points), exp)
	}
}

// TestCoordinatorEnsureShardMappingMultiple tests that MapShards maps multiple points
// across shard group boundaries to multiple shards
func TestCoordinatorEnsureShardMappingMultiple(t *testing.T) {
	ms := test.MetaStore{}
	rp := test.NewRetentionPolicy("myp", time.Hour, 3)
	test.AttachShardGroupInfo(rp, []uint64{1, 2, 3})
	test.AttachShardGroupInfo(rp, []uint64{1, 2, 3})

	ms.RetentionPolicyFn = func(db, retentionPolicy string) (*meta.RetentionPolicyInfo, error) {
		return rp, nil
	}

	ms.CreateShardGroupIfNotExistsFn = func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
		for i, sg := range rp.ShardGroups {
			if timestamp.Equal(sg.StartTime) || timestamp.After(sg.StartTime) && timestamp.Before(sg.EndTime) {
				return &rp.ShardGroups[i], nil
			}
		}
		panic("should not get here")
	}

	c := cluster.Coordinator{MetaStore: ms}
	pr := &cluster.WritePointsRequest{
		Database:         "mydb",
		RetentionPolicy:  "myrp",
		ConsistencyLevel: cluster.ConsistencyLevelOne,
	}

	// Three points that range over the shardGroup duration (1h) and should map to two
	// distinct shards
	pr.AddPoint("cpu", 1.0, time.Unix(0, 0), nil)
	pr.AddPoint("cpu", 2.0, time.Unix(0, 0).Add(time.Hour), nil)
	pr.AddPoint("cpu", 3.0, time.Unix(0, 0).Add(time.Hour+time.Second), nil)

	var (
		shardMappings *cluster.ShardMapping
		err           error
	)
	if shardMappings, err = c.MapShards(pr); err != nil {
		t.Fatalf("unexpected an error: %v", err)
	}

	if exp := 2; len(shardMappings.Points) != exp {
		t.Errorf("MapShards() len mismatch. got %v, exp %v", len(shardMappings.Points), exp)
	}

	for _, points := range shardMappings.Points {
		// First shard shoud have 1 point w/ first point added
		if len(points) == 1 && points[0].Time() != pr.Points[0].Time() {
			t.Fatalf("MapShards() value mismatch. got %v, exp %v", points[0].Time(), pr.Points[0].Time())
		}

		// Second shard shoud have the last two points added
		if len(points) == 2 && points[0].Time() != pr.Points[1].Time() {
			t.Fatalf("MapShards() value mismatch. got %v, exp %v", points[0].Time(), pr.Points[1].Time())
		}

		if len(points) == 2 && points[1].Time() != pr.Points[2].Time() {
			t.Fatalf("MapShards() value mismatch. got %v, exp %v", points[1].Time(), pr.Points[2].Time())
		}
	}
}

func TestCoordinatorWrite(t *testing.T) {

	tests := []struct {
		name        string
		consistency cluster.ConsistencyLevel

		// the responses returned by each shard write call.  node ID 1 = pos 0
		err    []error
		expErr error
	}{
		// Consistency one
		{
			name:        "write one success",
			consistency: cluster.ConsistencyLevelOne,
			err:         []error{nil, nil, nil},
			expErr:      nil,
		},
		{
			name:        "write one fail",
			consistency: cluster.ConsistencyLevelOne,
			err:         []error{fmt.Errorf("a failure"), fmt.Errorf("a failure"), fmt.Errorf("a failure")},
			expErr:      cluster.ErrWriteFailed,
		},

		// Consistency any
		{
			name:        "write any success",
			consistency: cluster.ConsistencyLevelAny,
			err:         []error{nil, nil, nil},
			expErr:      nil,
		},
		{
			name:        "write any failure",
			consistency: cluster.ConsistencyLevelAny,
			err:         []error{fmt.Errorf("a failure"), fmt.Errorf("a failure"), fmt.Errorf("a failure")},
			expErr:      cluster.ErrWriteFailed,
		},

		// Consistency all
		{
			name:        "write all success",
			consistency: cluster.ConsistencyLevelAll,
			err:         []error{nil, nil, nil},
			expErr:      nil,
		},
		{
			name:        "write all, 2/3, partial write",
			consistency: cluster.ConsistencyLevelAll,
			err:         []error{nil, fmt.Errorf("a failure"), nil},
			expErr:      cluster.ErrPartialWrite,
		},
		{
			name:        "write all, 1/3 (failure)",
			consistency: cluster.ConsistencyLevelAll,
			err:         []error{nil, fmt.Errorf("a failure"), fmt.Errorf("a failure")},
			expErr:      cluster.ErrPartialWrite,
		},

		// Consistency quorum
		{
			name:        "write quorum, 1/3 failure",
			consistency: cluster.ConsistencyLevelQuorum,
			err:         []error{fmt.Errorf("a failure"), fmt.Errorf("a failure"), nil},
			expErr:      cluster.ErrPartialWrite,
		},
		{
			name:        "write quorum, 2/3 success",
			consistency: cluster.ConsistencyLevelQuorum,
			err:         []error{nil, nil, fmt.Errorf("a failure")},
			expErr:      nil,
		},
		{
			name:        "write quorum, 3/3 success",
			consistency: cluster.ConsistencyLevelQuorum,
			err:         []error{nil, nil, nil},
			expErr:      nil,
		},

		// Error write failed
		{
			name:        "no writes succeed",
			consistency: cluster.ConsistencyLevelOne,
			err:         []error{fmt.Errorf("a failure"), fmt.Errorf("a failure"), fmt.Errorf("a failure")},
			expErr:      cluster.ErrWriteFailed,
		},
	}

	for _, test := range tests {

		pr := &cluster.WritePointsRequest{
			Database:         "mydb",
			RetentionPolicy:  "myrp",
			ConsistencyLevel: test.consistency,
		}

		// Three points that range over the shardGroup duration (1h) and should map to two
		// distinct shards
		pr.AddPoint("cpu", 1.0, time.Unix(0, 0), nil)
		pr.AddPoint("cpu", 2.0, time.Unix(0, 0).Add(time.Hour), nil)
		pr.AddPoint("cpu", 3.0, time.Unix(0, 0).Add(time.Hour+time.Second), nil)

		// copy to prevent data race
		theTest := test
		sm := cluster.NewShardMapping()
		sm.MapPoint(
			&meta.ShardInfo{ID: uint64(1), OwnerIDs: []uint64{uint64(1), uint64(2), uint64(3)}},
			pr.Points[0])
		sm.MapPoint(
			&meta.ShardInfo{ID: uint64(2), OwnerIDs: []uint64{uint64(1), uint64(2), uint64(3)}},
			pr.Points[1])
		sm.MapPoint(
			&meta.ShardInfo{ID: uint64(2), OwnerIDs: []uint64{uint64(1), uint64(2), uint64(3)}},
			pr.Points[2])

		// Local cluster.Node ShardWriter
		// lock on the write increment since these functions get called in parallel
		var mu sync.Mutex
		dn := &fakeShardWriter{
			ShardWriteFn: func(shardID, nodeID uint64, points []tsdb.Point) error {
				mu.Lock()
				defer mu.Unlock()
				return theTest.err[int(nodeID)-1]
			},
		}

		store := &fakeStore{
			WriteFn: func(shardID uint64, points []tsdb.Point) error {
				mu.Lock()
				defer mu.Unlock()
				return theTest.err[0]
			},
		}

		ms := newTestMetaStore()
		c := cluster.Coordinator{
			MetaStore:     ms,
			ClusterWriter: dn,
			Store:         store,
		}

		if err := c.Write(pr); err != test.expErr {
			t.Errorf("Coordinator.Write(): '%s' failed: got %v, exp %v", test.name, err, test.expErr)
		}
	}
}
