package data_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/influxdb/influxdb/data"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/test"
	"github.com/influxdb/influxdb/tsdb"
)

type fakeShardWriter struct {
	ShardWriteFn func(shardID uint64, points []tsdb.Point) (int, error)
}

func (f *fakeShardWriter) WriteShard(shardID uint64, points []tsdb.Point) (int, error) {
	return f.ShardWriteFn(shardID, points)
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

	c := data.Coordinator{MetaStore: ms}
	pr := &data.WritePointsRequest{
		Database:         "mydb",
		RetentionPolicy:  "myrp",
		ConsistencyLevel: data.ConsistencyLevelOne,
	}
	pr.AddPoint("cpu", 1.0, time.Now(), nil)

	var (
		shardMappings *data.ShardMapping
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

	c := data.Coordinator{MetaStore: ms}
	pr := &data.WritePointsRequest{
		Database:         "mydb",
		RetentionPolicy:  "myrp",
		ConsistencyLevel: data.ConsistencyLevelOne,
	}

	// Three points that range over the shardGroup duration (1h) and should map to two
	// distinct shards
	pr.AddPoint("cpu", 1.0, time.Unix(0, 0), nil)
	pr.AddPoint("cpu", 2.0, time.Unix(0, 0).Add(time.Hour), nil)
	pr.AddPoint("cpu", 3.0, time.Unix(0, 0).Add(time.Hour+time.Second), nil)

	var (
		shardMappings *data.ShardMapping
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
		consistency data.ConsistencyLevel
		dnWrote     int
		dnErr       error
		cwWrote     int
		cwErr       error
		expErr      error
	}{
		// Consistency one
		{
			name:        "write one local",
			consistency: data.ConsistencyLevelOne,
			dnWrote:     1,
			dnErr:       nil,
			cwWrote:     0,
			cwErr:       nil,
			expErr:      nil,
		},
		{
			name:        "write one remote",
			consistency: data.ConsistencyLevelOne,
			dnWrote:     0,
			dnErr:       nil,
			cwWrote:     1,
			cwErr:       nil,
			expErr:      nil,
		},
		{
			name:        "write one not local",
			consistency: data.ConsistencyLevelOne,
			dnWrote:     0,
			dnErr:       fmt.Errorf("random data node error"),
			cwWrote:     0,
			cwErr:       nil,
			expErr:      data.ErrWriteFailed,
		},
		{
			name:        "write one not local, remote success",
			consistency: data.ConsistencyLevelOne,
			dnWrote:     0,
			dnErr:       fmt.Errorf("random data node error"),
			cwWrote:     1,
			cwErr:       nil,
			expErr:      nil,
		},
		{
			name:        "write one not local, remote success",
			consistency: data.ConsistencyLevelOne,
			dnWrote:     0,
			dnErr:       fmt.Errorf("random data node error"),
			cwWrote:     1,
			cwErr:       fmt.Errorf("random remote write error"),
			expErr:      data.ErrPartialWrite,
		},
		// Consistency any
		{
			name:        "write any local",
			consistency: data.ConsistencyLevelAny,
			dnWrote:     1,
			dnErr:       nil,
			cwWrote:     0,
			cwErr:       nil,
			expErr:      nil,
		},
		{
			name:        "write any remote",
			consistency: data.ConsistencyLevelAny,
			dnWrote:     0,
			dnErr:       nil,
			cwWrote:     1,
			cwErr:       nil,
			expErr:      nil,
		},
		{
			name:        "write any not local",
			consistency: data.ConsistencyLevelAny,
			dnWrote:     0,
			dnErr:       fmt.Errorf("random data node error"),
			cwWrote:     0,
			cwErr:       nil,
			expErr:      data.ErrWriteFailed,
		},
		{
			name:        "write any local failed, remote success",
			consistency: data.ConsistencyLevelAny,
			dnWrote:     0,
			dnErr:       fmt.Errorf("random data node error"),
			cwWrote:     1,
			cwErr:       nil,
			expErr:      nil,
		},
		{
			name:        "write any local failed, remote success",
			consistency: data.ConsistencyLevelAny,
			dnWrote:     0,
			dnErr:       fmt.Errorf("random data node error"),
			cwWrote:     1,
			cwErr:       fmt.Errorf("random remote write error"),
			expErr:      data.ErrPartialWrite,
		},

		// Consistency all
		{
			name:        "write all",
			consistency: data.ConsistencyLevelAll,
			dnWrote:     1,
			dnErr:       nil,
			cwWrote:     2,
			cwErr:       nil,
			expErr:      nil,
		},
		{
			name:        "write all, 2/3",
			consistency: data.ConsistencyLevelAll,
			dnWrote:     1,
			dnErr:       nil,
			cwWrote:     1,
			cwErr:       nil,
			expErr:      data.ErrPartialWrite,
		},
		{
			name:        "write all, one error, one success",
			consistency: data.ConsistencyLevelAll,
			dnWrote:     0,
			dnErr:       data.ErrTimeout,
			cwWrote:     1,
			cwErr:       nil,
			expErr:      data.ErrPartialWrite,
		},
		{
			name:        "write all, 1/3",
			consistency: data.ConsistencyLevelAll,
			dnWrote:     0,
			dnErr:       nil,
			cwWrote:     1,
			cwErr:       nil,
			expErr:      data.ErrPartialWrite,
		},
		// Consistency quorum
		{
			name:        "write quorum, 1/3",
			consistency: data.ConsistencyLevelQuorum,
			dnWrote:     1,
			dnErr:       nil,
			cwWrote:     0,
			cwErr:       nil,
			expErr:      data.ErrPartialWrite,
		},
		{
			name:        "write quorum, 2/3, local & remote",
			consistency: data.ConsistencyLevelQuorum,
			dnWrote:     1,
			dnErr:       nil,
			cwWrote:     1,
			cwErr:       nil,
			expErr:      nil,
		},
		{
			name:        "write quorum, 2/3, not local, two remote",
			consistency: data.ConsistencyLevelQuorum,
			dnWrote:     0,
			dnErr:       fmt.Errorf("random data node error"),
			cwWrote:     2,
			cwErr:       nil,
			expErr:      nil,
		},

		// Error write failed
		{
			name:        "no writes succeed",
			consistency: data.ConsistencyLevelOne,
			dnWrote:     0,
			dnErr:       nil,
			cwWrote:     0,
			cwErr:       nil,
			expErr:      data.ErrWriteFailed,
		},
	}

	for _, test := range tests {

		// copy to prevent data race
		theTest := test
		sm := data.NewShardMapping()
		sm.MapPoint(
			meta.ShardInfo{ID: uint64(1), OwnerIDs: []uint64{uint64(1)}},
			tsdb.NewPoint(
				"cpu",
				nil,
				map[string]interface{}{"value": 0.0},
				time.Unix(0, 0),
			))

		// Local data.Node ShardWriter
		dn := &fakeShardWriter{
			ShardWriteFn: func(shardID uint64, points []tsdb.Point) (int, error) {
				return theTest.dnWrote, theTest.dnErr
			},
		}

		// Cluster ShardWriter
		cw := &fakeShardWriter{
			ShardWriteFn: func(shardID uint64, points []tsdb.Point) (int, error) {
				return theTest.cwWrote, theTest.cwErr
			},
		}

		ms := newTestMetaStore()
		c := data.Coordinator{
			MetaStore: ms,
		}
		c.AddShardWriter(dn)
		c.AddShardWriter(cw)

		pr := &data.WritePointsRequest{
			Database:         "mydb",
			RetentionPolicy:  "myrp",
			ConsistencyLevel: test.consistency,
		}

		// Three points that range over the shardGroup duration (1h) and should map to two
		// distinct shards
		pr.AddPoint("cpu", 1.0, time.Unix(0, 0), nil)
		pr.AddPoint("cpu", 2.0, time.Unix(0, 0).Add(time.Hour), nil)
		pr.AddPoint("cpu", 3.0, time.Unix(0, 0).Add(time.Hour+time.Second), nil)

		if err := c.Write(pr); err != test.expErr {
			t.Errorf("Coordinator.Write(): '%s' failed: got %v, exp %v", test.name, err, test.expErr)
		}
	}
}
