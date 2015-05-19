package influxdb_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/data"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/test"
)

type fakeShardWriter struct {
	ShardWriteFn func(shardID uint64, points []data.Point) (int, error)
}

func (f *fakeShardWriter) WriteShard(shardID uint64, points []data.Point) (int, error) {
	return f.ShardWriteFn(shardID, points)
}

func newTestMetaStore() meta.Store {
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
	return ms
}

func TestCoordinatorWriteOne(t *testing.T) {
	t.Skip("later")
	ms := test.MetaStore{}
	ms.RetentionPolicyFn = func(db, rp string) (*meta.RetentionPolicyInfo, error) {
		return nil, fmt.Errorf("boom!")
	}
	c := influxdb.Coordinator{MetaStore: ms}

	pr := &influxdb.WritePointsRequest{
		Database:         "mydb",
		RetentionPolicy:  "myrp",
		ConsistencyLevel: influxdb.ConsistencyLevelOne,
	}
	pr.AddPoint("cpu", 1.0, time.Now(), nil)

	if err := c.Write(pr); err != nil {
		t.Fatalf("Coordinator.Write() failed: %v", err)
	}
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

	c := influxdb.Coordinator{MetaStore: ms}
	pr := &influxdb.WritePointsRequest{
		Database:         "mydb",
		RetentionPolicy:  "myrp",
		ConsistencyLevel: influxdb.ConsistencyLevelOne,
	}
	pr.AddPoint("cpu", 1.0, time.Now(), nil)

	var (
		shardMappings *influxdb.ShardMapping
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

	c := influxdb.Coordinator{MetaStore: ms}
	pr := &influxdb.WritePointsRequest{
		Database:         "mydb",
		RetentionPolicy:  "myrp",
		ConsistencyLevel: influxdb.ConsistencyLevelOne,
	}

	// Three points that range over the shardGroup duration (1h) and should map to two
	// distinct shards
	pr.AddPoint("cpu", 1.0, time.Unix(0, 0), nil)
	pr.AddPoint("cpu", 2.0, time.Unix(0, 0).Add(time.Hour), nil)
	pr.AddPoint("cpu", 3.0, time.Unix(0, 0).Add(time.Hour+time.Second), nil)

	var (
		shardMappings *influxdb.ShardMapping
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
		if len(points) == 1 && points[0].Time != pr.Points[0].Time {
			t.Fatalf("MapShards() value mismatch. got %v, exp %v", points[0].Time, pr.Points[0].Time)
		}

		// Second shard shoud have the last two points added
		if len(points) == 2 && points[0].Time != pr.Points[1].Time {
			t.Fatalf("MapShards() value mismatch. got %v, exp %v", points[0].Time, pr.Points[1].Time)
		}

		if len(points) == 2 && points[1].Time != pr.Points[2].Time {
			t.Fatalf("MapShards() value mismatch. got %v, exp %v", points[1].Time, pr.Points[2].Time)
		}
	}
}

func TestCoordinatorWrite(t *testing.T) {

	tests := []struct {
		name        string
		consistency influxdb.ConsistencyLevel
		dnWrote     int
		dnErr       error
		cwWrote     int
		cwErr       error
		expErr      error
	}{
		// Consistency one
		{
			name:        "write one local",
			consistency: influxdb.ConsistencyLevelOne,
			dnWrote:     1,
			dnErr:       nil,
			cwWrote:     0,
			cwErr:       nil,
			expErr:      nil,
		},
		{
			name:        "write one remote",
			consistency: influxdb.ConsistencyLevelOne,
			dnWrote:     0,
			dnErr:       nil,
			cwWrote:     1,
			cwErr:       nil,
			expErr:      nil,
		},
		{
			name:        "write one not local",
			consistency: influxdb.ConsistencyLevelOne,
			dnWrote:     0,
			dnErr:       influxdb.ErrShardNotLocal,
			cwWrote:     0,
			cwErr:       nil,
			expErr:      influxdb.ErrWriteFailed,
		},
		{
			name:        "write one not local, remote success",
			consistency: influxdb.ConsistencyLevelOne,
			dnWrote:     0,
			dnErr:       influxdb.ErrShardNotLocal,
			cwWrote:     1,
			cwErr:       nil,
			expErr:      nil,
		},
		{
			name:        "write one not local, remote success",
			consistency: influxdb.ConsistencyLevelOne,
			dnWrote:     0,
			dnErr:       influxdb.ErrShardNotLocal,
			cwWrote:     1,
			cwErr:       influxdb.ErrShardNotFound,
			expErr:      influxdb.ErrShardNotFound,
		},
		// Consistency any
		{
			name:        "write any local",
			consistency: influxdb.ConsistencyLevelAny,
			dnWrote:     1,
			dnErr:       nil,
			cwWrote:     0,
			cwErr:       nil,
			expErr:      nil,
		},
		{
			name:        "write any remote",
			consistency: influxdb.ConsistencyLevelAny,
			dnWrote:     0,
			dnErr:       nil,
			cwWrote:     1,
			cwErr:       nil,
			expErr:      nil,
		},
		{
			name:        "write any not local",
			consistency: influxdb.ConsistencyLevelAny,
			dnWrote:     0,
			dnErr:       influxdb.ErrShardNotLocal,
			cwWrote:     0,
			cwErr:       nil,
			expErr:      influxdb.ErrWriteFailed,
		},
		{
			name:        "write any not local, remote success",
			consistency: influxdb.ConsistencyLevelAny,
			dnWrote:     0,
			dnErr:       influxdb.ErrShardNotLocal,
			cwWrote:     1,
			cwErr:       nil,
			expErr:      nil,
		},
		{
			name:        "write any not local, remote success",
			consistency: influxdb.ConsistencyLevelAny,
			dnWrote:     0,
			dnErr:       influxdb.ErrShardNotLocal,
			cwWrote:     1,
			cwErr:       influxdb.ErrShardNotFound,
			expErr:      influxdb.ErrShardNotFound,
		},

		// Consistency all
		{
			name:        "write all",
			consistency: influxdb.ConsistencyLevelAll,
			dnWrote:     1,
			dnErr:       nil,
			cwWrote:     2,
			cwErr:       nil,
			expErr:      nil,
		},
		{
			name:        "write all, 2/3",
			consistency: influxdb.ConsistencyLevelAll,
			dnWrote:     1,
			dnErr:       nil,
			cwWrote:     1,
			cwErr:       nil,
			expErr:      influxdb.ErrPartialWrite,
		},
		{
			name:        "write all, 1/3",
			consistency: influxdb.ConsistencyLevelAll,
			dnWrote:     0,
			dnErr:       nil,
			cwWrote:     1,
			cwErr:       nil,
			expErr:      influxdb.ErrPartialWrite,
		},
		// Consistency quorum
		{
			name:        "write quorum, 1/3",
			consistency: influxdb.ConsistencyLevelQuorum,
			dnWrote:     1,
			dnErr:       nil,
			cwWrote:     0,
			cwErr:       nil,
			expErr:      influxdb.ErrPartialWrite,
		},
		{
			name:        "write quorum, 2/3, local & remote",
			consistency: influxdb.ConsistencyLevelQuorum,
			dnWrote:     1,
			dnErr:       nil,
			cwWrote:     1,
			cwErr:       nil,
			expErr:      nil,
		},
		{
			name:        "write quorum, 2/3, not local, two remote",
			consistency: influxdb.ConsistencyLevelQuorum,
			dnWrote:     0,
			dnErr:       influxdb.ErrShardNotLocal,
			cwWrote:     2,
			cwErr:       nil,
			expErr:      nil,
		},

		// Error write failed
		{
			name:        "no writes succeed",
			consistency: influxdb.ConsistencyLevelOne,
			dnWrote:     0,
			dnErr:       nil,
			cwWrote:     0,
			cwErr:       nil,
			expErr:      influxdb.ErrWriteFailed,
		},
	}

	for _, test := range tests {

		sm := influxdb.NewShardMapping()
		sm.MapPoint(
			meta.ShardInfo{ID: uint64(1), OwnerIDs: []uint64{uint64(1)}},
			data.Point{
				Name:   "cpu",
				Fields: map[string]interface{}{"value": 0.0},
				Time:   time.Unix(0, 0),
			})

		// Local data.Node ShardWriter
		dn := &fakeShardWriter{
			ShardWriteFn: func(shardID uint64, points []data.Point) (int, error) {
				return test.dnWrote, test.dnErr
			},
		}

		// Cluster ShardWriter
		cw := &fakeShardWriter{
			ShardWriteFn: func(shardID uint64, points []data.Point) (int, error) {
				return test.cwWrote, test.cwErr
			},
		}

		ms := newTestMetaStore()
		c := influxdb.Coordinator{
			MetaStore: ms,
		}
		c.AddShardWriter(dn)
		c.AddShardWriter(cw)

		pr := &influxdb.WritePointsRequest{
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
