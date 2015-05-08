package influxdb_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/test"
)

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
		shardMappings influxdb.ShardMapping
		err           error
	)
	if shardMappings, err = c.MapShards(pr); err != nil {
		t.Fatalf("unexpected an error: %v", err)
	}

	if exp := 1; len(shardMappings) != exp {
		t.Errorf("MapShards() len mismatch. got %v, exp %v", len(shardMappings), exp)
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
		shardMappings influxdb.ShardMapping
		err           error
	)
	if shardMappings, err = c.MapShards(pr); err != nil {
		t.Fatalf("unexpected an error: %v", err)
	}

	if exp := 2; len(shardMappings) != exp {
		t.Errorf("MapShards() len mismatch. got %v, exp %v", len(shardMappings), exp)
	}

	for _, points := range shardMappings {
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
