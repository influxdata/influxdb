package coordinator_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
)

func TestLocalShardMapper(t *testing.T) {
	var metaClient MetaClient
	metaClient.ShardGroupsByTimeRangeFn = func(database, policy string, min, max time.Time) ([]meta.ShardGroupInfo, error) {
		if database != "db0" {
			t.Errorf("unexpected database: %s", database)
		}
		if policy != "rp0" {
			t.Errorf("unexpected retention policy: %s", policy)
		}
		return []meta.ShardGroupInfo{
			{ID: 1, Shards: []meta.ShardInfo{
				{ID: 1, Owners: []meta.ShardOwner{{NodeID: 0}}},
				{ID: 2, Owners: []meta.ShardOwner{{NodeID: 0}}},
			}},
			{ID: 2, Shards: []meta.ShardInfo{
				{ID: 3, Owners: []meta.ShardOwner{{NodeID: 0}}},
				{ID: 4, Owners: []meta.ShardOwner{{NodeID: 0}}},
			}},
		}, nil
	}

	var tsdbStore TSDBStore
	tsdbStore.ShardGroupFn = func(ids []uint64) query.ShardGroup {
		if !reflect.DeepEqual(ids, []uint64{1, 2, 3, 4}) {
			t.Errorf("unexpected shard ids: %#v", ids)
		}

		var sh MockShard
		sh.CreateIteratorFn = func(measurement string, opt influxql.IteratorOptions) (influxql.Iterator, error) {
			if measurement != "cpu" {
				t.Errorf("unexpected measurement: %s", measurement)
			}
			return &FloatIterator{}, nil
		}
		return &sh
	}

	// Initialize the shard mapper.
	shardMapper := &coordinator.LocalShardMapper{
		MetaClient: &metaClient,
		TSDBStore:  &tsdbStore,
	}

	// Normal measurement.
	measurement := &influxql.Measurement{
		Database:        "db0",
		RetentionPolicy: "rp0",
		Name:            "cpu",
	}
	ic, err := shardMapper.MapShards(measurement, &influxql.SelectOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// This should be a LocalShardMapping.
	if _, ok := ic.(*coordinator.LocalShardMapping); !ok {
		t.Fatalf("unexpected mapping type: %T", ic)
	}

	if _, err := ic.CreateIterator(influxql.IteratorOptions{}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}
