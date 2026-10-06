package coordinator_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

// newLimitPushdownFixture builds a LocalShardMapper over three disjoint,
// ascending-time shard groups (ids {1}, {2}, {3}), each able to produce up
// to two float points. It returns the shard mapper along with a log of
// which shard-id sets had CreateIterator actually invoked on them (as
// opposed to merely being constructed via ShardGroup, which always happens
// for every group while mapping shards).
func newLimitPushdownFixture(t *testing.T, pointsPerGroup map[string][]query.FloatPoint) (*coordinator.LocalShardMapper, *[]string) {
	t.Helper()

	var metaClient MetaClient
	metaClient.ShardGroupsByTimeRangeFn = func(database, policy string, min, max time.Time) ([]meta.ShardGroupInfo, error) {
		return []meta.ShardGroupInfo{
			{
				ID:        1,
				StartTime: time.Unix(0, 0),
				EndTime:   time.Unix(100, 0),
				Shards:    []meta.ShardInfo{{ID: 1, Owners: []meta.ShardOwner{{NodeID: 0}}}},
			},
			{
				ID:        2,
				StartTime: time.Unix(100, 0),
				EndTime:   time.Unix(200, 0),
				Shards:    []meta.ShardInfo{{ID: 2, Owners: []meta.ShardOwner{{NodeID: 0}}}},
			},
			{
				ID:        3,
				StartTime: time.Unix(200, 0),
				EndTime:   time.Unix(300, 0),
				Shards:    []meta.ShardInfo{{ID: 3, Owners: []meta.ShardOwner{{NodeID: 0}}}},
			},
		}, nil
	}

	var createIteratorLog []string
	key := func(ids []uint64) string {
		s := ""
		for _, id := range ids {
			if s != "" {
				s += ","
			}
			s += string(rune('0' + id))
		}
		return s
	}

	tsdbStore := &internal.TSDBStoreMock{}
	tsdbStore.ShardGroupFn = func(ids []uint64) tsdb.ShardGroup {
		k := key(ids)
		var sh MockShard
		sh.CreateIteratorFn = func(ctx context.Context, measurement *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
			createIteratorLog = append(createIteratorLog, k)
			pts := pointsPerGroup[k]
			if len(pts) == 0 {
				return nil, nil
			}
			return &FloatIterator{Points: pts}, nil
		}
		return &sh
	}

	shardMapper := &coordinator.LocalShardMapper{
		MetaClient: &metaClient,
		TSDBStore:  tsdbStore,
	}
	return shardMapper, &createIteratorLog
}

func limitPushdownMeasurement() *influxql.Measurement {
	return &influxql.Measurement{
		Database:        "db0",
		RetentionPolicy: "rp0",
		Name:            "cpu",
	}
}

func drainFloatIterator(t *testing.T, itr query.Iterator) []query.FloatPoint {
	t.Helper()
	fitr, ok := itr.(query.FloatIterator)
	if !ok {
		t.Fatalf("expected a FloatIterator, got %T", itr)
	}
	var pts []query.FloatPoint
	for {
		p, err := fitr.Next()
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if p == nil {
			break
		}
		pts = append(pts, *p)
	}
	return pts
}

func TestLocalShardMapping_CreateIterator_LimitPushdown_Ascending(t *testing.T) {
	shardMapper, log := newLimitPushdownFixture(t, map[string][]query.FloatPoint{
		"1": {{Name: "cpu", Time: 1, Value: 1}, {Name: "cpu", Time: 2, Value: 2}},
		"2": {{Name: "cpu", Time: 101, Value: 3}, {Name: "cpu", Time: 102, Value: 4}},
		"3": {{Name: "cpu", Time: 201, Value: 5}, {Name: "cpu", Time: 202, Value: 6}},
	})

	m := limitPushdownMeasurement()
	ic, err := shardMapper.MapShards([]influxql.Source{m}, influxql.TimeRange{}, query.SelectOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer ic.Close()

	itr, err := ic.CreateIterator(context.Background(), m, query.IteratorOptions{
		Limit:               2,
		Ascending:           true,
		GlobalLimitEligible: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	pts := drainFloatIterator(t, itr)
	if len(pts) != 2 {
		t.Fatalf("expected 2 points, got %d", len(pts))
	}

	if got, want := *log, []string{"1"}; !stringSlicesEqual(got, want) {
		t.Fatalf("expected only group 1 to have CreateIterator called, got %#v", got)
	}
}

func TestLocalShardMapping_CreateIterator_LimitPushdown_DescendingWithOffset(t *testing.T) {
	shardMapper, log := newLimitPushdownFixture(t, map[string][]query.FloatPoint{
		"1": {{Name: "cpu", Time: 1, Value: 1}},
		"2": {{Name: "cpu", Time: 101, Value: 2}},
		"3": {{Name: "cpu", Time: 201, Value: 3}},
	})

	m := limitPushdownMeasurement()
	ic, err := shardMapper.MapShards([]influxql.Source{m}, influxql.TimeRange{}, query.SelectOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer ic.Close()

	// Limit+Offset = 2, so group 3 (newest, opened first for DESC) plus
	// group 2 together satisfy the cap; group 1 (oldest) must never open.
	itr, err := ic.CreateIterator(context.Background(), m, query.IteratorOptions{
		Limit:               1,
		Offset:              1,
		Ascending:           false,
		GlobalLimitEligible: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	pts := drainFloatIterator(t, itr)
	if len(pts) != 2 {
		t.Fatalf("expected 2 points, got %d", len(pts))
	}

	if got, want := *log, []string{"3", "2"}; !stringSlicesEqual(got, want) {
		t.Fatalf("expected groups 3 then 2 to have CreateIterator called (newest-first), got %#v", got)
	}
}

func TestLocalShardMapping_CreateIterator_LimitPushdown_Exhaustion(t *testing.T) {
	shardMapper, log := newLimitPushdownFixture(t, map[string][]query.FloatPoint{
		"1": {{Name: "cpu", Time: 1, Value: 1}},
		"2": {{Name: "cpu", Time: 101, Value: 2}},
		"3": {{Name: "cpu", Time: 201, Value: 3}},
	})

	m := limitPushdownMeasurement()
	ic, err := shardMapper.MapShards([]influxql.Source{m}, influxql.TimeRange{}, query.SelectOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer ic.Close()

	// Limit exceeds the total rows available across all groups.
	itr, err := ic.CreateIterator(context.Background(), m, query.IteratorOptions{
		Limit:               100,
		Ascending:           true,
		GlobalLimitEligible: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	pts := drainFloatIterator(t, itr)
	if len(pts) != 3 {
		t.Fatalf("expected 3 points, got %d", len(pts))
	}

	if got, want := *log, []string{"1", "2", "3"}; !stringSlicesEqual(got, want) {
		t.Fatalf("expected all three groups to have CreateIterator called, got %#v", got)
	}
}

func TestLocalShardMapping_CreateIterator_LimitPushdown_FallbackUnaffected(t *testing.T) {
	tests := []struct {
		name string
		opt  query.IteratorOptions
	}{
		{"group by tag", query.IteratorOptions{Limit: 1, GlobalLimitEligible: true, Dimensions: []string{"host"}}},
		{"slimit set", query.IteratorOptions{Limit: 1, GlobalLimitEligible: true, SLimit: 1}},
		{"not global-limit-eligible (multi-source)", query.IteratorOptions{Limit: 1, GlobalLimitEligible: false}},
		{"no limit", query.IteratorOptions{GlobalLimitEligible: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shardMapper, log := newLimitPushdownFixture(t, map[string][]query.FloatPoint{
				"1,2,3": {{Name: "cpu", Time: 1, Value: 1}},
			})

			m := limitPushdownMeasurement()
			ic, err := shardMapper.MapShards([]influxql.Source{m}, influxql.TimeRange{}, query.SelectOptions{})
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			defer ic.Close()

			if _, err := ic.CreateIterator(context.Background(), m, tt.opt); err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			if got, want := *log, []string{"1,2,3"}; !stringSlicesEqual(got, want) {
				t.Fatalf("expected the unchanged flattened path to be used, got %#v", got)
			}
		})
	}
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
