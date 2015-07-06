package tsdb

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
)

var sID0 = uint64(1)
var sID1 = uint64(1)
var sgID = uint64(2)
var nID = uint64(42)

func TestWritePointsAndExecuteTwoShards(t *testing.T) {
	store, planner := testStoreAndPlanner()
	defer os.RemoveAll(store.path)

	// Write two points across shards.
	pt1time := time.Unix(1, 0).UTC()
	if err := store.WriteToShard(sID0, []Point{NewPoint(
		"cpu",
		map[string]string{"host": "serverA", "region": "us-east"},
		map[string]interface{}{"value": 100},
		pt1time,
	)}); err != nil {
		t.Fatalf(err.Error())
	}
	pt2time := time.Unix(2, 0).UTC()
	if err := store.WriteToShard(sID1, []Point{NewPoint(
		"cpu",
		map[string]string{"host": "serverB", "region": "us-east"},
		map[string]interface{}{"value": 200},
		pt2time,
	)}); err != nil {
		t.Fatalf(err.Error())
	}

	var tests = []struct {
		stmt     string
		expected string
	}{
		{
			stmt:     `SELECT value FROM cpu`,
			expected: `[{"name":"cpu","columns":["time","value"],"values":[["1970-01-01T00:00:01Z",100],["1970-01-01T00:00:02Z",200]]}]`,
		},
		{
			stmt:     `SELECT value FROM cpu LIMIT 1`,
			expected: `[{"name":"cpu","columns":["time","value"],"values":[["1970-01-01T00:00:01Z",100]]}]`,
		},
		{
			stmt:     `SELECT value FROM cpu WHERE host='serverA'`,
			expected: `[{"name":"cpu","columns":["time","value"],"values":[["1970-01-01T00:00:01Z",100]]}]`,
		},
		{
			stmt:     `SELECT value FROM cpu WHERE host='serverB'`,
			expected: `[{"name":"cpu","columns":["time","value"],"values":[["1970-01-01T00:00:02Z",200]]}]`,
		},
		{
			stmt:     `SELECT value FROM cpu WHERE host='serverC'`,
			expected: `null`,
		},
		{
			stmt:     `SELECT value FROM cpu GROUP BY host`,
			expected: `[{"name":"cpu","tags":{"host":"serverA"},"columns":["time","value"],"values":[["1970-01-01T00:00:01Z",100]]},{"name":"cpu","tags":{"host":"serverB"},"columns":["time","value"],"values":[["1970-01-01T00:00:02Z",200]]}]`,
		},
		{
			stmt:     `SELECT value FROM cpu GROUP BY region`,
			expected: `[{"name":"cpu","tags":{"region":"us-east"},"columns":["time","value"],"values":[["1970-01-01T00:00:01Z",100],["1970-01-01T00:00:02Z",200]]}]`,
		},
		{
			stmt:     `SELECT value FROM cpu GROUP BY host,region`,
			expected: `[{"name":"cpu","tags":{"host":"serverA","region":"us-east"},"columns":["time","value"],"values":[["1970-01-01T00:00:01Z",100]]},{"name":"cpu","tags":{"host":"serverB","region":"us-east"},"columns":["time","value"],"values":[["1970-01-01T00:00:02Z",200]]}]`,
		},
		{
			stmt:     `SELECT value FROM cpu WHERE host='serverA' GROUP BY host`,
			expected: `[{"name":"cpu","tags":{"host":"serverA"},"columns":["time","value"],"values":[["1970-01-01T00:00:01Z",100]]}]`,
		},
	}

	for _, tt := range tests {
		executor, err := planner.Plan(mustParseSelectStatement(tt.stmt), 100000)
		if err != nil {
			t.Fatalf("failed to plan query: %s", err.Error())
		}
		got := executeAndGetResults(executor)
		if got != tt.expected {
			t.Fatalf("Test %s\nexp: %s\ngot: %s\n", tt.stmt, tt.expected, got)
		}
	}
}

type testQEMetastore struct {
}

func (t *testQEMetastore) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error) {
	return []meta.ShardGroupInfo{
		{
			ID:        sgID,
			StartTime: time.Now().Add(-time.Hour),
			EndTime:   time.Now().Add(time.Hour),
			Shards: []meta.ShardInfo{
				{
					ID:       uint64(sID0),
					OwnerIDs: []uint64{nID},
				},
			},
		},
		{
			ID:        sgID,
			StartTime: time.Now().Add(-2 * time.Hour),
			EndTime:   time.Now().Add(-time.Hour),
			Shards: []meta.ShardInfo{
				{
					ID:       uint64(sID1),
					OwnerIDs: []uint64{nID},
				},
			},
		},
	}, nil
}

func (t *testQEMetastore) NodeID() uint64 { return nID }

func testStoreAndPlanner() (*Store, *Planner) {
	path, _ := ioutil.TempDir("", "")

	store := NewStore(path)
	err := store.Open()
	if err != nil {
		panic(err)
	}
	database := "foo"
	retentionPolicy := "bar"
	store.CreateShard(database, retentionPolicy, sID0)
	store.CreateShard(database, retentionPolicy, sID1)

	planner := NewPlanner(store)
	planner.MetaStore = &testQEMetastore{}

	return store, planner
}

func executeAndGetResults(executor Executor) string {
	ch := executor.Execute()

	var rows []*influxql.Row
	for r := range ch {
		rows = append(rows, r)
	}
	return string(mustMarshalJSON(rows))
}
