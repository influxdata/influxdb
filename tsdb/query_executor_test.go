package tsdb

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
)

func TestWritePointsAndExecuteQuery(t *testing.T) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	store := NewStore(path)
	err := store.Open()
	if err != nil {
		t.Fatalf("error opening store: %s", err.Error())
	}
	database := "foo"
	retentionPolicy := "bar"
	shardID := uint64(1)
	store.CreateShard(database, retentionPolicy, shardID)

	pt := NewPoint(
		"cpu",
		map[string]string{"host": "server"},
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	err = store.WriteToShard(shardID, []Point{pt})
	if err != nil {
		t.Fatalf(err.Error())
	}

	pt.SetTime(time.Unix(2, 3))
	err = store.WriteToShard(shardID, []Point{pt})
	if err != nil {
		t.Fatalf(err.Error())
	}

	executor := NewQueryExecutor(store)
	executor.MetaStore = &testMetastore{}
	executor.Stats = &fakeStats{}

	ch, err := executor.ExecuteQuery(mustParseQuery("select * from cpu"), "foo", 20)
	if err != nil {
		t.Fatalf(err.Error())
	}

	var results []*influxql.Result
	for r := range ch {
		results = append(results, r)
	}

	exepected := `[{"series":[{"name":"cpu","columns":["time","value"],"values":[["1970-01-01T00:00:01.000000002Z",1],["1970-01-01T00:00:02.000000003Z",1]]}]}]`
	got := string(mustMarshalJSON(results))
	if exepected != got {
		t.Fatalf("exp: %s\ngot: %s", exepected, got)
	}
}

type testMetastore struct{}

func (t *testMetastore) Database(name string) (*meta.DatabaseInfo, error) {
	return &meta.DatabaseInfo{
		Name: name,
		DefaultRetentionPolicy: "foo",
		RetentionPolicies: []meta.RetentionPolicyInfo{
			{
				Name: "bar",
				ShardGroups: []meta.ShardGroupInfo{
					{
						ID:        uint64(1),
						StartTime: time.Now().Add(-time.Hour),
						EndTime:   time.Now().Add(time.Hour),
						Shards: []meta.ShardInfo{
							{
								ID:       uint64(1),
								OwnerIDs: []uint64{1},
							},
						},
					},
				},
			},
		},
	}, nil
}

func (t *testMetastore) Databases() ([]meta.DatabaseInfo, error) {
	db, _ := t.Database("foo")
	return []meta.DatabaseInfo{*db}, nil
}

func (t *testMetastore) User(name string) (*meta.UserInfo, error) { return nil, nil }

func (t *testMetastore) AdminUserExists() (bool, error) { return false, nil }

func (t *testMetastore) Authenticate(username, password string) (*meta.UserInfo, error) {
	return nil, nil
}

func (t *testMetastore) RetentionPolicy(database, name string) (rpi *meta.RetentionPolicyInfo, err error) {
	return &meta.RetentionPolicyInfo{
		Name: "bar",
		ShardGroups: []meta.ShardGroupInfo{
			{
				ID:        uint64(1),
				StartTime: time.Now().Add(-time.Hour),
				EndTime:   time.Now().Add(time.Hour),
				Shards: []meta.ShardInfo{
					{
						ID:       uint64(1),
						OwnerIDs: []uint64{1},
					},
				},
			},
		},
	}, nil
}

type fakeStats struct{}

func (f *fakeStats) Add(key string, delta int64)  {}
func (f *fakeStats) Inc(key string)               {}
func (f *fakeStats) Name() string                 { return "test" }
func (f *fakeStats) Walk(fun func(string, int64)) {}

// MustParseQuery parses an InfluxQL query. Panic on error.
func mustParseQuery(s string) *influxql.Query {
	q, err := influxql.NewParser(strings.NewReader(s)).ParseQuery()
	if err != nil {
		panic(err.Error())
	}
	return q
}
