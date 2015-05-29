package influxdb_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/tsdb"
)

// Ensure the server can be successfully opened and closed.
func TestServer_Open(t *testing.T) {
	s := NewServer()
	defer s.Close()
	if err := s.Server.Open(); err != nil {
		t.Fatal(err)
	}
	if err := s.Server.Close(); err != nil {
		t.Fatal(err)
	}
}

// Ensure an error is returned when opening an already open server.
func TestServer_Open_ErrServerOpen(t *testing.T) {
	s := NewServer()
	defer s.Close()

	if err := s.Server.Open(); err != nil {
		t.Fatal(err)
	}
	if err := s.Server.Open(); err != influxdb.ErrServerOpen {
		t.Fatal(err)
	}
}

// Test unuathorized requests logging
func TestServer_UnauthorizedRequests(t *testing.T) {
	s := OpenServer()
	defer s.Close()

	s.SetAuthenticationEnabled(true)

	adminOnlyQuery := &influxql.Query{
		Statements: []influxql.Statement{
			&influxql.DropDatabaseStatement{Name: "foo"},
		},
	}

	e := s.Authorize(nil, adminOnlyQuery, "foo")
	if _, ok := e.(meta.AuthError); !ok {
		t.Fatalf("unexpected error.  expected %v, actual: %v", meta.NewAuthError(""), e)
	}

	// Create normal database user.
	if err := s.CreateUser("user1", "user1", false); err != nil {
		t.Fatal(err)
	}
	ui, err := s.User("user1")
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := s.Authorize(ui, adminOnlyQuery, "foo").(meta.AuthError); !ok {
		t.Fatalf("unexpected error.  expected %v, actual: %v", meta.NewAuthError(""), e)
	}
}

// Test user privilege authorization.
func TestServer_UserPrivilegeAuthorization(t *testing.T) {
	s := OpenServer()
	defer s.Close()

	// Create cluster admin.
	s.CreateUser("admin", "admin", true)
	admin, err := s.User("admin")
	if err != nil {
		t.Fatal(err)
	}

	// Create normal database user.
	s.CreateUser("user1", "user1", false)
	user1, err := s.User("user1")
	if err != nil {
		t.Fatal(err)
	}
	user1.Privileges["foo"] = influxql.ReadPrivilege

	s.Restart()

	// admin user should be authorized for all privileges.
	if !admin.Authorize(influxql.AllPrivileges, "") {
		t.Fatalf("cluster admin doesn't have influxql.AllPrivileges")
	} else if !admin.Authorize(influxql.WritePrivilege, "") {
		t.Fatalf("cluster admin doesn't have influxql.WritePrivilege")
	}

	// Normal user with only read privilege on database foo.
	if !user1.Authorize(influxql.ReadPrivilege, "foo") {
		t.Fatalf("user1 doesn't have influxql.ReadPrivilege on foo")
	} else if user1.Authorize(influxql.WritePrivilege, "foo") {
		t.Fatalf("user1 has influxql.WritePrivilege on foo")
	} else if user1.Authorize(influxql.ReadPrivilege, "bar") {
		t.Fatalf("user1 has influxql.ReadPrivilege on bar")
	} else if user1.Authorize(influxql.AllPrivileges, "") {
		t.Fatalf("user1 is cluster admin")
	}
}

// Test single statement query authorization.
func TestServer_SingleStatementQueryAuthorization(t *testing.T) {
	s := OpenServer()
	defer s.Close()

	// Create cluster admin.
	s.CreateUser("admin", "admin", true)
	admin, err := s.User("admin")
	if err != nil {
		t.Fatal(err)
	}

	// Create normal database user.
	s.CreateUser("user", "user", false)
	user, err := s.User("user")
	if err != nil {
		t.Fatal(err)
	}
	user.Privileges["foo"] = influxql.ReadPrivilege

	s.Restart()

	// Create a query that only cluster admins can run.
	adminOnlyQuery := &influxql.Query{
		Statements: []influxql.Statement{
			&influxql.DropDatabaseStatement{Name: "foo"},
		},
	}

	// Create a query that requires read on one db and write on another.
	readWriteQuery := &influxql.Query{
		Statements: []influxql.Statement{
			&influxql.CreateContinuousQueryStatement{
				Name:     "myquery",
				Database: "foo",
				Source: &influxql.SelectStatement{
					Fields: []*influxql.Field{{Expr: &influxql.Call{Name: "count"}}},
					Target: &influxql.Target{Measurement: &influxql.Measurement{
						Database: "bar",
						Name:     "measure1",
					},
					},
					Sources: []influxql.Source{&influxql.Measurement{Name: "myseries"}},
				},
			},
		},
	}

	// admin user should be authorized to execute any query.
	if err := s.Authorize(admin, adminOnlyQuery, ""); err != nil {
		t.Fatal(err)
	}

	if err := s.Authorize(admin, readWriteQuery, "foo"); err != nil {
		t.Fatal(err)
	}

	// Normal user should not be authorized to execute admin only query.
	if err := s.Authorize(user, adminOnlyQuery, ""); err == nil {
		t.Fatalf("normal user should not be authorized to execute cluster admin level queries")
	}

	// Normal user should not be authorized to execute query that selects into another
	// database which (s)he doesn't have privileges on.
	if err := s.Authorize(user, readWriteQuery, ""); err == nil {
		t.Fatalf("normal user should not be authorized to write to database bar")
	}

	// Grant normal user write privileges on database "bar".
	user.Privileges["bar"] = influxql.WritePrivilege

	//Authorization on the previous query should now succeed.
	if err := s.Authorize(user, readWriteQuery, ""); err != nil {
		t.Fatal(err)
	}
}

// Test multiple statement query authorization.
func TestServer_MultiStatementQueryAuthorization(t *testing.T) {
	s := OpenServer()
	defer s.Close()

	// Create cluster admin.
	s.CreateUser("admin", "admin", true)
	admin, err := s.User("admin")
	if err != nil {
		t.Fatal(err)
	}

	// Create normal database user.
	s.CreateUser("user", "user", false)
	user, err := s.User("user")
	if err != nil {
		t.Fatal(err)
	}
	user.Privileges["foo"] = influxql.ReadPrivilege

	s.Restart()

	// Create a query that requires read for one statement and write for the second.
	readWriteQuery := &influxql.Query{
		Statements: []influxql.Statement{
			// Statement that requires read.
			&influxql.SelectStatement{
				Fields:  []*influxql.Field{{Expr: &influxql.Call{Name: "count"}}},
				Sources: []influxql.Source{&influxql.Measurement{Name: "cpu"}},
			},

			// Statement that requires write.
			&influxql.SelectStatement{
				Fields:  []*influxql.Field{{Expr: &influxql.Call{Name: "count"}}},
				Sources: []influxql.Source{&influxql.Measurement{Name: "cpu"}},
				Target:  &influxql.Target{Measurement: &influxql.Measurement{Name: "tmp"}},
			},
		},
	}

	// Admin should be authorized to execute both statements in the query.
	if err := s.Authorize(admin, readWriteQuery, "foo"); err != nil {
		t.Fatal(err)
	}

	// Normal user with only read privileges should not be authorized to execute both statements.
	if err := s.Authorize(user, readWriteQuery, "foo"); err == nil {
		t.Fatalf("user should not be authorized to execute both statements")
	}
}

// Ensure the server pre-creates shard groups as needed.
/*
func TestServer_PreCreateRetentionPolices(t *testing.T) {
	s := OpenServer()
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{Name: "mypolicy", Duration: 60 * time.Minute})

	// Create two shard groups for the the new retention policy -- 1 which will age out immediately
	// the other in more than an hour.
	s.CreateShardGroupIfNotExists("foo", "mypolicy", time.Now().Add(-2*time.Hour))

	// Check the two shard groups exist.
	sgis, err := s.ShardGroups("foo")
	if err != nil {
		t.Fatal(err)
	} else if len(g) != 1 {
		t.Fatalf("expected 1 shard group but found %d", len(g))
	}

	// Run shard group pre-create.
	s.ShardGroupPreCreate(time.Hour)

	// Ensure enforcement is in effect across restarts.
	s.Restart()

	// Second shard group should now be created.
	g, err = s.ShardGroups("foo")
	if err != nil {
		t.Fatal(err)
	} else if len(g) != 2 {
		t.Fatalf("expected 2 shard group but found %d", len(g))
	}
}
*/

// Ensure the server prohibits a zero check interval for retention policy enforcement.
func TestServer_StartRetentionPolicyEnforcement_ErrZeroInterval(t *testing.T) {
	s := OpenServer()
	defer s.Close()
	if err := s.StartRetentionPolicyEnforcement(time.Duration(0)); err == nil {
		t.Fatal("failed to prohibit retention policies zero check interval")
	}
}

// Ensure the server can support writes of all data types.
func TestServer_WriteAllDataTypes(t *testing.T) {
	s := OpenServer()
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")

	// Write series with one point to the database.
	s.MustWriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("series1", nil, map[string]interface{}{"value": float64(20)}, mustParseTime("2000-01-01T00:00:00Z"))})
	s.MustWriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("series2", nil, map[string]interface{}{"value": int64(30)}, mustParseTime("2000-01-01T00:00:00Z"))})
	s.MustWriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("series3", nil, map[string]interface{}{"value": "baz"}, mustParseTime("2000-01-01T00:00:00Z"))})
	s.MustWriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("series4", nil, map[string]interface{}{"value": true}, mustParseTime("2000-01-01T00:00:00Z"))})
	time.Sleep(time.Millisecond * 100)

	f := func(t *testing.T, database, query, expected string) {
		results := s.executeQuery(MustParseQuery(query), database, nil)
		if res := results.Results[0]; res.Err != nil {
			t.Errorf("unexpected error: %s", res.Err)
		} else if len(res.Series) != 1 {
			t.Errorf("unexpected row count: %d", len(res.Series))
		} else if s := mustMarshalJSON(res); s != expected {
			t.Errorf("unexpected row(0): \nexp: %s\ngot: %s", expected, s)
		}
	}

	f(t, "foo", "SELECT * from series1", `{"series":[{"name":"series1","columns":["time","value"],"values":[["2000-01-01T00:00:00Z",20]]}]}`)
	f(t, "foo", "SELECT * from series2", `{"series":[{"name":"series2","columns":["time","value"],"values":[["2000-01-01T00:00:00Z",30]]}]}`)
	f(t, "foo", "SELECT * from series3", `{"series":[{"name":"series3","columns":["time","value"],"values":[["2000-01-01T00:00:00Z","baz"]]}]}`)
	f(t, "foo", "SELECT * from series4", `{"series":[{"name":"series4","columns":["time","value"],"values":[["2000-01-01T00:00:00Z",true]]}]}`)
}

/*
func TestServer_EnforceRetentionPolices(t *testing.T) {
	s := OpenServer()
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{Name: "mypolicy", Duration: 60 * time.Minute})

	// Create two shard groups for the the new retention policy -- 1 which will age out immediately
	// the other in more than an hour.
	s.CreateShardGroupIfNotExists("foo", "mypolicy", time.Now().Add(-2*time.Hour))
	s.CreateShardGroupIfNotExists("foo", "mypolicy", time.Now().Add(time.Hour))

	// Check the two shard groups exist.
	var g []*influxdb.ShardGroup
	g, err := s.ShardGroups("foo")
	if err != nil {
		t.Fatal(err)
	} else if len(g) != 2 {
		t.Fatalf("expected 2 shard group but found %d", len(g))
	}

	// Run retention enforcement.
	s.EnforceRetentionPolicies()

	// Ensure enforcement is in effect across restarts.
	s.Restart()

	// First shard group should have been removed.
	g, err = s.ShardGroups("foo")
	if err != nil {
		t.Fatal(err)
	} else if len(g) != 1 {
		t.Fatalf("expected 1 shard group but found %d", len(g))
	}
}
*/

// Ensure the server can drop a measurement.
/*
func TestServer_DropMeasurement(t *testing.T) {
	s := OpenServer()
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")
	s.CreateUser("susy", "pass", false)

	// Write series with one point to the database.
	tags := map[string]string{"host": "serverA", "region": "uswest"}
	index, err := s.WriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("cpu", tags, map[string]interface{}{"value": float64(23.2)}, mustParseTime("2000-01-01T00:00:00Z"))})
	if err != nil {
		t.Fatal(err)
	}

	// Ensure measurement exists
	results := s.executeQuery(MustParseQuery(`SHOW MEASUREMENTS`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"measurements","columns":["name"],"values":[["cpu"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	// Ensure series exists
	results = s.executeQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["_id","host","region"],"values":[[1,"serverA","uswest"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	// Drop measurement
	results = s.executeQuery(MustParseQuery(`DROP MEASUREMENT cpu`), "foo", nil)
	if results.Error() != nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}

	results = s.executeQuery(MustParseQuery(`SHOW MEASUREMENTS`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 0 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	results = s.executeQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 0 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{}` {
		t.Fatalf("unexpected row(0): %s", s)
	}
}
*/

// Ensure the server can drop a series.
/*
func TestServer_DropSeries(t *testing.T) {
	s := OpenServer()
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")
	s.CreateUser("susy", "pass", false)

	// Write series with one point to the database.
	tags := map[string]string{"host": "serverA", "region": "uswest"}
	index, err := s.WriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("cpu", tags, map[string]interface{}{"value": float64(23.2)}, mustParseTime("2000-01-01T00:00:00Z"))})

	if err != nil {
		t.Fatal(err)
	}

	// Ensure series exists
	results := s.executeQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["_id","host","region"],"values":[[1,"serverA","uswest"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	// Drop series
	results = s.executeQuery(MustParseQuery(`DROP SERIES FROM cpu`), "foo", nil)
	if results.Error() != nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}

	results = s.executeQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 0 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{}` {
		t.Fatalf("unexpected row(0): %s", s)
	}
}
*/

// Ensure the server can drop a series from measurement when more than one shard exists.
/*
func TestServer_DropSeriesFromMeasurement(t *testing.T) {
	s := OpenServer()
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")
	s.CreateUser("susy", "pass", false)

	// Write series with one point to the database.
	tags := map[string]string{"host": "serverA", "region": "uswest"}
	index, err := s.WriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("cpu", tags, map[string]interface{}{"value": float64(23.2)}, mustParseTime("2000-01-01T00:00:00Z"))})

	if err != nil {
		t.Fatal(err)
	}

	tags = map[string]string{"host": "serverb", "region": "useast"}
	index, err = s.WriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("memory", tags, map[string]interface{}{"value": float64(23465432423)}, mustParseTime("2000-01-02T00:00:00Z"))})

	if err != nil {
		t.Fatal(err)
	}

	// Drop series
	results := s.executeQuery(MustParseQuery(`DROP SERIES FROM memory`), "foo", nil)
	if results.Error() != nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}

	results = s.executeQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["_id","host","region"],"values":[[1,"serverA","uswest"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}
}
*/

// Ensure Drop Series can:
// write to measurement cpu with tags region=uswest host=serverA
// write to measurement cpu with tags region=uswest host=serverB
// drop one of those series
// ensure that the dropped series is gone
// ensure that we can still query: select value from cpu where region=uswest
/*
func TestServer_DropSeriesTagsPreserved(t *testing.T) {
	s := OpenServer()
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")
	s.CreateUser("susy", "pass", false)

	// Write series with one point to the database.
	tags := map[string]string{"host": "serverA", "region": "uswest"}
	index, err := s.WriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("cpu", tags, map[string]interface{}{"value": float64(23.2)}, mustParseTime("2000-01-01T00:00:00Z"))})

	if err != nil {
		t.Fatal(err)
	}

	tags = map[string]string{"host": "serverB", "region": "uswest"}
	index, err = s.WriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("cpu", tags, map[string]interface{}{"value": float64(33.2)}, mustParseTime("2000-01-01T00:00:01Z"))})

	if err != nil {
		t.Fatal(err)
	}

	results := s.executeQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["_id","host","region"],"values":[[1,"serverA","uswest"],[2,"serverB","uswest"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	results = s.executeQuery(MustParseQuery(`DROP SERIES FROM cpu where host='serverA'`), "foo", nil)
	if results.Error() != nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}

	results = s.executeQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["_id","host","region"],"values":[[2,"serverB","uswest"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	results = s.executeQuery(MustParseQuery(`SELECT * FROM cpu where host='serverA'`), "foo", nil)
	if len(results.Results) == 0 {
		t.Fatal("expected results to be non-empty")
	} else if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 0 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	}

	results = s.executeQuery(MustParseQuery(`SELECT * FROM cpu where host='serverB'`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:01Z",33.2]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	results = s.executeQuery(MustParseQuery(`SELECT * FROM cpu where region='uswest'`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:01Z",33.2]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}
}
*/

// Ensure the server respects limit and offset in show series queries
/*
func TestServer_ShowSeriesLimitOffset(t *testing.T) {
	s := OpenServer()
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")

	// Write series with one point to the database.
	s.MustWriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("cpu", map[string]string{"region": "us-east", "host": "serverA"}, map[string]interface{}{"value": float64(20)}, mustParseTime("2000-01-01T00:00:00Z"))})
	s.MustWriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("cpu", map[string]string{"region": "us-east", "host": "serverB"}, map[string]interface{}{"value": float64(30)}, mustParseTime("2000-01-01T00:00:01Z"))})
	s.MustWriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("cpu", map[string]string{"region": "us-west", "host": "serverC"}, map[string]interface{}{"value": float64(100)}, mustParseTime("2000-01-01T00:00:00Z"))})
	s.MustWriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("memory", map[string]string{"region": "us-west", "host": "serverB"}, map[string]interface{}{"value": float64(100)}, mustParseTime("2000-01-01T00:00:00Z"))})
	s.MustWriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("memory", map[string]string{"region": "us-east", "host": "serverA"}, map[string]interface{}{"value": float64(100)}, mustParseTime("2000-01-01T00:00:00Z"))})

	// Select data from the server.
	results := s.executeQuery(MustParseQuery(`SHOW SERIES LIMIT 3 OFFSET 1`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 2 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["_id","host","region"],"values":[[2,"serverB","us-east"],[3,"serverC","us-west"]]},{"name":"memory","columns":["_id","host","region"],"values":[[4,"serverB","us-west"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	// Select data from the server.
	results = s.executeQuery(MustParseQuery(`SHOW SERIES LIMIT 2 OFFSET 4`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"memory","columns":["_id","host","region"],"values":[[5,"serverA","us-east"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	// Select data from the server.
	results = s.executeQuery(MustParseQuery(`SHOW SERIES LIMIT 2 OFFSET 20`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 0 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	}

	// Select data from the server.
	results = s.executeQuery(MustParseQuery(`SHOW SERIES LIMIT 4 OFFSET 0`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 2 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	}

	// Select data from the server.
	results = s.executeQuery(MustParseQuery(`SHOW SERIES LIMIT 20`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 2 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["_id","host","region"],"values":[[1,"serverA","us-east"],[2,"serverB","us-east"],[3,"serverC","us-west"]]},{"name":"memory","columns":["_id","host","region"],"values":[[4,"serverB","us-west"],[5,"serverA","us-east"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}
}
*/

// Ensure the server can stream shards to client
/*
func TestServer_CopyShard(t *testing.T) {
	s := OpenServer()
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")

	// Write series with one point to the database to ensure shard 1 is created.
	s.MustWriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("series1", nil, map[string]interface{}{"value": float64(20)}, time.Unix(0, 0))})
	time.Sleep(time.Millisecond * 100)

	err := s.CopyShard(ioutil.Discard, 1234)
	if err != influxdb.ErrShardNotFound {
		t.Errorf("received unexpected result when requesting non-existing shard: %v", err)
	}

	err = s.CopyShard(ioutil.Discard, 1)
	if err != nil {
		t.Errorf("failed to copy shard 1: %s", err.Error())
	}
}
*/

/* TODO(benbjohnson): Change test to not expose underlying series ids directly.
func TestServer_Measurements(t *testing.T) {
	s := OpenServer()
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{Name: "mypolicy", Duration: 1 * time.Hour})
	s.CreateUser("susy", "pass", false)

	// Write series with one point to the database.
	timestamp := mustParseTime("2000-01-01T00:00:00Z")

	tags := map[string]string{"host": "servera.influx.com", "region": "uswest"}
	values := map[string]interface{}{"value": 23.2}

	index, err := s.WriteSeries("foo", "mypolicy", []tsdb.Point{tsdb.Point{Name: "cpu_load", Tags: tags, Time: timestamp, Fields: values}})
	if err != nil {
		t.Fatal(err)
	} else if err = s.Sync(index); err != nil {
		t.Fatalf("sync error: %s", err)
	}

	expectedMeasurementNames := []string{"cpu_load"}
	expectedSeriesIDs := influxdb.SeriesIDs([]uint32{uint32(1)})
	names := s.MeasurementNames("foo")
	if !reflect.DeepEqual(names, expectedMeasurementNames) {
		t.Fatalf("Mesurements not the same:\n  exp: %s\n  got: %s", expectedMeasurementNames, names)
	}
	ids := s.MeasurementSeriesIDs("foo", "foo")
	if !reflect.DeepEqual(ids, expectedSeriesIDs) {
		t.Fatalf("Series IDs not the same:\n  exp: %v\n  got: %v", expectedSeriesIDs, ids)
	}

	s.Restart()

	names = s.MeasurementNames("foo")
	if !reflect.DeepEqual(names, expectedMeasurementNames) {
		t.Fatalf("Mesurements not the same:\n  exp: %s\n  got: %s", expectedMeasurementNames, names)
	}
	ids = s.MeasurementSeriesIDs("foo", "foo")
	if !reflect.DeepEqual(ids, expectedSeriesIDs) {
		t.Fatalf("Series IDs not the same:\n  exp: %v\n  got: %v", expectedSeriesIDs, ids)
	}
}
*/

// Ensure the server can convert a measurement into its normalized form.
func TestServer_NormalizeMeasurement(t *testing.T) {
	var tests = []struct {
		in         *influxql.Measurement
		db         string // current database
		out        string // normalized string
		err        string // error, if any
		errPattern string // regex pattern
	}{
		{in: &influxql.Measurement{Name: `cpu`}, db: `db0`, out: `"db0"."rp0".cpu`},
		{in: &influxql.Measurement{RetentionPolicy: `rp0`, Name: `cpu`}, db: `db0`, out: `"db0"."rp0".cpu`},
		{in: &influxql.Measurement{Database: `db0`, RetentionPolicy: `rp0`, Name: `cpu`}, db: `db0`, out: `"db0"."rp0".cpu`},
		{in: &influxql.Measurement{Database: `db0`, Name: `cpu`}, db: `db0`, out: `"db0"."rp0".cpu`},

		{in: &influxql.Measurement{}, db: `db0`, err: `invalid measurement`},
		{in: &influxql.Measurement{Database: `no_db`, Name: `cpu`}, db: `db0`, errPattern: `database not found: no_db.*`},
		{in: &influxql.Measurement{Database: `db2`, Name: `cpu`}, db: `db0`, err: `default retention policy not set for: db2`},
		{in: &influxql.Measurement{Database: `db2`, RetentionPolicy: `no_policy`, Name: `cpu`}, db: `db0`, err: `retention policy does not exist: db2.no_policy`},
	}

	// Create server with a variety of databases, retention policies, and measurements
	s := OpenServer()
	defer s.Close()

	// Default database with one policy.
	s.CreateDatabase("db0")
	s.CreateRetentionPolicy("db0", &meta.RetentionPolicyInfo{Name: "rp0", Duration: time.Hour})
	s.SetDefaultRetentionPolicy("db0", "rp0")

	// Another database with two policies.
	s.CreateDatabase("db1")
	s.CreateRetentionPolicy("db1", &meta.RetentionPolicyInfo{Name: "rp1", Duration: time.Hour})
	s.CreateRetentionPolicy("db1", &meta.RetentionPolicyInfo{Name: "rp2", Duration: time.Hour})
	s.SetDefaultRetentionPolicy("db1", "rp1")

	// Another database with no policies.
	s.CreateDatabase("db2")

	// Execute the tests
	for i, tt := range tests {
		var re *regexp.Regexp
		if tt.errPattern != "" {
			re = regexp.MustCompile(tt.errPattern)
		}

		err := s.NormalizeMeasurement(tt.in, tt.db)
		if tt.err != "" && tt.err != errstr(err) {
			t.Errorf("%d. error:\n  exp: %s\n  got: %s\n", i, tt.err, errstr(err))
		} else if re != nil && !re.MatchString(err.Error()) {
			t.Errorf("%d. error:\n  exp: %s\n  got: %s\n", i, tt.errPattern, tt.in.String())
		} else if tt.err == "" && re == nil && tt.in.String() != tt.out {
			t.Errorf("%d. error:\n  exp: %s\n  got: %s\n", i, tt.out, tt.in.String())
		}
	}
}

// Ensure the server can normalize all statements in query.
func TestServer_NormalizeQuery(t *testing.T) {
	var tests = []struct {
		in         string // input query
		db         string // default database
		out        string // output query
		err        string // error, if any
		errPattern string // regex pattern
	}{
		{
			in: `SELECT value FROM cpu`, db: `db0`,
			out: `SELECT value FROM "db0"."rp0".cpu`,
		},

		{
			in: `SELECT value FROM cpu`, db: `no_db`, errPattern: `database not found: no_db.*`,
		},
	}

	// Start server with database & retention policy.
	s := OpenServer()
	defer s.Close()
	s.CreateDatabase("db0")
	s.CreateRetentionPolicy("db0", &meta.RetentionPolicyInfo{Name: "rp0", Duration: time.Hour})
	s.SetDefaultRetentionPolicy("db0", "rp0")

	// Execute the tests
	for i, tt := range tests {
		var re *regexp.Regexp
		if tt.errPattern != "" {
			re = regexp.MustCompile(tt.errPattern)
		}

		out := MustParseQuery(tt.in)
		err := s.NormalizeStatement(out.Statements[0], tt.db)
		if tt.err != "" && tt.err != errstr(err) {
			t.Errorf("%d. error:\n  exp: %s\n  got: %s\n", i, tt.err, errstr(err))
		} else if re != nil && !re.MatchString(err.Error()) {
			t.Errorf("%d. error:\n  exp: %s\n  got: %s\n", i, tt.errPattern, tt.in)
		} else if err == nil && re == nil && tt.out != out.String() {
			t.Errorf("%d. error:\n  exp: %s\n  got: %s\n", i, tt.out, out.String())
		}
	}
}

// Ensure continuous queries run
/*
func TestServer_RunContinuousQueries(t *testing.T) {
	t.Skip("skipped pending fix for issue #2218")
	s := OpenServer()
	defer s.Close()

	// Create the "foo" database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{Name: "raw"}); err != nil {
		t.Fatal(err)
	}
	s.SetDefaultRetentionPolicy("foo", "raw")

	s.RecomputePreviousN = 50
	s.RecomputeNoOlderThan = time.Second
	s.ComputeRunsPerInterval = 5
	s.ComputeNoMoreThan = 2 * time.Millisecond

	// create cq and check
	q := `CREATE CONTINUOUS QUERY myquery ON foo BEGIN SELECT mean(value) INTO cpu_region FROM cpu GROUP BY time(5ms), region END`
	stmt, err := influxql.NewParser(strings.NewReader(q)).ParseStatement()
	if err != nil {
		t.Fatalf("error parsing query %s", err.Error())
	}
	cq := stmt.(*influxql.CreateContinuousQueryStatement)
	if err := s.CreateContinuousQuery(cq); err != nil {
		t.Fatalf("error creating continuous query %s", err.Error())
	}
	if err := s.RunContinuousQueries(); err != nil {
		t.Fatalf("error running cqs when no data exists: %s", err.Error())
	}

	// set a test time in the middle of a 5 second interval that we can work with
	testTime := time.Now().UTC().Round(5 * time.Millisecond)
	if testTime.UnixNano() > time.Now().UnixNano() {
		testTime = testTime.Add(-5 * time.Millisecond)
	}
	testTime.Add(time.Millisecond * 2)

	s.MustWriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("cpu", map[string]string{"region": "us-east"}, map[string]interface{}{"value": float64(30)}, testTime)})
	s.MustWriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("cpu", map[string]string{"region": "us-east"}, map[string]interface{}{"value": float64(20)}, testTime.Add(-time.Millisecond*5))})
	s.MustWriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("cpu", map[string]string{"region": "us-west"}, map[string]interface{}{"value": float64(100)}, testTime)})

	// Run CQs after a period of time
	time.Sleep(time.Millisecond * 50)
	s.RunContinuousQueries()
	// give the CQs time to run
	time.Sleep(time.Millisecond * 100)

	verify := func(num int, exp string) {
		results := s.executeQuery(MustParseQuery(`SELECT mean(mean) FROM cpu_region GROUP BY region`), "foo", nil)
		if res := results.Results[0]; res.Err != nil {
			t.Fatalf("unexpected error verify %d: %s", num, res.Err)
		} else if len(res.Series) != 2 {
			t.Fatalf("unexpected row count on verify %d: %d", num, len(res.Series))
		} else if s := mustMarshalJSON(res); s != exp {
			t.Log("exp: ", exp)
			t.Log("got: ", s)
			t.Fatalf("unexpected row(0) on verify %d: %s", num, s)
		}
	}

	// ensure CQ results were saved
	verify(1, `{"series":[{"name":"cpu_region","tags":{"region":"us-east"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",25]]},{"name":"cpu_region","tags":{"region":"us-west"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",100]]}]}`)

	// ensure that repeated runs don't cause duplicate data
	s.RunContinuousQueries()
	verify(2, `{"series":[{"name":"cpu_region","tags":{"region":"us-east"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",25]]},{"name":"cpu_region","tags":{"region":"us-west"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",100]]}]}`)

	// ensure that data written into a previous window is picked up and the result recomputed.
	time.Sleep(time.Millisecond * 2)
	s.MustWriteSeries("foo", "raw", []tsdb.Point{tsdb.NewPoint("cpu", map[string]string{"region": "us-west"}, map[string]interface{}{"value": float64(50)}, testTime.Add(-time.Millisecond))})
	s.RunContinuousQueries()
	// give CQs time to run
	time.Sleep(time.Millisecond * 100)

	verify(3, `{"series":[{"name":"cpu_region","tags":{"region":"us-east"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",25]]},{"name":"cpu_region","tags":{"region":"us-west"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",75]]}]}`)
}
*/

// Ensure the server can create a snapshot writer.
/*
func TestServer_CreateSnapshotWriter(t *testing.T) {
	s := OpenServer()
	defer s.Close()

	// Write metadata.
	s.CreateDatabase("db")
	s.CreateRetentionPolicy("db", &meta.RetentionPolicyInfo{Name: "raw", Duration: 1 * time.Hour})
	s.CreateUser("susy", "pass", false)

	// Write one point.
	index, err := s.WriteSeries("db", "raw", []tsdb.Point{tsdb.NewPoint("cpu", nil, map[string]interface{}{"value": float64(100)}, mustParseTime("2000-01-01T00:00:00Z"))})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second) // FIX: Sync on shard.

	// Create snapshot writer.
	sw, err := s.CreateSnapshotWriter()
	if err != nil {
		t.Fatal(err)
	}
	defer sw.Close()

	// Verify snapshot is correct.
	//
	// NOTE: Sizes and indices here are subject to change.
	// They are tracked here so that we can see when they change over time.
	if len(sw.Snapshot.Files) != 2 {
		t.Fatalf("unexpected file count: %d", len(sw.Snapshot.Files))
	} else if !reflect.DeepEqual(sw.Snapshot.Files[0], influxdb.SnapshotFile{Name: "meta", Size: 45056, Index: 6}) {
		t.Fatalf("unexpected file(0): %#v", sw.Snapshot.Files[0])
	} else if !reflect.DeepEqual(sw.Snapshot.Files[1], influxdb.SnapshotFile{Name: "shards/1", Size: 24576, Index: index}) {
		t.Fatalf("unexpected file(1): %#v", sw.Snapshot.Files[1])
	}

	// Write to buffer to verify that it does not error or panic.
	var buf bytes.Buffer
	if _, err := sw.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
}
*/

func mustMarshalJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic("marshal: " + err.Error())
	}
	return string(b)
}

// Ensure server returns empty result when no tags exist
func TestServer_ShowTagKeysStatement(t *testing.T) {
	s := OpenServer()
	defer s.Close()

	// Create the "foo" database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{Name: "bar", Duration: time.Hour}); err != nil {
		t.Fatal(err)
	}
	s.SetDefaultRetentionPolicy("foo", "bar")

	q := "SHOW TAG KEYS"
	results := s.executeQuery(MustParseQuery(q), "foo", nil)

	if results.Error() != nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}
	expected := `{}`

	if res := results.Results[0]; res.Err != nil {
		t.Errorf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 0 {
		t.Errorf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != expected {
		t.Errorf("unexpected row(0): \nexp: %s\ngot: %s", expected, s)
	}
}

// Ensure ShowTagKeysStatement returns ErrDatabaseNotFound for nonexistent database
func TestServer_ShowTagKeysStatement_ErrDatabaseNotFound(t *testing.T) {
	s := OpenServer()
	defer s.Close()

	// Create the "foo" database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{Name: "bar", Duration: time.Hour}); err != nil {
		t.Fatal(err)
	}
	s.SetDefaultRetentionPolicy("foo", "bar")

	nonexistentDatabaseName := "baz"

	q := "SHOW TAG KEYS"
	results := s.executeQuery(MustParseQuery(q), nonexistentDatabaseName, nil)

	expectedErr := influxdb.ErrDatabaseNotFound(nonexistentDatabaseName)

	extractMessage := func(e error) string {
		r, _ := regexp.Compile("(.+)\\(.+\\)")
		match := r.FindStringSubmatch(e.Error())[1]
		return match
	}

	if err := results.Error(); extractMessage(err) != extractMessage(expectedErr) {
		t.Fatal(err)
	}
}

// Ensure ShowTagKeysStatement returns ErrMeasurementNotFound for non existent measurement
func TestServer_ShowTagKeysStatement_ErrMeasurementNotFound(t *testing.T) {
	s := OpenServer()
	defer s.Close()

	// Create the "foo" database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{Name: "bar", Duration: time.Hour}); err != nil {
		t.Fatal(err)
	}
	s.SetDefaultRetentionPolicy("foo", "bar")

	nonExistentMeasurement := "src"

	q := fmt.Sprintf(
		"SHOW TAG KEYS FROM %v",
		nonExistentMeasurement,
	)

	results := s.executeQuery(MustParseQuery(q), "foo", nil)

	expectedErr := influxdb.ErrMeasurementNotFound(nonExistentMeasurement)

	extractMessage := func(e error) string {
		r, _ := regexp.Compile("(.+)\\(.+\\)")
		match := r.FindStringSubmatch(e.Error())[1]
		return match
	}

	if err := results.Error(); extractMessage(err) != extractMessage(expectedErr) {
		t.Fatal(err)
	}
}

// Ensure ShowTagKeysStatement returns tag keys when tags exist
func TestServer_ShowTagKeysStatement_TagsExist(t *testing.T) {
	s := OpenServer()
	defer s.Close()

	// Create the "foo" database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{Name: "bar", Duration: time.Hour}); err != nil {
		t.Fatal(err)
	}
	s.SetDefaultRetentionPolicy("foo", "bar")

	// Write series with one point to the database.
	tags := map[string]string{"host": "serverA", "region": "uswest"}
	_, err := s.WriteSeries("foo", "bar", []tsdb.Point{tsdb.NewPoint("cpu", tags, map[string]interface{}{"value": float64(23.2)}, mustParseTime("2000-01-01T00:00:00Z"))})
	if err != nil {
		t.Fatal(err)
	}

	q := "SHOW TAG KEYS FROM cpu"
	results := s.executeQuery(MustParseQuery(q), "foo", nil)

	if results.Error() != nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}
	expected := `{"series":[{"name":"cpu","columns":["tagKey"],"values":[["host"],["region"]]}]}`

	if res := results.Results[0]; res.Err != nil {
		t.Errorf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Errorf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != expected {
		t.Errorf("unexpected row(0): \nexp: %s\ngot: %s", expected, s)
	}
}

// Ensure ShowTagValuesStatement returns tag values
func TestServer_ShowTagValuesStatement_TagsExist(t *testing.T) {
	s := OpenServer()
	defer s.Close()

	// Create the "foo" database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{Name: "bar", Duration: time.Hour}); err != nil {
		t.Fatal(err)
	}
	s.SetDefaultRetentionPolicy("foo", "bar")

	// Write series with one point to the database.
	tags := map[string]string{"host": "serverA", "region": "uswest"}
	_, err := s.WriteSeries("foo", "bar", []tsdb.Point{tsdb.NewPoint("cpu", tags, map[string]interface{}{"value": float64(23.2)}, mustParseTime("2000-01-01T00:00:00Z"))})

	if err != nil {
		t.Fatal(err)
	}

	q := "SHOW TAG VALUES FROM cpu WITH KEY = region"
	results := s.executeQuery(MustParseQuery(q), "foo", nil)

	if results.Error() != nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}
	expected := `{"series":[{"name":"regionTagValues","columns":["region"],"values":[["uswest"]]}]}`

	if res := results.Results[0]; res.Err != nil {
		t.Errorf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Errorf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != expected {
		t.Errorf("unexpected row(0): \nexp: %s\ngot: %s", expected, s)
	}
}

// Ensure ShowTagValuesStatement returns tag values when where clause specified
func TestServer_ShowTagValuesStatement_WhereClause(t *testing.T) {
	s := OpenServer()
	defer s.Close()

	// Create the "foo" database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{Name: "bar", Duration: time.Hour}); err != nil {
		t.Fatal(err)
	}
	s.SetDefaultRetentionPolicy("foo", "bar")

	// Write series with one point to the database.
	tags := map[string]string{"host": "serverA", "region": "uswest"}
	_, err := s.WriteSeries("foo", "bar", []tsdb.Point{tsdb.NewPoint("cpu", tags, map[string]interface{}{"value": float64(23.2)}, mustParseTime("2000-01-01T00:00:00Z"))})

	if err != nil {
		t.Fatal(err)
	}
	tags2 := map[string]string{"host": "serverC", "region": "useast"}
	_, err = s.WriteSeries("foo", "bar", []tsdb.Point{tsdb.NewPoint("cpu", tags2, map[string]interface{}{"value": float64(23.2)}, mustParseTime("2000-01-01T00:00:00Z"))})

	if err != nil {
		t.Fatal(err)
	}

	q := "SHOW TAG VALUES FROM cpu WITH KEY = region WHERE region = 'useast'"
	results := s.executeQuery(MustParseQuery(q), "foo", nil)

	if results.Error() != nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}
	expected := `{"series":[{"name":"regionTagValues","columns":["region"],"values":[["useast"]]}]}`

	if res := results.Results[0]; res.Err != nil {
		t.Errorf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Errorf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != expected {
		t.Errorf("unexpected row(0): \nexp: %s\ngot: %s", expected, s)
	}
}

// Ensure ShowTagValuesStatement returns ErrDatabaseNotFound for non existent database
func TestServer_ShowTagValuesStatement_ErrDatabaseNotFound(t *testing.T) {
	s := OpenServer()
	defer s.Close()

	// Create the "foo" database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{Name: "bar", Duration: time.Hour}); err != nil {
		t.Fatal(err)
	}
	s.SetDefaultRetentionPolicy("foo", "bar")

	// Write series with one point to the database.
	tags := map[string]string{"host": "serverA", "region": "uswest"}
	_, err := s.WriteSeries("foo", "bar", []tsdb.Point{tsdb.NewPoint("cpu", tags, map[string]interface{}{"value": float64(23.2)}, mustParseTime("2000-01-01T00:00:00Z"))})

	if err != nil {
		t.Fatal(err)
	}

	nonexistentDatabaseName := "baz"

	q := "SHOW TAG VALUES FROM cpu WITH KEY = region"
	results := s.executeQuery(MustParseQuery(q), nonexistentDatabaseName, nil)

	expectedErr := influxdb.ErrDatabaseNotFound(nonexistentDatabaseName)

	extractMessage := func(e error) string {
		r, _ := regexp.Compile("(.+)\\(.+\\)")
		match := r.FindStringSubmatch(e.Error())[1]
		return match
	}

	if err := results.Error(); extractMessage(err) != extractMessage(expectedErr) {
		t.Fatal(err)
	}
}

// Ensure ShowTagValuesStatement returns ErrMeasurementNotFound for non existent database
func TestServer_ShowTagValuesStatement_ErrMeasurementNotFound(t *testing.T) {
	s := OpenServer()
	defer s.Close()

	// Create the "foo" database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{Name: "bar", Duration: time.Hour}); err != nil {
		t.Fatal(err)
	}
	s.SetDefaultRetentionPolicy("foo", "bar")

	// Write series with one point to the database.
	tags := map[string]string{"host": "serverA", "region": "uswest"}
	_, err := s.WriteSeries("foo", "bar", []tsdb.Point{tsdb.NewPoint("cpu", tags, map[string]interface{}{"value": float64(23.2)}, mustParseTime("2000-01-01T00:00:00Z"))})

	if err != nil {
		t.Fatal(err)
	}

	nonExistentMeasurement := "src"

	q := fmt.Sprintf(
		"SHOW TAG VALUES FROM %v WITH KEY = region",
		nonExistentMeasurement,
	)

	results := s.executeQuery(MustParseQuery(q), "foo", nil)

	expectedErr := influxdb.ErrMeasurementNotFound(nonExistentMeasurement)

	extractMessage := func(e error) string {
		r, _ := regexp.Compile("(.+)\\(.+\\)")
		match := r.FindStringSubmatch(e.Error())[1]
		return match
	}

	if err := results.Error(); extractMessage(err) != extractMessage(expectedErr) {
		t.Fatal(err)
	}
}

// Ensure database is created if it does not exist
func TestServer_CreateDatabaseIfNotExists(t *testing.T) {
	s := OpenServer()
	defer s.Close()

	result := s.CreateDatabaseIfNotExists("foo")
	if result != nil {
		t.Fatal(result.Error())
	}

	if a, err := s.Databases(); err != nil {
		t.Fatal(err)
	} else if len(a) != 1 {
		t.Fatalf("unexpected db count: %d", len(a))
	}

	err := s.CreateDatabaseIfNotExists("foo")
	if err != nil {
		t.Fatal("An error should be returned if the database already exists")
	}
}

func TestServer_SeriesByTagNames(t *testing.T)  { t.Skip("pending") }
func TestServer_SeriesByTagValues(t *testing.T) { t.Skip("pending") }
func TestServer_TagNamesBySeries(t *testing.T)  { t.Skip("pending") }
func TestServer_TagValuesBySeries(t *testing.T) { t.Skip("pending") }

// Point JSON Unmarshal tests

func TestbatchWrite_UnmarshalEpoch(t *testing.T) {
	var (
		now     = time.Now()
		nanos   = now.UnixNano()
		micros  = nanos / int64(time.Microsecond)
		millis  = nanos / int64(time.Millisecond)
		seconds = nanos / int64(time.Second)
		minutes = nanos / int64(time.Minute)
		hours   = nanos / int64(time.Hour)
	)

	tests := []struct {
		name  string
		epoch int64
	}{
		{name: "nanos", epoch: nanos},
		{name: "micros", epoch: micros},
		{name: "millis", epoch: millis},
		{name: "seconds", epoch: seconds},
		{name: "minutes", epoch: minutes},
		{name: "hours", epoch: hours},
	}

	for _, test := range tests {
		json := fmt.Sprintf(`"points": [{time: "%d"}`, test.epoch)
		log.Println(json)
		t.Fatal("foo")
	}

}

// Server is a wrapping test struct for influxdb.Server.
type Server struct {
	*influxdb.Server
}

// NewServer returns a new test server instance.
func NewServer() *Server {
	s := influxdb.NewServer(tempfile())
	s.SetAuthenticationEnabled(false)
	return &Server{s}
}

// OpenServer returns a new, open test server instance.
func OpenServer() *Server {
	s := OpenUninitializedServer()
	if err := s.Initialize(url.URL{Host: "127.0.0.1:8080"}); err != nil {
		panic(err.Error())
	}
	return s
}

// OpenUninitializedServer returns a new, uninitialized, open test server instance.
func OpenUninitializedServer() *Server {
	s := NewServer()
	if err := s.Open(); err != nil {
		panic(err.Error())
	}
	return s
}

// OpenDefaultServer opens a server and creates a default db & retention policy.
func OpenDefaultServer() *Server {
	s := OpenServer()
	if err := s.CreateDatabase("db"); err != nil {
		panic(err.Error())
	} else if err = s.CreateRetentionPolicy("db", &meta.RetentionPolicyInfo{Name: "raw", Duration: 1 * time.Hour}); err != nil {
		panic(err.Error())
	} else if err = s.SetDefaultRetentionPolicy("db", "raw"); err != nil {
		panic(err.Error())
	}
	return s
}

// Restart stops and restarts the server.
func (s *Server) Restart() {
	// Stop the server.
	if err := s.Server.Close(); err != nil {
		panic("close: " + err.Error())
	}

	// Open and reset the client.
	if err := s.Server.Open(); err != nil {
		panic("open: " + err.Error())
	}
}

// Close shuts down the server and removes all temporary files.
func (s *Server) Close() {
	defer os.RemoveAll(s.Path())
	s.Server.Close()
}

// MustWriteSeries writes series data and waits for the data to be applied.
// Returns the messaging index for the write.
func (s *Server) MustWriteSeries(database, retentionPolicy string, points []tsdb.Point) uint64 {
	index, err := s.WriteSeries(database, retentionPolicy, points)
	if err != nil {
		panic(err.Error())
	}
	return index
}

func (s *Server) executeQuery(q *influxql.Query, db string, user *meta.UserInfo) influxdb.Response {
	results, err := s.ExecuteQuery(q, db, user, 10000)
	if err != nil {
		return influxdb.Response{Err: err}
	}
	res := influxdb.Response{}
	for r := range results {
		l := len(res.Results)
		if l == 0 {
			res.Results = append(res.Results, r)
		} else if res.Results[l-1].StatementID == r.StatementID {
			res.Results[l-1].Series = append(res.Results[l-1].Series, r.Series...)
		} else {
			res.Results = append(res.Results, r)
		}
	}
	return res
}

// tempfile returns a temporary path.
func tempfile() string {
	f, _ := ioutil.TempFile("", "influxdb-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}

// mustParseTime parses an IS0-8601 string. Panic on error.
func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err.Error())
	}
	return t
}

// MustParseQuery parses an InfluxQL query. Panic on error.
func MustParseQuery(s string) *influxql.Query {
	q, err := influxql.NewParser(strings.NewReader(s)).ParseQuery()
	if err != nil {
		panic(err.Error())
	}
	return q
}

// MustParseSelectStatement parses an InfluxQL select statement. Panic on error.
func MustParseSelectStatement(s string) *influxql.SelectStatement {
	stmt, err := influxql.NewParser(strings.NewReader(s)).ParseStatement()
	if err != nil {
		panic(err.Error())
	}
	return stmt.(*influxql.SelectStatement)
}

// errstr is an ease-of-use function to convert an error to a string.
func errstr(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }
