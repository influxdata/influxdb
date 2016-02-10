package meta_test

import (
	"encoding/json"
	"io/ioutil"
	"net"
	"os"
	"path"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tcp"
	"github.com/influxdata/influxdb/toml"
)

func TestMetaService_CreateDatabase(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	if res := c.ExecuteStatement(mustParseStatement("CREATE DATABASE db0")); res.Err != nil {
		t.Fatal(res.Err)
	}

	db, err := c.Database("db0")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	// Make sure a default retention policy was created.
	_, err = c.RetentionPolicy("db0", "default")
	if err != nil {
		t.Fatal(err)
	} else if db.DefaultRetentionPolicy != "default" {
		t.Fatalf("rp name wrong: %s", db.DefaultRetentionPolicy)
	}
}

func TestMetaService_CreateDatabaseIfNotExists(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	qry := `CREATE DATABASE IF NOT EXISTS db0`
	if res := c.ExecuteStatement(mustParseStatement(qry)); res.Err != nil {
		t.Fatal(res.Err)
	}

	db, err := c.Database("db0")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	if res := c.ExecuteStatement(mustParseStatement(qry)); res.Err != nil {
		t.Fatal(res.Err)
	}
}

func TestMetaService_CreateDatabaseWithRetentionPolicy(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	qry := `CREATE DATABASE db0 WITH DURATION 1h REPLICATION 1 NAME rp0`
	if res := c.ExecuteStatement(mustParseStatement(qry)); res.Err != nil {
		t.Fatal(res.Err)
	}

	db, err := c.Database("db0")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	rp := db.RetentionPolicy("rp0")
	if err != nil {
		t.Fatal(err)
	} else if rp.Name != "rp0" {
		t.Fatalf("rp name wrong: %s", rp.Name)
	} else if rp.Duration != time.Hour {
		t.Fatalf("rp duration wrong: %s", rp.Duration.String())
	} else if rp.ReplicaN != 1 {
		t.Fatalf("rp replication wrong: %d", rp.ReplicaN)
	}
}

func TestMetaService_Databases(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	// Create two databases.
	db, err := c.CreateDatabase("db0")
	if err != nil {
		t.Fatalf(err.Error())
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	db, err = c.CreateDatabase("db1")
	if err != nil {
		t.Fatalf(err.Error())
	} else if db.Name != "db1" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	dbs, err := c.Databases()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if len(dbs) != 2 {
		t.Fatalf("expected 2 databases but got %d", len(dbs))
	} else if dbs[0].Name != "db0" {
		t.Fatalf("db name wrong: %s", dbs[0].Name)
	} else if dbs[1].Name != "db1" {
		t.Fatalf("db name wrong: %s", dbs[1].Name)
	}
}

func TestMetaService_DropDatabase(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	qry := `CREATE DATABASE db0`
	if res := c.ExecuteStatement(mustParseStatement(qry)); res.Err != nil {
		t.Fatal(res.Err)
	}

	db, err := c.Database("db0")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	qry = `DROP DATABASE db0`
	if res := c.ExecuteStatement(mustParseStatement(qry)); res.Err != nil {
		t.Fatal(res.Err)
	}

	if db, _ = c.Database("db0"); db != nil {
		t.Fatalf("expected database to not return: %v", db)
	}
}

func TestMetaService_CreateRetentionPolicy(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	if res := c.ExecuteStatement(mustParseStatement("CREATE DATABASE db0")); res.Err != nil {
		t.Fatal(res.Err)
	}

	db, err := c.Database("db0")
	if err != nil {
		t.Fatalf(err.Error())
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	qry := `CREATE RETENTION POLICY rp0 ON db0 DURATION 1h REPLICATION 1`
	if res := c.ExecuteStatement(mustParseStatement(qry)); res.Err != nil {
		t.Fatal(res.Err)
	}

	rp, err := c.RetentionPolicy("db0", "rp0")
	if err != nil {
		t.Fatal(err)
	} else if rp.Name != "rp0" {
		t.Fatalf("rp name wrong: %s", rp.Name)
	} else if rp.Duration != time.Hour {
		t.Fatalf("rp duration wrong: %s", rp.Duration.String())
	} else if rp.ReplicaN != 1 {
		t.Fatalf("rp replication wrong: %d", rp.ReplicaN)
	}

	// Create the same policy.  Should not error.
	if res := c.ExecuteStatement(mustParseStatement(qry)); res.Err != nil {
		t.Fatal(res.Err)
	}

	rp, err = c.RetentionPolicy("db0", "rp0")
	if err != nil {
		t.Fatal(err)
	} else if rp.Name != "rp0" {
		t.Fatalf("rp name wrong: %s", rp.Name)
	} else if rp.Duration != time.Hour {
		t.Fatalf("rp duration wrong: %s", rp.Duration.String())
	} else if rp.ReplicaN != 1 {
		t.Fatalf("rp replication wrong: %d", rp.ReplicaN)
	}
}

func TestMetaService_SetDefaultRetentionPolicy(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	qry := `CREATE DATABASE db0 WITH DURATION 1h REPLICATION 1 NAME rp0`
	if res := c.ExecuteStatement(mustParseStatement(qry)); res.Err != nil {
		t.Fatal(res.Err)
	}

	db, err := c.Database("db0")
	if err != nil {
		t.Fatalf(err.Error())
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	rp, err := c.RetentionPolicy("db0", "rp0")
	if err != nil {
		t.Fatal(err)
	} else if rp.Name != "rp0" {
		t.Fatalf("rp name wrong: %s", rp.Name)
	} else if rp.Duration != time.Hour {
		t.Fatalf("rp duration wrong: %s", rp.Duration.String())
	} else if rp.ReplicaN != 1 {
		t.Fatalf("rp replication wrong: %d", rp.ReplicaN)
	}

	// Make sure default retention policy is now rp0
	if db.DefaultRetentionPolicy != "rp0" {
		t.Fatalf("rp name wrong: %s", db.DefaultRetentionPolicy)
	}
}

func TestMetaService_DropRetentionPolicy(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	if res := c.ExecuteStatement(mustParseStatement("CREATE DATABASE db0")); res.Err != nil {
		t.Fatal(res.Err)
	}

	db, err := c.Database("db0")
	if err != nil {
		t.Fatalf(err.Error())
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	qry := `CREATE RETENTION POLICY rp0 ON db0 DURATION 1h REPLICATION 1`
	if res := c.ExecuteStatement(mustParseStatement(qry)); res.Err != nil {
		t.Fatal(res.Err)
	}

	rp, err := c.RetentionPolicy("db0", "rp0")
	if err != nil {
		t.Fatal(err)
	} else if rp.Name != "rp0" {
		t.Fatalf("rp name wrong: %s", rp.Name)
	} else if rp.Duration != time.Hour {
		t.Fatalf("rp duration wrong: %s", rp.Duration.String())
	} else if rp.ReplicaN != 1 {
		t.Fatalf("rp replication wrong: %d", rp.ReplicaN)
	}

	qry = `DROP RETENTION POLICY rp0 ON db0`
	if res := c.ExecuteStatement(mustParseStatement(qry)); res.Err != nil {
		t.Fatal(res.Err)
	}

	rp, err = c.RetentionPolicy("db0", "rp0")
	if err != nil {
		t.Fatal(err)
	} else if rp != nil {
		t.Fatalf("rp should have been dropped")
	}
}

func TestMetaService_CreateUser(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	// Create an admin user
	if res := c.ExecuteStatement(mustParseStatement("CREATE USER fred WITH PASSWORD 'supersecure' WITH ALL PRIVILEGES")); res.Err != nil {
		t.Fatal(res.Err)
	}

	// Create a non-admin user
	if res := c.ExecuteStatement(mustParseStatement("CREATE USER wilma WITH PASSWORD 'password'")); res.Err != nil {
		t.Fatal(res.Err)
	}

	u, err := c.User("fred")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "fred", u.Name; exp != got {
		t.Fatalf("unexpected user name: exp: %s got: %s", exp, got)
	}
	if !u.Admin {
		t.Fatalf("expected user to be admin")
	}

	u, err = c.Authenticate("fred", "supersecure")
	if u == nil || err != nil {
		t.Fatalf("failed to authenticate")
	}
	if u.Name != "fred" {
		t.Fatalf("failed to authenticate")
	}

	// Auth for bad password should fail
	u, err = c.Authenticate("fred", "badpassword")
	if u != nil || err != meta.ErrAuthenticate {
		t.Fatalf("authentication should fail with %s", meta.ErrAuthenticate)
	}

	// Auth for no password should fail
	u, err = c.Authenticate("fred", "")
	if u != nil || err != meta.ErrAuthenticate {
		t.Fatalf("authentication should fail with %s", meta.ErrAuthenticate)
	}

	// Change password should succeed.
	if res := c.ExecuteStatement(mustParseStatement("SET PASSWORD FOR fred = 'moresupersecure'")); res.Err != nil {
		t.Fatal(res.Err)
	}

	// Auth for old password should fail
	u, err = c.Authenticate("fred", "supersecure")
	if u != nil || err != meta.ErrAuthenticate {
		t.Fatalf("authentication should fail with %s", meta.ErrAuthenticate)
	}

	// Auth for new password should succeed.
	u, err = c.Authenticate("fred", "moresupersecure")
	if u == nil || err != nil {
		t.Fatalf("failed to authenticate")
	}
	if u.Name != "fred" {
		t.Fatalf("failed to authenticate")
	}

	// Auth for unkonwn user should fail
	u, err = c.Authenticate("foo", "")
	if u != nil || err != meta.ErrUserNotFound {
		t.Fatalf("authentication should fail with %s", meta.ErrUserNotFound)
	}

	u, err = c.User("wilma")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "wilma", u.Name; exp != got {
		t.Fatalf("unexpected user name: exp: %s got: %s", exp, got)
	}
	if u.Admin {
		t.Fatalf("expected user not to be an admin")
	}

	if exp, got := 2, c.UserCount(); exp != got {
		t.Fatalf("unexpected user count.  got: %d exp: %d", got, exp)
	}

	// Grant privilidges to a non-admin user
	if res := c.ExecuteStatement(mustParseStatement("GRANT ALL PRIVILEGES TO wilma")); res.Err != nil {
		t.Fatal(res.Err)
	}

	u, err = c.User("wilma")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "wilma", u.Name; exp != got {
		t.Fatalf("unexpected user name: exp: %s got: %s", exp, got)
	}
	if !u.Admin {
		t.Fatalf("expected user to be an admin")
	}

	// Revoke privilidges from user
	if res := c.ExecuteStatement(mustParseStatement("REVOKE ALL PRIVILEGES FROM wilma")); res.Err != nil {
		t.Fatal(res.Err)
	}

	u, err = c.User("wilma")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "wilma", u.Name; exp != got {
		t.Fatalf("unexpected user name: exp: %s got: %s", exp, got)
	}
	if u.Admin {
		t.Fatalf("expected user not to be an admin")
	}

	// Revoke privilidges from user
	if res := c.ExecuteStatement(mustParseStatement("REVOKE ALL PRIVILEGES FROM wilma")); res.Err != nil {
		t.Fatal(res.Err)
	}

	u, err = c.User("wilma")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "wilma", u.Name; exp != got {
		t.Fatalf("unexpected user name: exp: %s got: %s", exp, got)
	}
	if u.Admin {
		t.Fatalf("expected user not to be an admin")
	}

	// Create a database to use for assiging privileges to.
	if res := c.ExecuteStatement(mustParseStatement("CREATE DATABASE db0")); res.Err != nil {
		t.Fatal(res.Err)
	}

	db, err := c.Database("db0")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	// Assign a single privilege at the database level
	if res := c.ExecuteStatement(mustParseStatement("GRANT READ ON db0 TO wilma")); res.Err != nil {
		t.Fatal(res.Err)
	}

	p, err := c.UserPrivilege("wilma", "db0")
	if err != nil {
		t.Fatal(err)
	}
	if p == nil {
		t.Fatal("expected privilege but was nil")
	}
	if exp, got := influxql.ReadPrivilege, *p; exp != got {
		t.Fatalf("unexpected privilege.  exp: %d, got: %d", exp, got)
	}

	// Remove a single privilege at the database level
	if res := c.ExecuteStatement(mustParseStatement("REVOKE READ ON db0 FROM wilma")); res.Err != nil {
		t.Fatal(res.Err)
	}
	p, err = c.UserPrivilege("wilma", "db0")
	if err != nil {
		t.Fatal(err)
	}
	if p == nil {
		t.Fatal("expected privilege but was nil")
	}
	if exp, got := influxql.NoPrivileges, *p; exp != got {
		t.Fatalf("unexpected privilege.  exp: %d, got: %d", exp, got)
	}

	// Drop a user
	if res := c.ExecuteStatement(mustParseStatement("DROP USER wilma")); res.Err != nil {
		t.Fatal(res.Err)
	}

	u, err = c.User("wilma")
	if err != meta.ErrUserNotFound {
		t.Fatalf("user lookup should fail with %s", meta.ErrUserNotFound)
	}

	if exp, got := 1, c.UserCount(); exp != got {
		t.Fatalf("unexpected user count.  got: %d exp: %d", got, exp)
	}
}

func TestMetaService_ContinuousQueries(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	// Create a database to use
	if res := c.ExecuteStatement(mustParseStatement("CREATE DATABASE db0")); res.Err != nil {
		t.Fatal(res.Err)
	}
	db, err := c.Database("db0")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	// Create a CQ
	if res := c.ExecuteStatement(mustParseStatement("CREATE CONTINUOUS QUERY cq0 ON db0 BEGIN SELECT count(value) INTO foo_count FROM foo GROUP BY time(10m) END")); res.Err != nil {
		t.Fatal(res.Err)
	}

	res := c.ExecuteStatement(mustParseStatement("SHOW CONTINUOUS QUERIES"))
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	exp := `{"series":[{"name":"db0","columns":["name","query"],"values":[["cq0","CREATE CONTINUOUS QUERY cq0 ON db0 BEGIN SELECT count(value) INTO foo_count FROM foo GROUP BY time(10m) END"]]}]}`
	got := mustMarshalJSON(res)
	if exp != got {
		t.Fatalf("unexpected response.\n\nexp: %s\ngot: %s\n", exp, got)
	}

	// Recreate an existing CQ
	if res := c.ExecuteStatement(mustParseStatement("CREATE CONTINUOUS QUERY cq0 ON db0 BEGIN SELECT max(value) INTO foo_max FROM foo GROUP BY time(10m) END")); res.Err == nil {
		t.Fatalf("expected error: got %v", res.Err)
	}

	// Create a few more CQ's
	if res := c.ExecuteStatement(mustParseStatement("CREATE CONTINUOUS QUERY cq1 ON db0 BEGIN SELECT max(value) INTO foo_max FROM foo GROUP BY time(10m) END")); res.Err != nil {
		t.Fatal(res.Err)
	}
	if res := c.ExecuteStatement(mustParseStatement("CREATE CONTINUOUS QUERY cq2 ON db0 BEGIN SELECT min(value) INTO foo_min FROM foo GROUP BY time(10m) END")); res.Err != nil {
		t.Fatal(res.Err)
	}

	// Drop a single CQ
	if res := c.ExecuteStatement(mustParseStatement("DROP CONTINUOUS QUERY cq1 ON db0")); res.Err != nil {
		t.Fatal(res.Err)
	}

	res = c.ExecuteStatement(mustParseStatement("SHOW CONTINUOUS QUERIES"))
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	exp = `{"series":[{"name":"db0","columns":["name","query"],"values":[["cq0","CREATE CONTINUOUS QUERY cq0 ON db0 BEGIN SELECT count(value) INTO foo_count FROM foo GROUP BY time(10m) END"],["cq2","CREATE CONTINUOUS QUERY cq2 ON db0 BEGIN SELECT min(value) INTO foo_min FROM foo GROUP BY time(10m) END"]]}]}`
	got = mustMarshalJSON(res)
	if exp != got {
		t.Fatalf("unexpected response.\n\nexp: %s\ngot: %s\n", exp, got)
	}
}

func TestMetaService_Subscriptions_Create(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	// Create a database to use
	if res := c.ExecuteStatement(mustParseStatement("CREATE DATABASE db0")); res.Err != nil {
		t.Fatal(res.Err)
	}
	db, err := c.Database("db0")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	// Create a subscription
	if res := c.ExecuteStatement(mustParseStatement(`CREATE SUBSCRIPTION sub0 ON db0."default" DESTINATIONS ALL 'udp://example.com:9090'`)); res.Err != nil {
		t.Fatal(res.Err)
	}

	// Re-create a subscription
	if res := c.ExecuteStatement(mustParseStatement(`CREATE SUBSCRIPTION sub0 ON db0."default" DESTINATIONS ALL 'udp://example.com:9090'`)); res.Err == nil {
		t.Fatal(res.Err)
	}

	res := c.ExecuteStatement(mustParseStatement(`SHOW SUBSCRIPTIONS`))
	if res.Err != nil {
		t.Fatal(res.Err)
	} else if got, exp := len(res.Series), 1; got != exp {
		t.Fatalf("unexpected response.\n\ngot: %d series\nexp: %d\n", got, exp)
	}

	// Create another subscription.
	if res := c.ExecuteStatement(mustParseStatement(`CREATE SUBSCRIPTION sub1 ON db0."default" DESTINATIONS ALL 'udp://example.com:6060'`)); res.Err != nil {
		t.Fatal(res.Err)
	}

	// The subscriptions are correctly created.
	if res = c.ExecuteStatement(mustParseStatement(`SHOW SUBSCRIPTIONS`)); res.Err != nil {
		t.Fatal(res.Err)
	}

	exp := `{"series":[{"name":"db0","columns":["retention_policy","name","mode","destinations"],"values":[["default","sub0","ALL",["udp://example.com:9090"]],["default","sub1","ALL",["udp://example.com:6060"]]]}]}`
	got := mustMarshalJSON(res)
	if got != exp {
		t.Fatalf("unexpected response.\n\ngot: %s\nexp: %s\n", exp, got)
	}
}

func TestMetaService_Subscriptions_Show(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	// Create a database to use
	if res := c.ExecuteStatement(mustParseStatement("CREATE DATABASE db0")); res.Err != nil {
		t.Fatal(res.Err)
	}
	db, err := c.Database("db0")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	// SHOW SUBSCRIPTIONS returns no subscriptions when there are none.
	res := c.ExecuteStatement(mustParseStatement(`SHOW SUBSCRIPTIONS`))
	if res.Err != nil {
		t.Fatal(res.Err)
	} else if got, exp := len(res.Series), 0; got != exp {
		t.Fatalf("got %d series, expected %d", got, exp)
	}

	// Create a subscription.
	if res = c.ExecuteStatement(mustParseStatement(`CREATE SUBSCRIPTION sub0 ON db0."default" DESTINATIONS ALL 'udp://example.com:9090'`)); res.Err != nil {
		t.Fatal(res.Err)
	}

	// SHOW SUBSCRIPTIONS returns the created subscription.
	if res = c.ExecuteStatement(mustParseStatement(`SHOW SUBSCRIPTIONS`)); res.Err != nil {
		t.Fatal(res.Err)
	} else if got, exp := len(res.Series), 1; got != exp {
		t.Fatalf("got %d series, expected %d", got, exp)
	}

	exp := `{"series":[{"name":"db0","columns":["retention_policy","name","mode","destinations"],"values":[["default","sub0","ALL",["udp://example.com:9090"]]]}]}`
	got := mustMarshalJSON(res)
	if got != exp {
		t.Fatalf("unexpected response.\n\ngot: %s\nexp: %s\n", got, exp)
	}
}

func TestMetaService_Subscriptions_Drop(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	// Create a database to use
	if res := c.ExecuteStatement(mustParseStatement("CREATE DATABASE db0")); res.Err != nil {
		t.Fatal(res.Err)
	}
	db, err := c.Database("db0")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	// DROP SUBSCRIPTION returns ErrSubscriptionNotFound when the
	// subscription is unknown.
	res := c.ExecuteStatement(mustParseStatement(`DROP SUBSCRIPTION foo ON db0."default"`))
	if got, exp := res.Err, meta.ErrSubscriptionNotFound; got.Error() != exp.Error() {
		t.Fatalf("got: %s, exp: %s", got.Error(), exp)
	}

	// Create a subscription.
	if res = c.ExecuteStatement(mustParseStatement(`CREATE SUBSCRIPTION sub0 ON db0."default" DESTINATIONS ALL 'udp://example.com:9090'`)); res.Err != nil {
		t.Fatal(res.Err)
	}

	// DROP SUBSCRIPTION returns an influxdb.ErrDatabaseNotFound when
	// the database is unknown.
	res = c.ExecuteStatement(mustParseStatement(`DROP SUBSCRIPTION sub0 ON foo."default"`))
	if got, exp := res.Err, influxdb.ErrDatabaseNotFound("foo"); got.Error() != exp.Error() {
		t.Fatalf("got: %s, exp: %s", got.Error(), exp)
	}

	// DROP SUBSCRIPTION returns an influxdb.ErrRetentionPolicyNotFound
	// when the retention policy is unknown.
	res = c.ExecuteStatement(mustParseStatement(`DROP SUBSCRIPTION sub0 ON db0."foo_policy"`))
	if got, exp := res.Err, influxdb.ErrRetentionPolicyNotFound("foo_policy"); got.Error() != exp.Error() {
		t.Fatalf("got: %s, exp: %s", got.Error(), exp)
	}

	// DROP SUBSCRIPTION drops the subsciption if it can find it.
	res = c.ExecuteStatement(mustParseStatement(`DROP SUBSCRIPTION sub0 ON db0."default"`))
	if got := res.Err; got != nil {
		t.Fatalf("got: %s, exp: %v", got.Error(), nil)
	}

	if res = c.ExecuteStatement(mustParseStatement(`SHOW SUBSCRIPTIONS`)); res.Err != nil {
		t.Fatal(res.Err)
	} else if got, exp := len(res.Series), 0; got != exp {
		t.Fatalf("got %d series, expected %d", got, exp)
	}
}

func TestMetaService_Shards(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	exp := &meta.NodeInfo{
		ID:      2,
		Host:    "foo:8180",
		TCPHost: "bar:8281",
	}

	if _, err := c.CreateDataNode(exp.Host, exp.TCPHost); err != nil {
		t.Fatal(err.Error())
	}

	if _, err := c.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	// Test creating a shard group.
	tmin := time.Now()
	sg, err := c.CreateShardGroup("db0", "default", tmin)
	if err != nil {
		t.Fatal(err)
	} else if sg == nil {
		t.Fatalf("expected ShardGroup")
	}

	// Test pre-creating shard groups.
	dur := sg.EndTime.Sub(sg.StartTime) + time.Nanosecond
	tmax := tmin.Add(dur)
	if err := c.PrecreateShardGroups(tmin, tmax); err != nil {
		t.Fatal(err)
	}

	// Test finding shard groups by time range.
	groups, err := c.ShardGroupsByTimeRange("db0", "default", tmin, tmax)
	if err != nil {
		t.Fatal(err)
	} else if len(groups) != 2 {
		t.Fatalf("wrong number of shard groups: %d", len(groups))
	}

	// Test finding shard owner.
	db, rp, owner := c.ShardOwner(groups[0].Shards[0].ID)
	if db != "db0" {
		t.Fatalf("wrong db name: %s", db)
	} else if rp != "default" {
		t.Fatalf("wrong rp name: %s", rp)
	} else if owner.ID != groups[0].ID {
		t.Fatalf("wrong owner: exp %d got %d", groups[0].ID, owner.ID)
	}

	// Test deleting a shard group.
	if err := c.DeleteShardGroup("db0", "default", groups[0].ID); err != nil {
		t.Fatal(err)
	} else if groups, err = c.ShardGroupsByTimeRange("db0", "default", tmin, tmax); err != nil {
		t.Fatal(err)
	} else if len(groups) != 1 {
		t.Fatalf("wrong number of shard groups after delete: %d", len(groups))
	}
}

func TestMetaService_CreateRemoveMetaNode(t *testing.T) {
	t.Parallel()

	cfg1 := newConfig()
	defer os.RemoveAll(cfg1.Dir)
	cfg2 := newConfig()
	defer os.RemoveAll(cfg2.Dir)
	cfg3 := newConfig()
	defer os.RemoveAll(cfg3.Dir)
	cfg4 := newConfig()
	defer os.RemoveAll(cfg4.Dir)

	s1 := newService(cfg1)
	if err := s1.Open(); err != nil {
		t.Fatalf(err.Error())
	}
	defer s1.Close()

	cfg2.JoinPeers = []string{s1.HTTPAddr()}
	s2 := newService(cfg2)
	if err := s2.Open(); err != nil {
		t.Fatal(err.Error())
	}
	defer s2.Close()

	func() {
		cfg3.JoinPeers = []string{s2.HTTPAddr()}
		s3 := newService(cfg3)
		if err := s3.Open(); err != nil {
			t.Fatal(err.Error())
		}
		defer s3.Close()

		c1 := meta.NewClient([]string{s1.HTTPAddr()}, false)
		if err := c1.Open(); err != nil {
			t.Fatal(err.Error())
		}
		defer c1.Close()

		metaNodes, _ := c1.MetaNodes()
		if len(metaNodes) != 3 {
			t.Fatalf("meta nodes wrong: %v", metaNodes)
		}
	}()

	c := meta.NewClient([]string{s1.HTTPAddr()}, false)
	if err := c.Open(); err != nil {
		t.Fatal(err.Error())
	}
	defer c.Close()

	if res := c.ExecuteStatement(mustParseStatement("DROP META SERVER 3")); res.Err != nil {
		t.Fatal(res.Err)
	}

	metaNodes, _ := c.MetaNodes()
	if len(metaNodes) != 2 {
		t.Fatalf("meta nodes wrong: %v", metaNodes)
	}

	cfg4.JoinPeers = []string{s1.HTTPAddr()}
	s4 := newService(cfg4)
	if err := s4.Open(); err != nil {
		t.Fatal(err.Error())
	}
	defer s4.Close()

	metaNodes, _ = c.MetaNodes()
	if len(metaNodes) != 3 {
		t.Fatalf("meta nodes wrong: %v", metaNodes)
	}
}

// Ensure that if we attempt to create a database and the client
// is pointed at a server that isn't the leader, it automatically
// hits the leader and finishes the command
func TestMetaService_CommandAgainstNonLeader(t *testing.T) {
	t.Parallel()

	cfgs := make([]*meta.Config, 3)
	srvs := make([]*testService, 3)
	for i := range cfgs {
		c := newConfig()

		cfgs[i] = c

		if i > 0 {
			c.JoinPeers = []string{srvs[0].HTTPAddr()}
		}
		srvs[i] = newService(c)
		if err := srvs[i].Open(); err != nil {
			t.Fatal(err.Error())
		}
		defer srvs[i].Close()
		defer os.RemoveAll(c.Dir)
	}

	c := meta.NewClient([]string{srvs[2].HTTPAddr()}, false)
	if err := c.Open(); err != nil {
		t.Fatal(err.Error())
	}
	defer c.Close()

	metaNodes, _ := c.MetaNodes()
	if len(metaNodes) != 3 {
		t.Fatalf("meta nodes wrong: %v", metaNodes)
	}

	if _, err := c.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}

	if db, err := c.Database("foo"); db == nil || err != nil {
		t.Fatalf("database foo wasn't created: %s", err.Error())
	}
}

// Ensure that the client will fail over to another server if the leader goes
// down. Also ensure that the cluster will come back up successfully after restart
func TestMetaService_FailureAndRestartCluster(t *testing.T) {
	t.Parallel()

	cfgs := make([]*meta.Config, 3)
	srvs := make([]*testService, 3)
	for i := range cfgs {
		c := newConfig()

		cfgs[i] = c

		if i > 0 {
			c.JoinPeers = []string{srvs[0].HTTPAddr()}
		}
		srvs[i] = newService(c)
		if err := srvs[i].Open(); err != nil {
			t.Fatal(err.Error())
		}
		c.HTTPBindAddress = srvs[i].HTTPAddr()
		c.BindAddress = srvs[i].RaftAddr()
		c.JoinPeers = nil
		defer srvs[i].Close()
		defer os.RemoveAll(c.Dir)
	}

	c := meta.NewClient([]string{srvs[0].HTTPAddr(), srvs[1].HTTPAddr()}, false)
	if err := c.Open(); err != nil {
		t.Fatal(err.Error())
	}
	defer c.Close()

	// check to see we were assigned a valid clusterID
	c1ID := c.ClusterID()
	if c1ID == 0 {
		t.Fatalf("invalid cluster id: %d", c1ID)
	}

	if _, err := c.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}

	if db, err := c.Database("foo"); db == nil || err != nil {
		t.Fatalf("database foo wasn't created: %s", err.Error())
	}

	if err := srvs[0].Close(); err != nil {
		t.Fatal(err.Error())
	}

	if _, err := c.CreateDatabase("bar"); err != nil {
		t.Fatal(err)
	}

	if db, err := c.Database("bar"); db == nil || err != nil {
		t.Fatalf("database bar wasn't created: %s", err.Error())
	}

	if err := srvs[1].Close(); err != nil {
		t.Fatal(err.Error())
	}
	if err := srvs[2].Close(); err != nil {
		t.Fatal(err.Error())
	}

	// give them a second to shut down
	time.Sleep(time.Second)

	// when we start back up they need to happen simultaneously, otherwise
	// a leader won't get elected
	var wg sync.WaitGroup
	for i, cfg := range cfgs {
		srvs[i] = newService(cfg)
		wg.Add(1)
		go func(srv *testService) {
			if err := srv.Open(); err != nil {
				panic(err)
			}
			wg.Done()
		}(srvs[i])
		defer srvs[i].Close()
	}
	wg.Wait()
	time.Sleep(time.Second)

	c2 := meta.NewClient([]string{srvs[0].HTTPAddr()}, false)
	if err := c2.Open(); err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	c2ID := c2.ClusterID()
	if c1ID != c2ID {
		t.Fatalf("invalid cluster id. got: %d, exp: %d", c2ID, c1ID)
	}

	if db, err := c2.Database("bar"); db == nil || err != nil {
		t.Fatalf("database bar wasn't created: %s", err.Error())
	}

	if _, err := c2.CreateDatabase("asdf"); err != nil {
		t.Fatal(err)
	}

	if db, err := c2.Database("asdf"); db == nil || err != nil {
		t.Fatalf("database bar wasn't created: %s", err.Error())
	}
}

// Ensures that everything works after a host name change. This is
// skipped by default. To enable add hosts foobar and asdf to your
// /etc/hosts file and point those to 127.0.0.1
func TestMetaService_NameChangeSingleNode(t *testing.T) {
	t.Skip("not enabled")
	t.Parallel()

	cfg := newConfig()
	defer os.RemoveAll(cfg.Dir)
	cfg.BindAddress = "foobar:0"
	cfg.HTTPBindAddress = "foobar:0"
	s := newService(cfg)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	c := meta.NewClient([]string{s.HTTPAddr()}, false)
	if err := c.Open(); err != nil {
		t.Fatal(err.Error())
	}
	defer c.Close()

	if _, err := c.CreateDatabase("foo"); err != nil {
		t.Fatal(err.Error())
	}

	s.Close()
	time.Sleep(time.Second)

	cfg.BindAddress = "asdf" + ":" + strings.Split(s.RaftAddr(), ":")[1]
	cfg.HTTPBindAddress = "asdf" + ":" + strings.Split(s.HTTPAddr(), ":")[1]
	s = newService(cfg)
	if err := s.Open(); err != nil {
		t.Fatal(err.Error())
	}
	defer s.Close()

	c2 := meta.NewClient([]string{s.HTTPAddr()}, false)
	if err := c2.Open(); err != nil {
		t.Fatal(err.Error())
	}
	defer c2.Close()

	db, err := c2.Database("foo")
	if db == nil || err != nil {
		t.Fatal(err.Error())
	}

	nodes, err := c2.MetaNodes()
	if err != nil {
		t.Fatal(err.Error())
	}
	exp := []meta.NodeInfo{{ID: 1, Host: cfg.HTTPBindAddress, TCPHost: cfg.BindAddress}}

	time.Sleep(10 * time.Second)
	if !reflect.DeepEqual(nodes, exp) {
		t.Fatalf("nodes don't match: %v", nodes)
	}
}

func TestMetaService_CreateDataNode(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	exp := &meta.NodeInfo{
		ID:      1,
		Host:    "foo:8180",
		TCPHost: "bar:8281",
	}

	n, err := c.CreateDataNode(exp.Host, exp.TCPHost)
	if err != nil {
		t.Fatal(err.Error())
	}

	if !reflect.DeepEqual(n, exp) {
		t.Fatalf("data node attributes wrong: %v", n)
	}

	nodes, err := c.DataNodes()
	if err != nil {
		t.Fatal(err.Error())
	}

	if !reflect.DeepEqual(nodes, []meta.NodeInfo{*exp}) {
		t.Fatalf("nodes wrong: %v", nodes)
	}
}

func TestMetaService_DropDataNode(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	exp := &meta.NodeInfo{
		ID:      1,
		Host:    "foo:8180",
		TCPHost: "bar:8281",
	}

	n, err := c.CreateDataNode(exp.Host, exp.TCPHost)
	if err != nil {
		t.Fatal(err.Error())
	}

	if !reflect.DeepEqual(n, exp) {
		t.Fatalf("data node attributes wrong: %v", n)
	}

	nodes, err := c.DataNodes()
	if err != nil {
		t.Fatal(err.Error())
	}

	if !reflect.DeepEqual(nodes, []meta.NodeInfo{*exp}) {
		t.Fatalf("nodes wrong: %v", nodes)
	}

	if _, err := c.CreateDatabase("foo"); err != nil {
		t.Fatal(err.Error())
	}
	sg, err := c.CreateShardGroup("foo", "default", time.Now())
	if err != nil {
		t.Fatal(err.Error())
	}

	if !reflect.DeepEqual(sg.Shards[0].Owners, []meta.ShardOwner{{1}}) {
		t.Fatalf("expected owners to be [1]: %v", sg.Shards[0].Owners)
	}

	if res := c.ExecuteStatement(mustParseStatement("DROP DATA SERVER 1")); res.Err != nil {
		t.Fatal(res.Err.Error())
	}

	rp, _ := c.RetentionPolicy("foo", "default")
	if len(rp.ShardGroups[0].Shards[0].Owners) != 0 {
		t.Fatalf("expected shard to have no owners: %v", rp.ShardGroups[0].Shards[0].Owners)
	}
}

func TestMetaService_PersistClusterIDAfterRestart(t *testing.T) {
	t.Parallel()

	cfg := newConfig()
	defer os.RemoveAll(cfg.Dir)
	s := newService(cfg)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	c := meta.NewClient([]string{s.HTTPAddr()}, false)
	if err := c.Open(); err != nil {
		t.Fatal(err.Error())
	}
	id := c.ClusterID()
	if id == 0 {
		t.Fatal("cluster ID can't be zero")
	}

	s.Close()
	s = newService(cfg)
	if err := s.Open(); err != nil {
		t.Fatal(err.Error())
	}

	c = meta.NewClient([]string{s.HTTPAddr()}, false)
	if err := c.Open(); err != nil {
		t.Fatal(err.Error())
	}
	defer c.Close()

	idAfter := c.ClusterID()
	if idAfter == 0 {
		t.Fatal("cluster ID can't be zero")
	} else if idAfter != id {
		t.Fatalf("cluster id not the same: %d, %d", idAfter, id)
	}
}

func TestMetaService_Ping(t *testing.T) {
	cfgs := make([]*meta.Config, 3)
	srvs := make([]*testService, 3)
	for i := range cfgs {
		c := newConfig()

		cfgs[i] = c

		if i > 0 {
			c.JoinPeers = []string{srvs[0].HTTPAddr()}
		}
		srvs[i] = newService(c)
		if err := srvs[i].Open(); err != nil {
			t.Fatal(err.Error())
		}
		c.HTTPBindAddress = srvs[i].HTTPAddr()
		c.BindAddress = srvs[i].RaftAddr()
		c.JoinPeers = nil
		defer srvs[i].Close()
		defer os.RemoveAll(c.Dir)
	}

	c := meta.NewClient([]string{srvs[0].HTTPAddr(), srvs[1].HTTPAddr()}, false)
	if err := c.Open(); err != nil {
		t.Fatal(err.Error())
	}
	defer c.Close()

	if err := c.Ping(false); err != nil {
		t.Fatal(err.Error())
	}
	if err := c.Ping(true); err != nil {
		t.Fatal(err.Error())
	}

	srvs[1].Close()

	if err := c.Ping(false); err != nil {
		t.Fatal(err.Error())
	}

	if err := c.Ping(true); err == nil {
		t.Fatal("expected error on ping")
	}
}

func TestMetaService_AcquireLease(t *testing.T) {
	t.Parallel()

	d, s, c1 := newServiceAndClient()
	c2 := newClient(s)
	defer os.RemoveAll(d)
	defer s.Close()
	defer c1.Close()
	defer c2.Close()

	n1, err := c1.CreateDataNode("foo1:8180", "bar1:8281")
	if err != nil {
		t.Fatal(err.Error())
	}

	n2, err := c2.CreateDataNode("foo2:8180", "bar2:8281")
	if err != nil {
		t.Fatal(err.Error())
	}

	// Client 1 acquires a lease.  Should succeed.
	l, err := c1.AcquireLease("foo")
	if err != nil {
		t.Fatal(err)
	} else if l == nil {
		t.Fatal("expected *Lease")
	} else if l.Name != "foo" {
		t.Fatalf("lease name wrong: %s", l.Name)
	} else if l.Owner != n1.ID {
		t.Fatalf("owner ID wrong. exp %d got %d", n1.ID, l.Owner)
	}

	t.Logf("c1: %d, c2: %d", c1.NodeID(), c2.NodeID())
	// Client 2 attempts to acquire the same lease.  Should fail.
	l, err = c2.AcquireLease("foo")
	if err == nil {
		t.Fatal("expected to fail because another node owns the lease")
	}

	// Wait for Client 1's lease to expire.
	time.Sleep(1 * time.Second)

	// Client 2 retries to acquire the lease.  Should succeed this time.
	l, err = c2.AcquireLease("foo")
	if err != nil {
		t.Fatal(err)
	} else if l == nil {
		t.Fatal("expected *Lease")
	} else if l.Name != "foo" {
		t.Fatalf("lease name wrong: %s", l.Name)
	} else if l.Owner != n2.ID {
		t.Fatalf("owner ID wrong. exp %d got %d", n2.ID, l.Owner)
	}
}

// newServiceAndClient returns new data directory, *Service, and *Client or panics.
// Caller is responsible for deleting data dir and closing client.
func newServiceAndClient() (string, *testService, *meta.Client) {
	cfg := newConfig()
	s := newService(cfg)
	if err := s.Open(); err != nil {
		panic(err)
	}

	c := newClient(s)

	return cfg.Dir, s, c
}

func newClient(s *testService) *meta.Client {
	c := meta.NewClient([]string{s.HTTPAddr()}, false)
	if err := c.Open(); err != nil {
		panic(err)
	}
	return c
}

func newConfig() *meta.Config {
	cfg := meta.NewConfig()
	cfg.BindAddress = "127.0.0.1:0"
	cfg.HTTPBindAddress = "127.0.0.1:0"
	cfg.Dir = testTempDir(2)
	cfg.LeaseDuration = toml.Duration(1 * time.Second)
	return cfg
}

func testTempDir(skip int) string {
	// Get name of the calling function.
	pc, _, _, ok := runtime.Caller(skip)
	if !ok {
		panic("failed to get name of test function")
	}
	_, prefix := path.Split(runtime.FuncForPC(pc).Name())
	// Make a temp dir prefixed with calling function's name.
	dir, err := ioutil.TempDir(os.TempDir(), prefix)
	if err != nil {
		panic(err)
	}
	return dir
}

type testService struct {
	*meta.Service
	ln net.Listener
}

func (t *testService) Close() error {
	if err := t.Service.Close(); err != nil {
		return err
	}
	return t.ln.Close()
}

func newService(cfg *meta.Config) *testService {
	// Open shared TCP connection.
	ln, err := net.Listen("tcp", cfg.BindAddress)
	if err != nil {
		panic(err)
	}

	// Multiplex listener.
	mux := tcp.NewMux()

	s := meta.NewService(cfg)
	s.RaftListener = mux.Listen(meta.MuxHeader)

	go mux.Serve(ln)

	return &testService{Service: s, ln: ln}
}

func mustParseStatement(s string) influxql.Statement {
	stmt, err := influxql.ParseStatement(s)
	if err != nil {
		panic(err)
	}
	return stmt
}

func mustMarshalJSON(v interface{}) string {
	b, e := json.Marshal(v)
	if e != nil {
		panic(e)
	}
	return string(b)
}
