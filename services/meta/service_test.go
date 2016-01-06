package meta_test

import (
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/services/meta"
	"github.com/influxdb/influxdb/tcp"
)

// Test the ping endpoint.
func TestMetaService_PingEndpoint(t *testing.T) {
	t.Parallel()

	cfg := newConfig()
	defer os.RemoveAll(cfg.Dir)
	s := newService(cfg)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	url, err := url.Parse(s.HTTPAddr())
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Head("http://" + url.String() + "/ping")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status:\n\texp: %d\n\tgot: %d\n", http.StatusOK, resp.StatusCode)
	}
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

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
		t.Fatal("expected database to not return: %v", db)
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
	for i, _ := range cfgs {
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
	for i, _ := range cfgs {
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
		ID:      2,
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

// newServiceAndClient returns new data directory, *Service, and *Client or panics.
// Caller is responsible for deleting data dir and closing client.
func newServiceAndClient() (string, *testService, *meta.Client) {
	cfg := newConfig()
	s := newService(cfg)
	if err := s.Open(); err != nil {
		panic(err)
	}

	c := meta.NewClient([]string{s.HTTPAddr()}, false)
	if err := c.Open(); err != nil {
		panic(err)
	}

	return cfg.Dir, s, c
}

func newConfig() *meta.Config {
	cfg := meta.NewConfig()
	cfg.BindAddress = "127.0.0.1:0"
	cfg.HTTPBindAddress = "127.0.0.1:0"
	cfg.Dir = testTempDir(2)
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
	dir, err := ioutil.TempDir("/tmp", prefix)
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
