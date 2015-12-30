package meta_test

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"runtime"
	"testing"
	"time"

	"github.com/influxdb/influxdb"
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

	url, err := url.Parse(s.URL())
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
	db, err := c.CreateDatabase("db0", false)
	if err != nil {
		t.Fatalf(err.Error())
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	db, err = c.CreateDatabase("db1", false)
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

	if _, err = c.Database("db0"); err == nil {
		t.Fatal("expected an error")
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
}

func TestMetaService_CreateRetentionPolicyIfNotExists(t *testing.T) {
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

	rpi := &meta.RetentionPolicyInfo{
		Name:     "rp0",
		ReplicaN: 1,
		Duration: time.Hour,
	}

	// InfluxQL doesn't support IF NOT EXISTS yet so use the API.
	rp, err := c.CreateRetentionPolicy("db0", rpi, true)

	if err != nil {
		t.Fatal(err)
	} else if rp.Name != "rp0" {
		t.Fatalf("rp name wrong: %s", rp.Name)
	} else if rp.Duration != time.Hour {
		t.Fatalf("rp duration wrong: %s", rp.Duration.String())
	} else if rp.ReplicaN != 1 {
		t.Fatalf("rp replication wrong: %d", rp.ReplicaN)
	}

	// Create the same policy with ifNotExists = true.  Shouldn't error.
	rp, err = c.CreateRetentionPolicy("db0", rpi, true)

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

// newServiceAndClient returns new data directory, *Service, and *Client or panics.
// Caller is responsible for deleting data dir and closing client.
func newServiceAndClient() (string, *meta.Service, *meta.Client) {
	cfg := newConfig()
	s := newService(cfg)
	if err := s.Open(); err != nil {
		panic(err)
	}

	c := meta.NewClient([]string{s.URL()}, false)
	if err := c.Open(); err != nil {
		panic(err)
	}

	return cfg.Dir, s, c
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

	cfg2.JoinPeers = []string{s1.URL()}
	s2 := newService(cfg2)
	if err := s2.Open(); err != nil {
		t.Fatal(err.Error())
	}
	defer s2.Close()

	func() {
		cfg3.JoinPeers = []string{s2.URL()}
		s3 := newService(cfg3)
		if err := s3.Open(); err != nil {
			t.Fatal(err.Error())
		}
		defer s3.Close()

		fmt.Println("ALL OPEN!")

		c1 := meta.NewClient([]string{s1.URL()}, false)
		if err := c1.Open(); err != nil {
			t.Fatal(err.Error())
		}
		defer c1.Close()

		metaNodes, _ := c1.MetaNodes()
		if len(metaNodes) != 3 {
			t.Fatalf("meta nodes wrong: %v", metaNodes)
		}
	}()

	c := meta.NewClient([]string{s1.URL()}, false)
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

	cfg4.JoinPeers = []string{s1.URL()}
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
	srvs := make([]*meta.Service, 3)
	for i, _ := range cfgs {
		c := newConfig()

		cfgs[i] = c

		if i > 0 {
			c.JoinPeers = []string{srvs[0].URL()}
		}
		srvs[i] = newService(c)
		if err := srvs[i].Open(); err != nil {
			t.Fatal(err.Error())
		}
		defer srvs[i].Close()
	}

	c := meta.NewClient([]string{srvs[2].URL()}, false)
	if err := c.Open(); err != nil {
		t.Fatal(err.Error())
	}
	metaNodes, _ := c.MetaNodes()
	if len(metaNodes) != 3 {
		t.Fatalf("meta nodes wrong: %v", metaNodes)
	}

	if _, err := c.CreateDatabase("foo", true); err != nil {
		t.Fatal(err)
	}

	if db, err := c.Database("foo"); db == nil || err != nil {
		t.Fatalf("database foo wasn't created: %s", err.Error())
	}
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

func newService(cfg *meta.Config) *meta.Service {
	// Open shared TCP connection.
	ln, err := net.Listen("tcp", cfg.BindAddress)
	if err != nil {
		panic(err)
	}

	// Multiplex listener.
	mux := tcp.NewMux()

	s := meta.NewService(cfg, &influxdb.Node{})
	s.RaftListener = mux.Listen(meta.MuxHeader)

	go mux.Serve(ln)

	return s
}

func mustParseStatement(s string) influxql.Statement {
	stmt, err := influxql.ParseStatement(s)
	if err != nil {
		panic(err)
	}
	return stmt
}
