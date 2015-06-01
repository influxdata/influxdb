package run_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb/cmd/influxd/run"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/services/httpd"
	"github.com/influxdb/influxdb/toml"
)

// Ensure the server can create a database and retrieve it back.
func TestServer_CreateDatabase(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	// Create the database.
	if res, err := s.Query(`CREATE DATABASE db0`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{}]}` {
		t.Fatalf("unexpected results: %s", res)
	}

	// Verify the database was created.
	if res, err := s.Query(`SHOW DATABASES`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"series":[{"name":"databases","columns":["name"],"values":[["db0"]]}]}]}` {
		t.Fatalf("unexpected results: %s", res)
	}
}

// Ensure the server can delete a database.
func TestServer_DropDatabase(t *testing.T) {
	t.Skip("FIXME(pauldix)")

	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	// Create the database.
	if _, err := s.MetaStore.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	// Delete the database.
	if res, err := s.Query(`DROP DATABASE db0`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{}]}` {
		t.Fatalf("unexpected results: %s", res)
	}

	// Verify the database was deleted.
	if res, err := s.Query(`SHOW DATABASES`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"series":[{"name":"databases","columns":["name"],"values":[[]]}]}]}` {
		t.Fatalf("unexpected results: %s", res)
	}
}

// Ensure the server can create a retention policy.
func TestServer_CreateRetentionPolicy(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	// Create a database.
	if _, err := s.MetaStore.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	// Create the retention policy.
	if res, err := s.Query(`CREATE RETENTION POLICY rp0 ON db0 DURATION 1h REPLICATION 1`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{}]}` {
		t.Fatalf("unexpected results: %s", res)
	}

	// Verify the database was deleted.
	if res, err := s.Query(`SHOW RETENTION POLICIES db0`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"series":[{"columns":["name","duration","replicaN","default"],"values":[["rp0","1h0m0s",1,false]]}]}]}` {
		t.Fatalf("unexpected results: %s", res)
	}
}

// Ensure the server can drop a retention policy.
func TestServer_DropRetentionPolicy(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	// Create a database.
	if _, err := s.MetaStore.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	} else if _, err := s.MetaStore.CreateRetentionPolicy("db0", &meta.RetentionPolicyInfo{
		Name:     "rp0",
		ReplicaN: 1,
		Duration: 1 * time.Hour,
	}); err != nil {
		t.Fatal(err)
	}

	// Drop retenetion policy.
	if res, err := s.Query(`DROP RETENTION POLICY rp0 ON db0`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{}]}` {
		t.Fatalf("unexpected results: %s", res)
	}

	// Verify the retention policy was deleted.
	if res, err := s.Query(`SHOW RETENTION POLICIES db0`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"series":[{"columns":["name","duration","replicaN","default"]}]}]}` {
		t.Fatalf("unexpected results: %s", res)
	}
}

// Ensure the server can create a single point via line protocol and read it back.
func TestServer_Write_LineProtocol(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	// Create the database.
	if _, err := s.MetaStore.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	} else if _, err := s.MetaStore.CreateRetentionPolicy("db0", &meta.RetentionPolicyInfo{Name: "rp0", ReplicaN: 1, Duration: 1 * time.Hour}); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	if res, err := s.Write("db0", "rp0", `cpu,host=server01 value=1.0 `+strconv.FormatInt(now.UnixNano(), 10)); err != nil {
		t.Fatal(err)
	} else if res != `` {
		t.Fatalf("unexpected results: %s", res)
	}

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.cpu`); err != nil {
		t.Fatal(err)
	} else if res != fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["%s",1]]}]}]}`, now.Format(time.RFC3339Nano)) {
		t.Fatalf("unexpected results: %s", res)
	}
}

// Ensure the server can create a single point via json protocol and read it back.
func TestServer_Write_JSON(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	// Create the database.
	if _, err := s.MetaStore.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	} else if _, err := s.MetaStore.CreateRetentionPolicy("db0", &meta.RetentionPolicyInfo{Name: "rp0", ReplicaN: 1, Duration: 1 * time.Hour}); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	if res, err := s.Write("", "", fmt.Sprintf(`{"database" : "db0", "retentionPolicy" : "rp0", "points": [{"name": "cpu", "tags": {"host": "server02"},"fields": {"value": 1.0}}],"time":"%s"} `, now.Format(time.RFC3339Nano))); err != nil {
		t.Fatal(err)
	} else if res != `` {
		t.Fatalf("unexpected results: %s", res)
	}

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.cpu`); err != nil {
		t.Fatal(err)
	} else if res != fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["%s",1]]}]}]}`, now.Format(time.RFC3339Nano)) {
		t.Fatalf("unexpected results: %s", res)
	}
}

// Ensure the server can query with the count aggregate function
func TestServer_Query_Count(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	// Create the database.
	if _, err := s.MetaStore.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	} else if _, err := s.MetaStore.CreateRetentionPolicy("db0", &meta.RetentionPolicyInfo{Name: "rp0", ReplicaN: 1, Duration: 1 * time.Hour}); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	if res, err := s.Write("db0", "rp0", `cpu,host=server01 value=1.0 `+strconv.FormatInt(now.UnixNano(), 10)); err != nil {
		t.Fatal(err)
	} else if res != `` {
		t.Fatalf("unexpected results: %s", res)
	}

	// Verify the data was written.
	if res, err := s.Query(`SELECT count(value) FROM db0.rp0.cpu`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}` {
		t.Fatalf("unexpected results: %s", res)
	}
}

// Server represents a test wrapper for run.Server.
type Server struct {
	*run.Server
	Config *run.Config
}

// NewServer returns a new instance of Server.
func NewServer(c *run.Config, joinURLs string) *Server {
	return &Server{
		Server: run.NewServer(c, joinURLs),
		Config: c,
	}
}

// OpenServer opens a test server.
func OpenServer(c *run.Config, joinURLs string) *Server {
	s := NewServer(c, joinURLs)
	if err := s.Open(); err != nil {
		panic(err.Error())
	}
	return s
}

// Close shuts down the server and removes all temporary paths.
func (s *Server) Close() {
	os.RemoveAll(s.Config.Meta.Dir)
	os.RemoveAll(s.Config.Data.Dir)
	s.Server.Close()
}

// URL returns the base URL for the httpd endpoint.
func (s *Server) URL() string {
	for _, service := range s.Services {
		if service, ok := service.(*httpd.Service); ok {
			return "http://" + service.Addr().String()
		}
	}
	panic("httpd server not found in services")
}

// Query executes a query against the server and returns the results.
func (s *Server) Query(query string) (results string, err error) {
	resp, err := http.Get(s.URL() + "/query?q=" + url.QueryEscape(query))
	if err != nil {
		return "", err
	} else if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("invalid status code: code=%d, body=%s", resp.StatusCode, MustReadAll(resp.Body))
	}
	return string(MustReadAll(resp.Body)), nil
}

// Write executes a write against the server and returns the results.
func (s *Server) Write(db, rp, body string) (results string, err error) {
	v := url.Values{"db": {db}, "rp": {rp}}
	resp, err := http.Post(s.URL()+"/write?"+v.Encode(), "", strings.NewReader(body))
	if err != nil {
		return "", err
	} else if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return "", fmt.Errorf("invalid status code: code=%d, body=%s", resp.StatusCode, MustReadAll(resp.Body))
	}
	return string(MustReadAll(resp.Body)), nil
}

// NewConfig returns the default config with temporary paths.
func NewConfig() *run.Config {
	c := run.NewConfig()
	c.Cluster.BindAddress = "127.0.0.1:0"
	c.Meta.Dir = MustTempFile()
	c.Meta.BindAddress = "127.0.0.1:0"
	c.Meta.HeartbeatTimeout = toml.Duration(50 * time.Millisecond)
	c.Meta.ElectionTimeout = toml.Duration(50 * time.Millisecond)
	c.Meta.LeaderLeaseTimeout = toml.Duration(50 * time.Millisecond)
	c.Meta.CommitTimeout = toml.Duration(5 * time.Millisecond)

	c.Data.Dir = MustTempFile()

	c.HTTPD.Enabled = true
	c.HTTPD.BindAddress = "127.0.0.1:0"
	return c
}

// MustReadAll reads r. Panic on error.
func MustReadAll(r io.Reader) []byte {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		panic(err)
	}
	return b
}

// MustTempFile returns a path to a temporary file.
func MustTempFile() string {
	f, err := ioutil.TempFile("", "influxd-")
	if err != nil {
		panic(err)
	}
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}
