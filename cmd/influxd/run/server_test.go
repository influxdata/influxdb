package run_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb/cmd/influxd/run"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/services/httpd"
	"github.com/influxdb/influxdb/toml"
)

// Ensure the database commands work.
func TestServer_DatabaseCommands(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	test := Test{
		queries: []*Query{
			&Query{
				name:    "create database should succeed",
				command: `CREATE DATABASE db0`,
				exp:     `{"results":[{}]}`,
			},
			&Query{
				name:    "create database should fail if it already exists",
				command: `CREATE DATABASE db0`,
				exp:     `{"results":[{"error":"database already exists"}]}`,
			},
			&Query{
				skip:    true,
				name:    "drop database should succeed - FIXME pauldix",
				command: `DROP DATABASE db0`,
				exp:     `{"results":[{}]}`,
			},
			&Query{
				skip:    true,
				name:    "drop database should fail if it doesn't exist - FIXME pauldix",
				command: `DROP DATABASE db0`,
				exp:     `FIXME`,
			},
		},
	}

	for _, query := range test.queries {
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := s.Execute(query); err != nil {
			t.Fatal(query.Error(err))
		} else if !query.success() {
			t.Fatal(query.failureMessage())
		}
	}
}

// Ensure retention policy commands work.
func TestServer_RetentionPolicyCommands(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	// Create a database.
	if _, err := s.MetaStore.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	test := Test{
		queries: []*Query{
			&Query{
				name:    "create retention policy should succeed",
				command: `CREATE RETENTION POLICY rp0 ON db0 DURATION 1h REPLICATION 1`,
				exp:     `{"results":[{}]}`,
			},
			&Query{
				name:    "create retention policy should fail if it already exists",
				command: `CREATE RETENTION POLICY rp0 ON db0 DURATION 1h REPLICATION 1`,
				exp:     `{"results":[{"error":"retention policy already exists"}]}`,
			},
			&Query{
				name:    "show retention policy should succeed",
				command: `SHOW RETENTION POLICIES db0`,
				exp:     `{"results":[{"series":[{"columns":["name","duration","replicaN","default"],"values":[["rp0","1h0m0s",1,false]]}]}]}`,
			},
			&Query{
				name:    "alter retention policy should succeed",
				command: `ALTER RETENTION POLICY rp0 ON db0 DURATION 2h REPLICATION 3 DEFAULT`,
				exp:     `{"results":[{}]}`,
			},
			&Query{
				name:    "show retention policy should have new altered information",
				command: `SHOW RETENTION POLICIES db0`,
				exp:     `{"results":[{"series":[{"columns":["name","duration","replicaN","default"],"values":[["rp0","2h0m0s",3,true]]}]}]}`,
			},
			&Query{
				name:    "drop retention policy should succeed",
				command: `DROP RETENTION POLICY rp0 ON db0`,
				exp:     `{"results":[{}]}`,
			},
			&Query{
				name:    "show retention policy should be empty after dropping them",
				command: `SHOW RETENTION POLICIES db0`,
				exp:     `{"results":[{"series":[{"columns":["name","duration","replicaN","default"]}]}]}`,
			},
		},
	}

	for _, query := range test.queries {
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := s.Execute(query); err != nil {
			t.Fatal(query.Error(err))
		} else if !query.success() {
			t.Fatal(query.failureMessage())
		}
	}
}

// Ensure the server can create a single point via json protocol and read it back.
func TestServer_Write_JSON(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", newRetentionPolicyInfo("rp0", 1, 1*time.Hour)); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	if res, err := s.Write("", "", fmt.Sprintf(`{"database" : "db0", "retentionPolicy" : "rp0", "points": [{"measurement": "cpu", "tags": {"host": "server02"},"fields": {"value": 1.0}}],"time":"%s"} `, now.Format(time.RFC3339Nano))); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.cpu`); err != nil {
		t.Fatal(err)
	} else if exp := fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","tags":{"host":"server02"},"columns":["time","value"],"values":[["%s",1]]}]}]}`, now.Format(time.RFC3339Nano)); exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

// Ensure the server can create a single point via line protocol with float type and read it back.
func TestServer_Write_LineProtocol_Float(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", newRetentionPolicyInfo("rp0", 1, 1*time.Hour)); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	if res, err := s.Write("db0", "rp0", `cpu,host=server01 value=1.0 `+strconv.FormatInt(now.UnixNano(), 10)); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.cpu`); err != nil {
		t.Fatal(err)
	} else if exp := fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["%s",1]]}]}]}`, now.Format(time.RFC3339Nano)); exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

// Ensure the server can create a single point via line protocol with bool type and read it back.
func TestServer_Write_LineProtocol_Bool(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", newRetentionPolicyInfo("rp0", 1, 1*time.Hour)); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	if res, err := s.Write("db0", "rp0", `cpu,host=server01 value=true `+strconv.FormatInt(now.UnixNano(), 10)); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.cpu`); err != nil {
		t.Fatal(err)
	} else if exp := fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["%s",true]]}]}]}`, now.Format(time.RFC3339Nano)); exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

// Ensure the server can create a single point via line protocol with string type and read it back.
func TestServer_Write_LineProtocol_String(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", newRetentionPolicyInfo("rp0", 1, 1*time.Hour)); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	if res, err := s.Write("db0", "rp0", `cpu,host=server01 value="disk full" `+strconv.FormatInt(now.UnixNano(), 10)); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.cpu`); err != nil {
		t.Fatal(err)
	} else if exp := fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["%s","disk full"]]}]}]}`, now.Format(time.RFC3339Nano)); exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

// Ensure the server can create a single point via line protocol with integer type and read it back.
func TestServer_Write_LineProtocol_Integer(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", newRetentionPolicyInfo("rp0", 1, 1*time.Hour)); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	if res, err := s.Write("db0", "rp0", `cpu,host=server01 value=100 `+strconv.FormatInt(now.UnixNano(), 10)); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.cpu`); err != nil {
		t.Fatal(err)
	} else if exp := fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["%s",100]]}]}]}`, now.Format(time.RFC3339Nano)); exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

// Ensure the server can query with the count aggregate function
func TestServer_Query_Count(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", newRetentionPolicyInfo("rp0", 1, 1*time.Hour)); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	if res, err := s.Write("db0", "rp0", `cpu,host=server01 value=1.0 `+strconv.FormatInt(now.UnixNano(), 10)); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	// Verify the data was written.
	if res, err := s.Query(`SELECT count(value) FROM db0.rp0.cpu`); err != nil {
		t.Fatal(err)
	} else if exp := `{"results":[{"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

// Ensure the server can query with Now().
func TestServer_Query_Now(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", newRetentionPolicyInfo("rp0", 1, 1*time.Hour)); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	if res, err := s.Write("db0", "rp0", `cpu,host=server01 value=1.0 `+strconv.FormatInt(now.UnixNano(), 10)); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.cpu where time < now()`); err != nil {
		t.Fatal(err)
	} else if exp := fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["%s",1]]}]}]}`, now.Format(time.RFC3339Nano)); exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

// Ensure the server can query with epoch precisions.
func TestServer_Query_EpochPrecision(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", newRetentionPolicyInfo("rp0", 1, 1*time.Hour)); err != nil {
		t.Fatal(err)
	}

	now := now()
	test := Test{
		db:    "db0",
		rp:    "rp0",
		write: `cpu,host=server01 value=1.0 ` + strconv.FormatInt(now.UnixNano(), 10),
		queries: []*Query{
			&Query{
				name:    "nanosecond precision",
				command: `SELECT * FROM db0.rp0.cpu`,
				params:  url.Values{"epoch": []string{"n"}},
				exp:     fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[[%d,1]]}]}]}`, now.UnixNano()),
			},
			&Query{
				name:    "microsecond precision",
				command: `SELECT * FROM db0.rp0.cpu`,
				params:  url.Values{"epoch": []string{"u"}},
				exp:     fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[[%d,1]]}]}]}`, now.UnixNano()/int64(time.Microsecond)),
			},
			&Query{
				name:    "millisecond precision",
				command: `SELECT * FROM db0.rp0.cpu`,
				params:  url.Values{"epoch": []string{"ms"}},
				exp:     fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[[%d,1]]}]}]}`, now.UnixNano()/int64(time.Millisecond)),
			},
			&Query{
				name:    "second precision",
				command: `SELECT * FROM db0.rp0.cpu`,
				params:  url.Values{"epoch": []string{"s"}},
				exp:     fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[[%d,1]]}]}]}`, now.UnixNano()/int64(time.Second)),
			},
			&Query{
				name:    "minute precision",
				command: `SELECT * FROM db0.rp0.cpu`,
				params:  url.Values{"epoch": []string{"m"}},
				exp:     fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[[%d,1]]}]}]}`, now.UnixNano()/int64(time.Minute)),
			},
			&Query{
				name:    "hour precision",
				command: `SELECT * FROM db0.rp0.cpu`,
				params:  url.Values{"epoch": []string{"h"}},
				exp:     fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[[%d,1]]}]}]}`, now.UnixNano()/int64(time.Hour)),
			},
		},
	}

	if res, err := s.Write(test.db, test.rp, test.write); err != nil {
		t.Fatal(err)
	} else if test.exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", test.exp, res)
	}

	for _, query := range test.queries {
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := s.Execute(query); err != nil {
			t.Fatal(query.Error(err))
		} else if !query.success() {
			t.Fatal(query.failureMessage())
		}
	}
}

// Ensure the server works with tag queries.
func TestServer_Query_Tags(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", newRetentionPolicyInfo("rp0", 1, 1*time.Hour)); err != nil {
		t.Fatal(err)
	}

	now := now()
	test := Test{
		db:    "db0",
		rp:    "rp0",
		write: fmt.Sprintf("cpu,host=server01 value=100,core=4 %s\ncpu,host=server02 value=50,core=2 %s", strconv.FormatInt(now.UnixNano(), 10), strconv.FormatInt(now.Add(1).UnixNano(), 10)),
		queries: []*Query{
			&Query{
				name:    "tag without field should return error",
				command: `SELECT host FROM db0.rp0.cpu`,
				exp:     `{"results":[{"error":"select statement must include at least one field or function call"}]}`,
			},
			&Query{
				name:    "field with tag should succeed",
				command: `SELECT host, value FROM db0.rp0.cpu`,
				exp:     fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["%s",100]]},{"name":"cpu","tags":{"host":"server02"},"columns":["time","value"],"values":[["%s",50]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
			},
			&Query{
				name:    "field with two tags should succeed",
				command: `SELECT host, value, core FROM db0.rp0.cpu`,
				exp:     fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value","core"],"values":[["%s",100,4]]},{"name":"cpu","tags":{"host":"server02"},"columns":["time","value","core"],"values":[["%s",50,2]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
			},
			&Query{
				name:    "select * with tags should succeed",
				command: `SELECT * FROM db0.rp0.cpu`,
				exp:     fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","core","value"],"values":[["%s",4,100]]},{"name":"cpu","tags":{"host":"server02"},"columns":["time","core","value"],"values":[["%s",2,50]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
			},
		},
	}

	if res, err := s.Write(test.db, test.rp, test.write); err != nil {
		t.Fatal(err)
	} else if test.exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", test.exp, res)
	}

	for _, query := range test.queries {
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := s.Execute(query); err != nil {
			t.Fatal(query.Error(err))
		} else if !query.success() {
			t.Fatal(query.failureMessage())
		}
	}
}

// Ensure the server will succeed and error for common scenarios.
func TestServer_Query_Common(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", newRetentionPolicyInfo("rp0", 1, 1*time.Hour)); err != nil {
		t.Fatal(err)
	}

	now := now()
	test := Test{
		db:    "db0",
		rp:    "rp0",
		write: fmt.Sprintf("cpu,host=server01 value=1 %s", strconv.FormatInt(now.UnixNano(), 10)),
		queries: []*Query{
			&Query{
				name:    "selecting a valid  measurement and field should succeed",
				command: `SELECT value FROM db0.rp0.cpu`,
				exp:     fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["%s",1]]}]}]}`, now.Format(time.RFC3339Nano)),
			},
			&Query{
				name:    "selecting a measurement that doesn't exist should fail",
				command: `SELECT value FROM db0.rp0.idontexist`,
				exp:     `.*measurement not found*`,
				pattern: true,
			},
			&Query{
				name:    "selecting a field that doesn't exist should fail",
				command: `SELECT idontexist FROM db0.rp0.cpu`,
				exp:     `{"results":[{"error":"unknown field or tag name in select clause: idontexist"}]}`,
			},
			&Query{
				skip:    true,
				name:    "no results should return an empty result - FIXME pauldix",
				command: `SELECT value FROM db0.rp0.cpu where time > now()`,
				exp:     `{"results":[{}]}`,
			},
		},
	}

	if res, err := s.Write(test.db, test.rp, test.write); err != nil {
		t.Fatal(err)
	} else if test.exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", test.exp, res)
	}

	for _, query := range test.queries {
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := s.Execute(query); err != nil {
			t.Fatal(query.Error(err))
		} else if !query.success() {
			t.Fatal(query.failureMessage())
		}
	}

}

// Ensure the server can query two points.
func TestServer_Query_SelectTwoPoints(t *testing.T) {
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
	if res, err := s.Write("db0", "rp0", fmt.Sprintf("cpu value=100 %s\ncpu value=200 %s", strconv.FormatInt(now.UnixNano(), 10), strconv.FormatInt(now.Add(1).UnixNano(), 10))); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.cpu`); err != nil {
		t.Fatal(err)
	} else if exp := fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["%s",100],["%s",200]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)); exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
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

// CreateDatabaseAndRetentionPolicy will create the datbase and retnetion policy.
func (s *Server) CreateDatabaseAndRetentionPolicy(db string, rp *meta.RetentionPolicyInfo) error {
	if _, err := s.MetaStore.CreateDatabase(db); err != nil {
		return err
	} else if _, err := s.MetaStore.CreateRetentionPolicy(db, rp); err != nil {
		return err
	}
	return nil
}

// Execute takes a Query and executes it
func (s *Server) Execute(q *Query) (err error) {
	if q.params == nil {
		q.act, err = s.Query(q.command)
		return
	}
	q.act, err = s.QueryWithParams(q.command, q.params)
	return
}

// Query executes a query against the server and returns the results.
func (s *Server) Query(query string) (results string, err error) {
	return s.QueryWithParams(query, nil)
}

// Query executes a query against the server and returns the results.
func (s *Server) QueryWithParams(query string, values url.Values) (results string, err error) {
	if values == nil {
		values = url.Values{}
	}
	values.Set("q", query)
	resp, err := http.Get(s.URL() + "/query?" + values.Encode())
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

func newRetentionPolicyInfo(name string, rf int, duration time.Duration) *meta.RetentionPolicyInfo {
	return &meta.RetentionPolicyInfo{Name: name, ReplicaN: rf, Duration: duration}
}

func now() time.Time {
	return time.Now().UTC()
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

func expectPattern(exp, act string) bool {
	re := regexp.MustCompile(exp)
	if !re.MatchString(act) {
		return false
	}
	return true
}

type Query struct {
	name     string
	command  string
	params   url.Values
	exp, act string
	pattern  bool
	skip     bool
}

func (q *Query) success() bool {
	if q.pattern {
		return expectPattern(q.exp, q.act)
	}
	return q.exp == q.act
}

func (q *Query) Error(err error) string {
	return fmt.Sprintf("%s: %v", q.name, err)
}

func (q *Query) failureMessage() string {
	return fmt.Sprintf("%s: unexpected results for query: %s\nexp:    %s\nactual: %s\n", q.name, q.command, q.exp, q.act)
}

type Test struct {
	write   string
	db      string
	rp      string
	exp     string
	queries []*Query
}
