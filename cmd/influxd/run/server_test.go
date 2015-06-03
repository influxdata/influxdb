package run_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
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
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
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
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
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

// Ensure the server can query with default databases (via param) and default retention policy
func TestServer_Query_DefaultDBAndRP(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", newRetentionPolicyInfo("rp0", 1, 1*time.Hour)); err != nil {
		t.Fatal(err)
	}

	if err := s.MetaStore.SetDefaultRetentionPolicy("db0", "rp0"); err != nil {
		t.Fatal(err)
	}

	test := NewTest("db0", "rp0")
	test.write = fmt.Sprintf(`cpu value=1.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z").UnixNano())

	test.addQueries([]*Query{
		&Query{
			name:    "default db and rp",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM cpu`,
			exp:     `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T01:00:00Z",1]]}]}]}`,
		},
		&Query{
			skip:    true,
			name:    "default rp - FIXME pauldix",
			command: `SELECT * FROM db0..cpu`,
			exp:     `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T01:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "default dp",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM rp0.cpu`,
			exp:     `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T01:00:00Z",1]]}]}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
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

	test := NewTest("db0", "rp0")
	test.write = `cpu,host=server01 value=1.0 ` + strconv.FormatInt(now.UnixNano(), 10)

	test.addQueries([]*Query{
		&Query{
			name:    "selecting count(value) should succeed",
			command: `SELECT count(value) FROM db0.rp0.cpu`,
			exp:     `{"results":[{"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "selecting count(*) should fail",
			command: `SELECT count(*) FROM db0.rp0.cpu`,
			exp:     `{"results":[{"error":"expected field argument in count()"}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
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

	test := NewTest("db0", "rp0")
	test.write = `cpu,host=server01 value=1.0 ` + strconv.FormatInt(now.UnixNano(), 10)

	test.addQueries([]*Query{
		&Query{
			name:    "where with time < now() should work",
			command: `SELECT * FROM db0.rp0.cpu where time < now()`,
			exp:     fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["%s",1]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			skip:    true,
			name:    "where with time > now() should return an empty result - FIXME pauldix",
			command: `SELECT * FROM db0.rp0.cpu where time > now()`,
			exp:     `{"results":[{}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
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

	now := time.Now().UTC()

	test := NewTest("db0", "rp0")
	test.write = `cpu,host=server01 value=1.0 ` + strconv.FormatInt(now.UnixNano(), 10)

	test.addQueries([]*Query{
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
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
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

	now := time.Now().UTC()

	test := NewTest("db0", "rp0")
	test.write = fmt.Sprintf("cpu,host=server01 value=100,core=4 %s\ncpu,host=server02 value=50,core=2 %s", strconv.FormatInt(now.UnixNano(), 10), strconv.FormatInt(now.Add(1).UnixNano(), 10))

	test.addQueries([]*Query{
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
		&Query{
			skip:    true,
			name:    "group by tag",
			command: `SELECT value FROM db0.rp0.cpu GROUP by host`,
			exp:     `FIXME pauldix`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
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

	now := time.Now().UTC()

	test := NewTest("db0", "rp0")
	test.write = fmt.Sprintf("cpu,host=server01 value=1 %s", strconv.FormatInt(now.UnixNano(), 10))

	test.addQueries([]*Query{
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
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

// Ensure the server can query two points.
func TestServer_Query_SelectTwoPoints(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", newRetentionPolicyInfo("rp0", 1, 1*time.Hour)); err != nil {
		t.Fatal(err)
	}

	now := now()

	test := NewTest("db0", "rp0")
	test.write = fmt.Sprintf("cpu value=100 %s\ncpu value=200 %s", strconv.FormatInt(now.UnixNano(), 10), strconv.FormatInt(now.Add(1).UnixNano(), 10))

	test.addQueries(&Query{
		name:    "selecting two points should result in two points",
		command: `SELECT * FROM db0.rp0.cpu`,
		exp:     fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["%s",100],["%s",200]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
	})

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

// Ensure the server can query two negative points.
func TestServer_Query_SelectTwoNegativePoints(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", newRetentionPolicyInfo("rp0", 1, 1*time.Hour)); err != nil {
		t.Fatal(err)
	}

	now := now()

	test := NewTest("db0", "rp0")
	test.write = fmt.Sprintf("cpu value=-100 %s\ncpu value=-200 %s", strconv.FormatInt(now.UnixNano(), 10), strconv.FormatInt(now.Add(1).UnixNano(), 10))

	test.addQueries(&Query{
		name:    "selecting two negative points should succeed",
		command: `SELECT * FROM db0.rp0.cpu`,
		exp:     fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["%s",-100],["%s",-200]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
	})

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

// Ensure the server can query with relative time.
func TestServer_Query_SelectRelativeTime(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", newRetentionPolicyInfo("rp0", 1, 1*time.Hour)); err != nil {
		t.Fatal(err)
	}

	now := now()
	yesterday := yesterday()

	test := NewTest("db0", "rp0")
	test.write = fmt.Sprintf("cpu,host=server01 value=100 %s\ncpu,host=server01 value=200 %s", strconv.FormatInt(yesterday.UnixNano(), 10), strconv.FormatInt(now.UnixNano(), 10))

	test.addQueries([]*Query{
		&Query{
			name:    "single point with time pre-calculated for past time queries yesterday",
			command: `SELECT * FROM db0.rp0.cpu where time >= '` + yesterday.Add(-1*time.Minute).Format(time.RFC3339Nano) + `'`,
			exp:     fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["%s",100],["%s",200]]}]}]}`, yesterday.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "single point with time pre-calculated for relative time queries now",
			command: `SELECT * FROM db0.rp0.cpu where time >= now() - 1m`,
			exp:     fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["%s",200]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

// mergeMany ensures that when merging many series together and some of them have a different number
// of points than others in a group by interval the results are correct
func TestServer_Query_MergeMany(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	// set infinite retention policy as we are inserting data in the past and don't want retention policy enforcement to make this test racy
	if err := s.CreateDatabaseAndRetentionPolicy("db0", newRetentionPolicyInfo("rp0", 1, 0)); err != nil {
		t.Fatal(err)
	}

	test := NewTest("db0", "rp0")

	writes := []string{}
	for i := 1; i < 11; i++ {
		for j := 1; j < 5+i%3; j++ {
			data := fmt.Sprintf(`cpu,host=server_%d value=22 %d`, i, time.Unix(int64(j), int64(0)).UTC().UnixNano())
			writes = append(writes, data)
		}
	}
	test.write = strings.Join(writes, "\n")

	test.addQueries([]*Query{
		&Query{
			name:    "GROUP by time",
			command: `SELECT count(value) FROM db0.rp0.cpu WHERE time >= '1970-01-01T00:00:01Z' AND time <= '1970-01-01T00:00:06Z' GROUP BY time(1s)`,
			exp:     `{"results":[{"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:01Z",10],["1970-01-01T00:00:02Z",10],["1970-01-01T00:00:03Z",10],["1970-01-01T00:00:04Z",10],["1970-01-01T00:00:05Z",7],["1970-01-01T00:00:06Z",3]]}]}]}`,
		},
		&Query{
			skip:    true,
			name:    "GROUP by tag - FIXME pauldix",
			command: `SELECT count(value) FROM db0.rp0.cpu where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T02:00:00Z' group by host`,
			exp:     `{"results":[{"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","count"],"values":[["2000-01-01T00:00:00Z",1]]},{"name":"cpu","tags":{"host":"server02"},"columns":["time","count"],"values":[["2000-01-01T00:00:00Z",1]]},{"name":"cpu","tags":{"host":"server03"},"columns":["time","count"],"values":[["2000-01-01T00:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "GROUP by field",
			command: `SELECT count(value) FROM db0.rp0.cpu where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T02:00:00Z' group by value`,
			exp:     `{"results":[{"error":"can not use field in group by clause: value"}]}`,
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_Query_LimitAndOffset(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	// set infinite retention policy as we are inserting data in the past and don't want retention policy enforcement to make this test racy
	if err := s.CreateDatabaseAndRetentionPolicy("db0", newRetentionPolicyInfo("rp0", 1, 0)); err != nil {
		t.Fatal(err)
	}

	test := NewTest("db0", "rp0")

	writes := []string{}
	for i := 1; i < 10; i++ {
		data := fmt.Sprintf(`cpu,region=us-east,host=server-%d value=%d %d`, i, i, time.Unix(int64(i), int64(0)).UnixNano())
		writes = append(writes, data)
	}
	test.write = strings.Join(writes, "\n")

	test.addQueries([]*Query{
		&Query{
			name:    "SLIMIT 2 SOFFSET 1",
			command: `SELECT count(value) FROM db0.rp0.cpu GROUP BY * SLIMIT 2 SOFFSET 1`,
			exp:     `{"results":[{"series":[{"name":"cpu","tags":{"host":"server-2","region":"us-east"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]},{"name":"cpu","tags":{"host":"server-3","region":"us-east"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "SLIMIT 2 SOFFSET 3",
			command: `SELECT count(value) FROM db0.rp0.cpu GROUP BY * SLIMIT 2 SOFFSET 3`,
			exp:     `{"results":[{"series":[{"name":"cpu","tags":{"host":"server-4","region":"us-east"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]},{"name":"cpu","tags":{"host":"server-5","region":"us-east"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "SLIMIT 3 SOFFSET 8",
			command: `SELECT count(value) FROM db0.rp0.cpu GROUP BY * SLIMIT 3 SOFFSET 8`,
			exp:     `{"results":[{"series":[{"name":"cpu","tags":{"host":"server-9","region":"us-east"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_Query_Regex(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", newRetentionPolicyInfo("rp0", 1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := s.MetaStore.SetDefaultRetentionPolicy("db0", "rp0"); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu1,host=server01 value=10 %d`, mustParseTime(time.RFC3339Nano, "2015-02-28T01:03:36.703820946Z").UnixNano()),
		fmt.Sprintf(`cpu2,host=server01 value=20 %d`, mustParseTime(time.RFC3339Nano, "2015-02-28T01:03:36.703820946Z").UnixNano()),
		fmt.Sprintf(`cpu3,host=server01 value=30 %d`, mustParseTime(time.RFC3339Nano, "2015-02-28T01:03:36.703820946Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.write = strings.Join(writes, "\n")

	test.addQueries([]*Query{
		&Query{
			name:    "default db and rp",
			command: `SELECT * FROM /cpu[13]/`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"series":[{"name":"cpu1","tags":{"host":"server01"},"columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",10]]},{"name":"cpu3","tags":{"host":"server01"},"columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",30]]}]}]}`,
		},
		&Query{
			name:    "specifying db and rp",
			command: `SELECT * FROM db0.rp0./cpu[13]/`,
			exp:     `{"results":[{"series":[{"name":"cpu1","tags":{"host":"server01"},"columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",10]]},{"name":"cpu3","tags":{"host":"server01"},"columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",30]]}]}]}`,
		},
		&Query{
			name:    "default db and specified rp",
			command: `SELECT * FROM rp0./cpu[13]/`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"series":[{"name":"cpu1","tags":{"host":"server01"},"columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",10]]},{"name":"cpu3","tags":{"host":"server01"},"columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",30]]}]}]}`,
		},
		&Query{
			skip:    true,
			name:    "specified db and default rp - FIXME pauldix",
			command: `SELECT * FROM db0../cpu[13]/`,
			exp:     `{"results":[{"series":[{"name":"cpu1","tags":{"host":"server01"},"columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",10]]},{"name":"cpu3","tags":{"host":"server01"},"columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",30]]}]}]}`,
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

// Server represents a test wrapper for run.Server.
type Server struct {
	*run.Server
	Config *run.Config
}

// NewServer returns a new instance of Server.
func NewServer(c *run.Config, joinURLs string) *Server {

	s := Server{
		Server: run.NewServer(c, joinURLs),
		Config: c,
	}
	// Set the logger to discard unless verbose is on
	if !testing.Verbose() {
		type logSetter interface {
			SetLogger(*log.Logger)
		}
		nullLogger := log.New(ioutil.Discard, "", 0)
		s.MetaStore.Logger = nullLogger
		s.TSDBStore.Logger = nullLogger
		for _, service := range s.Services {
			if service, ok := service.(logSetter); ok {
				service.SetLogger(nullLogger)
			}
		}
	}

	return &s
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
	c.HTTPD.LogEnabled = testing.Verbose()
	return c
}

func newRetentionPolicyInfo(name string, rf int, duration time.Duration) *meta.RetentionPolicyInfo {
	return &meta.RetentionPolicyInfo{Name: name, ReplicaN: rf, Duration: duration}
}

func now() time.Time {
	return time.Now().UTC()
}

func yesterday() time.Time {
	return now().Add(-1 * time.Hour * 24)
}

func mustParseTime(layout, value string) time.Time {
	tm, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return tm
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

// Execute runs the command and returns an err if it fails
func (q *Query) Execute(s *Server) (err error) {
	if q.params == nil {
		q.act, err = s.Query(q.command)
		return
	}
	q.act, err = s.QueryWithParams(q.command, q.params)
	return
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
	initialized bool
	write       string
	db          string
	rp          string
	exp         string
	queries     []*Query
}

func NewTest(db, rp string) Test {
	return Test{
		db: db,
		rp: rp,
	}
}

func (t *Test) addQueries(q ...*Query) {
	t.queries = append(t.queries, q...)
}

func (t *Test) init(s *Server) error {
	if t.write == "" || t.initialized {
		return nil
	}
	t.initialized = true
	if res, err := s.Write(t.db, t.rp, t.write); err != nil {
		return err
	} else if t.exp != res {
		return fmt.Errorf("unexpected results\nexp: %s\ngot: %s\n", t.exp, res)
	}
	return nil
}
