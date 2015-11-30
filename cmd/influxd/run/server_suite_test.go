package run_test

import (
	"fmt"
	"net/url"
	"testing"
	"time"
)

var tests Tests

// Load all shared tests
func init() {
	tests = make(map[string]Test)

	tests["database_commands"] = Test{
		queries: []*Query{
			&Query{
				name:    "create database should succeed",
				command: `CREATE DATABASE db0`,
				exp:     `{"results":[{}]}`,
				once:    true,
			},
			&Query{
				name:    "create database with retention duration should succeed",
				command: `CREATE DATABASE db0_r WITH DURATION 24h REPLICATION 2 NAME db0_r_policy`,
				exp:     `{"results":[{}]}`,
				once:    true,
			},
			&Query{
				name:    "create database should error with bad name",
				command: `CREATE DATABASE 0xdb0`,
				exp:     `{"error":"error parsing query: found 0, expected identifier at line 1, char 17"}`,
			},
			&Query{
				name:    "create database with retention duration should error with bad retention duration",
				command: `CREATE DATABASE db0 WITH DURATION xyz`,
				exp:     `{"error":"error parsing query: found xyz, expected duration at line 1, char 35"}`,
			},
			&Query{
				name:    "create database with retention replication should error with bad retention replication number",
				command: `CREATE DATABASE db0 WITH REPLICATION xyz`,
				exp:     `{"error":"error parsing query: found xyz, expected number at line 1, char 38"}`,
			},
			&Query{
				name:    "create database with retention name should error with missing retention name",
				command: `CREATE DATABASE db0 WITH NAME`,
				exp:     `{"error":"error parsing query: found EOF, expected identifier at line 1, char 31"}`,
			},
			&Query{
				name:    "show database should succeed",
				command: `SHOW DATABASES`,
				exp:     `{"results":[{"series":[{"name":"databases","columns":["name"],"values":[["db0"],["db0_r"]]}]}]}`,
			},
			&Query{
				name:    "create database should error if it already exists",
				command: `CREATE DATABASE db0`,
				exp:     `{"results":[{"error":"database already exists"}]}`,
			},
			&Query{
				name:    "create database should error if it already exists",
				command: `CREATE DATABASE db0_r`,
				exp:     `{"results":[{"error":"database already exists"}]}`,
			},
			&Query{
				name:    "create database should not error with existing database with IF NOT EXISTS",
				command: `CREATE DATABASE IF NOT EXISTS db0`,
				exp:     `{"results":[{}]}`,
			},
			&Query{
				name:    "create database should create non-existing database with IF NOT EXISTS",
				command: `CREATE DATABASE IF NOT EXISTS db1`,
				exp:     `{"results":[{}]}`,
			},
			&Query{
				name:    "create database with retention duration should not error with existing database with IF NOT EXISTS",
				command: `CREATE DATABASE IF NOT EXISTS db1 WITH DURATION 24h`,
				exp:     `{"results":[{}]}`,
			},
			&Query{
				name:    "create database should error IF NOT EXISTS with bad retention duration",
				command: `CREATE DATABASE IF NOT EXISTS db1 WITH DURATION xyz`,
				exp:     `{"error":"error parsing query: found xyz, expected duration at line 1, char 49"}`,
			},
			&Query{
				name:    "show database should succeed",
				command: `SHOW DATABASES`,
				exp:     `{"results":[{"series":[{"name":"databases","columns":["name"],"values":[["db0"],["db0_r"],["db1"]]}]}]}`,
			},
			&Query{
				name:    "drop database db0 should succeed",
				command: `DROP DATABASE db0`,
				exp:     `{"results":[{}]}`,
				once:    true,
			},
			&Query{
				name:    "drop database db0_r should succeed",
				command: `DROP DATABASE db0_r`,
				exp:     `{"results":[{}]}`,
				once:    true,
			},
			&Query{
				name:    "drop database db1 should succeed",
				command: `DROP DATABASE db1`,
				exp:     `{"results":[{}]}`,
				once:    true,
			},
			&Query{
				name:    "drop database should error if it does not exists",
				command: `DROP DATABASE db1`,
				exp:     `{"results":[{"error":"database not found: db1"}]}`,
			},
			&Query{
				name:    "drop database should not error with non-existing database db1 WITH IF EXISTS",
				command: `DROP DATABASE IF EXISTS db1`,
				exp:     `{"results":[{}]}`,
			},
			&Query{
				name:    "show database should have no results",
				command: `SHOW DATABASES`,
				exp:     `{"results":[{"series":[{"name":"databases","columns":["name"]}]}]}`,
			},
			&Query{
				name:    "drop database should error if it doesn't exist",
				command: `DROP DATABASE db0`,
				exp:     `{"results":[{"error":"database not found: db0"}]}`,
			},
		},
	}

	tests["drop_and_recreate_database"] = Test{
		write: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		queries: []*Query{
			&Query{
				name:    "Drop database after data write",
				command: `DROP DATABASE db0`,
				exp:     `{"results":[{}]}`,
				once:    true,
			},
			&Query{
				name:    "Recreate database",
				command: `CREATE DATABASE db0`,
				exp:     `{"results":[{}]}`,
				once:    true,
			},
			&Query{
				name:    "Recreate retention policy",
				command: `CREATE RETENTION POLICY rp0 ON db0 DURATION 365d REPLICATION 1 DEFAULT`,
				exp:     `{"results":[{}]}`,
				once:    true,
			},
			&Query{
				name:    "Show measurements after recreate",
				command: `SHOW MEASUREMENTS`,
				exp:     `{"results":[{}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Query data after recreate",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
		},
	}

	tests["drop_database_isolated"] = Test{
		db:    "db0",
		rp:    "rp0",
		write: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		queries: []*Query{
			&Query{
				name:    "Query data from 1st database",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["2000-01-01T00:00:00Z","serverA","uswest",23.2]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Query data from 1st database with GROUP BY *",
				command: `SELECT * FROM cpu GROUP BY *`,
				exp:     `{"results":[{"series":[{"name":"cpu","tags":{"host":"serverA","region":"uswest"},"columns":["time","val"],"values":[["2000-01-01T00:00:00Z",23.2]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Drop other database",
				command: `DROP DATABASE db1`,
				once:    true,
				exp:     `{"results":[{}]}`,
			},
			&Query{
				name:    "Query data from 1st database and ensure it's still there",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["2000-01-01T00:00:00Z","serverA","uswest",23.2]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Query data from 1st database and ensure it's still there with GROUP BY *",
				command: `SELECT * FROM cpu GROUP BY *`,
				exp:     `{"results":[{"series":[{"name":"cpu","tags":{"host":"serverA","region":"uswest"},"columns":["time","val"],"values":[["2000-01-01T00:00:00Z",23.2]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
		},
	}

}

func (tests Tests) load(t *testing.T, key string) Test {
	test, ok := tests[key]
	if !ok {
		t.Fatalf("no test %q", key)
	}

	return test.duplicate()
}
