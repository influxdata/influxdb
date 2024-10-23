package tests

import (
	"fmt"
	"net/url"
	"strings"
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
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "create database with retention duration should succeed",
				command: `CREATE DATABASE db0_r WITH DURATION 24h REPLICATION 2 NAME db0_r_policy`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "create database with retention policy should fail with invalid name",
				command: `CREATE DATABASE db1 WITH NAME "."`,
				exp:     `{"results":[{"statement_id":0,"error":"invalid name"}]}`,
				once:    true,
			},
			&Query{
				name:    "create database should error with some unquoted names",
				command: `CREATE DATABASE 0xdb0`,
				exp:     `{"error":"error parsing query: found 0xdb0, expected identifier at line 1, char 17"}`,
			},
			&Query{
				name:    "create database should error with invalid characters",
				command: `CREATE DATABASE "."`,
				exp:     `{"results":[{"statement_id":0,"error":"invalid name"}]}`,
			},
			&Query{
				name:    "create database with retention duration should error with bad retention duration",
				command: `CREATE DATABASE db0 WITH DURATION xyz`,
				exp:     `{"error":"error parsing query: found xyz, expected duration at line 1, char 35"}`,
			},
			&Query{
				name:    "create database with retention replication should error with bad retention replication number",
				command: `CREATE DATABASE db0 WITH REPLICATION xyz`,
				exp:     `{"error":"error parsing query: found xyz, expected integer at line 1, char 38"}`,
			},
			&Query{
				name:    "create database with retention name should error with missing retention name",
				command: `CREATE DATABASE db0 WITH NAME`,
				exp:     `{"error":"error parsing query: found EOF, expected identifier at line 1, char 31"}`,
			},
			&Query{
				name:    "show database should succeed",
				command: `SHOW DATABASES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"],"values":[["db0"],["db0_r"]]}]}]}`,
			},
			&Query{
				name:    "create database should not error with existing database",
				command: `CREATE DATABASE db0`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "create database should create non-existing database",
				command: `CREATE DATABASE db1`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "create database with retention duration should error if retention policy is different",
				command: `CREATE DATABASE db1 WITH DURATION 24h`,
				exp:     `{"results":[{"statement_id":0,"error":"retention policy conflicts with an existing policy"}]}`,
			},
			&Query{
				name:    "create database should error with bad retention duration",
				command: `CREATE DATABASE db1 WITH DURATION xyz`,
				exp:     `{"error":"error parsing query: found xyz, expected duration at line 1, char 35"}`,
			},
			&Query{
				name:    "show database should succeed",
				command: `SHOW DATABASES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"],"values":[["db0"],["db0_r"],["db1"]]}]}]}`,
			},
			&Query{
				name:    "drop database db0 should succeed",
				command: `DROP DATABASE db0`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "drop database db0_r should succeed",
				command: `DROP DATABASE db0_r`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "drop database db1 should succeed",
				command: `DROP DATABASE db1`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "drop database should not error if it does not exists",
				command: `DROP DATABASE db1`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "drop database should not error with non-existing database db1",
				command: `DROP DATABASE db1`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "show database should have no results",
				command: `SHOW DATABASES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"]}]}]}`,
			},
			&Query{
				name:    "create database with shard group duration should succeed",
				command: `CREATE DATABASE db0 WITH SHARD DURATION 61m`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "create database with shard group duration and duration should succeed",
				command: `CREATE DATABASE db1 WITH DURATION 60m SHARD DURATION 30m`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
		},
	}

	tests["drop_and_recreate_database"] = Test{
		db: "db0",
		rp: "rp0",
		writes: Writes{
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
		},
		queries: []*Query{
			&Query{
				name:    "Drop database after data write",
				command: `DROP DATABASE db0`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "Recreate database",
				command: `CREATE DATABASE db0`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "Recreate retention policy",
				command: `CREATE RETENTION POLICY rp0 ON db0 DURATION 365d REPLICATION 1 DEFAULT`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "Show measurements after recreate",
				command: `SHOW MEASUREMENTS`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Query data after recreate",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
		},
	}

	tests["drop_database_isolated"] = Test{
		db: "db0",
		rp: "rp0",
		writes: Writes{
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
		},
		queries: []*Query{
			&Query{
				name:    "Query data from 1st database",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["2000-01-01T00:00:00Z","serverA","uswest",23.2]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Query data from 1st database with GROUP BY *",
				command: `SELECT * FROM cpu GROUP BY *`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"serverA","region":"uswest"},"columns":["time","val"],"values":[["2000-01-01T00:00:00Z",23.2]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Drop other database",
				command: `DROP DATABASE db1`,
				once:    true,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "Query data from 1st database and ensure it's still there",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["2000-01-01T00:00:00Z","serverA","uswest",23.2]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Query data from 1st database and ensure it's still there with GROUP BY *",
				command: `SELECT * FROM cpu GROUP BY *`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"serverA","region":"uswest"},"columns":["time","val"],"values":[["2000-01-01T00:00:00Z",23.2]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
		},
	}

	tests["delete_series_time"] = Test{
		db: "db0",
		rp: "rp0",
		writes: Writes{
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=100 %d`, mustParseTime(time.RFC3339Nano, "2000-01-02T00:00:00Z").UnixNano())},
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=200 %d`, mustParseTime(time.RFC3339Nano, "2000-01-03T00:00:00Z").UnixNano())},
			&Write{db: "db0", rp: "rp1", data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=200 %d`, mustParseTime(time.RFC3339Nano, "2000-01-03T00:00:00Z").UnixNano())},
			&Write{db: "db0", rp: "rp1", data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=200 %d`, mustParseTime(time.RFC3339Nano, "2000-01-04T00:00:00Z").UnixNano())},
			&Write{db: "db0", rp: "rp1", data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=200 %d`, mustParseTime(time.RFC3339Nano, "2000-01-05T00:00:00Z").UnixNano())},
			&Write{db: "db1", data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
			&Write{db: "db1", rp: "rp1", data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-03-01T00:00:00Z").UnixNano())},
			// Queries for wildcard deletes
			&Write{db: "db2", rp: "rp2", data: fmt.Sprintf(`cpu1,host=serverA,region=uswest val=200 %d`, mustParseTime(time.RFC3339Nano, "2000-01-05T00:00:00Z").UnixNano())},
			&Write{db: "db2", rp: "rp2", data: fmt.Sprintf(`cpu2,host=serverA,region=uswest val=200 %d`, mustParseTime(time.RFC3339Nano, "2000-01-06T00:00:00Z").UnixNano())},
			&Write{db: "db2", rp: "rp2", data: fmt.Sprintf(`gpu,host=serverA,region=uswest val=200 %d`, mustParseTime(time.RFC3339Nano, "2000-01-06T00:00:00Z").UnixNano())},
			&Write{db: "db2", rp: "rp3", data: fmt.Sprintf(`cpu1,host=serverA,region=uswest val=200 %d`, mustParseTime(time.RFC3339Nano, "2000-01-05T00:00:00Z").UnixNano())},
			&Write{db: "db2", rp: "rp3", data: fmt.Sprintf(`cpu2,host=serverA,region=uswest val=200 %d`, mustParseTime(time.RFC3339Nano, "2000-01-06T00:00:00Z").UnixNano())},
			&Write{db: "db2", rp: "rp3", data: fmt.Sprintf(`gpu,host=serverA,region=uswest val=200 %d`, mustParseTime(time.RFC3339Nano, "2000-01-06T00:00:00Z").UnixNano())},
		},
		queries: []*Query{
			&Query{
				name:    "Show series is present",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["cpu,host=serverA,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Show series is present in retention policy 0",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["cpu,host=serverA,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}, "rp": []string{"rp0"}},
			},
			&Query{
				name:    "Show series is present in retention policy 1",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["cpu,host=serverA,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}, "rp": []string{"rp1"}},
			},
			&Query{
				name:    "Delete series in retention policy only across shards",
				command: `DELETE FROM rp1.cpu WHERE time < '2000-01-05T00:00:00Z'`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
				once:    true,
			},
			&Query{
				name:    "Series is not deleted from db1",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["2000-03-01T00:00:00Z","serverA","uswest",23.2]]}]}]}`,
				params:  url.Values{"db": []string{"db1"}, "rp": []string{"rp1"}},
			},
			&Query{
				name:    "First two series across first two shards are deleted in rp1 on db0 and third shard still has data",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["2000-01-05T00:00:00Z","serverA","uswest",200]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}, "rp": []string{"rp1"}},
			},
			&Query{
				name:    "Show series is present in database",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["cpu,host=serverA,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}, "rp": []string{"rp0"}},
			},
			&Query{
				name:    "Delete series",
				command: `DELETE FROM cpu WHERE time < '2000-01-03T00:00:00Z'`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
				once:    true,
			},
			&Query{
				name:    "Show series still exists",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["cpu,host=serverA,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}, "rp": []string{"rp0"}},
			},
			&Query{
				name:    "Make sure last point still exists",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["2000-01-03T00:00:00Z","serverA","uswest",200]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}, "rp": []string{"rp0"}},
			},
			&Query{
				name:    "Make sure data wasn't deleted from other database.",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["2000-03-01T00:00:00Z","serverA","uswest",23.2]]}]}]}`,
				params:  url.Values{"db": []string{"db1"}},
			},
			&Query{
				name:    "Delete remaining instances of series",
				command: `DELETE FROM cpu WHERE time < '2000-01-06T00:00:00Z'`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Show series should now be empty",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Delete cpu* wildcard from series in rp2 db2",
				command: `DELETE FROM rp2./cpu*/`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db2"}},
				once:    true,
			},
			{
				name:    "Show that cpu is not in series for rp2 db2",
				command: `SELECT * FROM rp2./cpu*/`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db2"}},
				once:    true,
			},
			&Query{
				name:    "Show that gpu is still in series for rp2 db2",
				command: `SELECT * FROM rp2./gpu*/`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"gpu","columns":["time","host","region","val"],"values":[["2000-01-06T00:00:00Z","serverA","uswest",200]]}]}]}`,
				params:  url.Values{"db": []string{"db2"}},
				once:    true,
			},
			{
				name:    "Show that cpu is in series for rp3 db2",
				command: `SELECT * FROM rp3./cpu*/`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu1","columns":["time","host","region","val"],"values":[["2000-01-05T00:00:00Z","serverA","uswest",200]]},{"name":"cpu2","columns":["time","host","region","val"],"values":[["2000-01-06T00:00:00Z","serverA","uswest",200]]}]}]}`,
				params:  url.Values{"db": []string{"db2"}},
				once:    true,
			},
			&Query{
				name:    "Show that gpu is still in series for rp3 db2",
				command: `SELECT * FROM rp3./gpu*/`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"gpu","columns":["time","host","region","val"],"values":[["2000-01-06T00:00:00Z","serverA","uswest",200]]}]}]}`,
				params:  url.Values{"db": []string{"db2"}},
				once:    true,
			},
			&Query{
				name:    "Delete with wildcard in retention policy should fail parsing",
				command: `DELETE FROM /rp*/.gpu`,
				exp:     `{"error":"error parsing query: found ., expected ; at line 1, char 18"}`,
				params:  url.Values{"db": []string{"db2"}},
				once:    true,
			},
			&Query{
				name:    "Delete with database in query should fail parsing",
				command: `DELETE FROM db2..gpu`,
				exp:     `{"error":"error parsing query: database not supported at line 1, char 1"}`,
				params:  url.Values{"db": []string{"db2"}},
				once:    true,
			},
			&Query{
				name:    "Delete with empty retention policy and database should fail parsing",
				command: `DELETE FROM ...gpu`,
				exp:     `{"error":"error parsing query: found ., expected identifier at line 1, char 13"}`,
				params:  url.Values{"db": []string{"db2"}},
				once:    true,
			},
		},
	}

	tests["delete_series_time_tag_filter"] = Test{
		db: "db0",
		rp: "rp0",
		writes: Writes{
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
			&Write{data: fmt.Sprintf(`cpu,host=serverB,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=100 %d`, mustParseTime(time.RFC3339Nano, "2000-01-02T00:00:00Z").UnixNano())},
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=200 %d`, mustParseTime(time.RFC3339Nano, "2000-01-03T00:00:00Z").UnixNano())},
			&Write{db: "db1", data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
		},
		queries: []*Query{
			&Query{
				name:    "Show series is present",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["cpu,host=serverA,region=uswest"],["cpu,host=serverB,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Delete series",
				command: `DELETE FROM cpu WHERE host = 'serverA' AND time < '2000-01-03T00:00:00Z'`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
				once:    true,
			},
			&Query{
				name:    "Show series still exists",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["cpu,host=serverA,region=uswest"],["cpu,host=serverB,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Make sure last point still exists",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["2000-01-01T00:00:00Z","serverB","uswest",23.2],["2000-01-03T00:00:00Z","serverA","uswest",200]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Make sure other points are deleted",
				command: `SELECT COUNT(val) FROM cpu WHERE "host" = 'serverA'`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Make sure data wasn't deleted from other database.",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["2000-01-01T00:00:00Z","serverA","uswest",23.2]]}]}]}`,
				params:  url.Values{"db": []string{"db1"}},
			},
		},
	}

	tests["drop_and_recreate_series"] = Test{
		db: "db0",
		rp: "rp0",
		writes: Writes{
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
			&Write{db: "db1", data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
		},
		queries: []*Query{
			&Query{
				name:    "Show series is present",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["cpu,host=serverA,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Drop series after data write",
				command: `DROP SERIES FROM cpu`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
				once:    true,
			},
			&Query{
				name:    "Show series is gone",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Make sure data wasn't deleted from other database.",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["2000-01-01T00:00:00Z","serverA","uswest",23.2]]}]}]}`,
				params:  url.Values{"db": []string{"db1"}},
			},
		},
	}
	tests["drop_and_recreate_series_retest"] = Test{
		db: "db0",
		rp: "rp0",
		writes: Writes{
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
		},
		queries: []*Query{
			&Query{
				name:    "Show series is present again after re-write",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["cpu,host=serverA,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
		},
	}

	tests["drop_series_from_regex"] = Test{
		db: "db0",
		rp: "rp0",
		writes: Writes{
			&Write{data: strings.Join([]string{
				fmt.Sprintf(`a,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
				fmt.Sprintf(`aa,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
				fmt.Sprintf(`b,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
				fmt.Sprintf(`c,host=serverA,region=uswest val=30.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			}, "\n")},
		},
		queries: []*Query{
			&Query{
				name:    "Show series is present",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["a,host=serverA,region=uswest"],["aa,host=serverA,region=uswest"],["b,host=serverA,region=uswest"],["c,host=serverA,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Drop series after data write",
				command: `DROP SERIES FROM /a.*/`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
				once:    true,
			},
			&Query{
				name:    "Show series is gone",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["b,host=serverA,region=uswest"],["c,host=serverA,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Drop series from regex that matches no measurements",
				command: `DROP SERIES FROM /a.*/`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
				once:    true,
			},
			&Query{
				name:    "make sure DROP SERIES doesn't delete anything when regex doesn't match",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["b,host=serverA,region=uswest"],["c,host=serverA,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Drop series with WHERE field should error",
				command: `DROP SERIES FROM c WHERE val > 50.0`,
				exp:     `{"results":[{"statement_id":0,"error":"shard 1: fields not supported in WHERE clause during deletion"}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "make sure DROP SERIES with field in WHERE didn't delete data",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["b,host=serverA,region=uswest"],["c,host=serverA,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Drop series with WHERE time should error",
				command: `DROP SERIES FROM c WHERE time > now() - 1d`,
				exp:     `{"results":[{"statement_id":0,"error":"DROP SERIES doesn't support time in WHERE clause"}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
		},
	}

	tests["retention_policy_commands"] = Test{
		db: "db0",
		queries: []*Query{
			&Query{
				name:    "create retention policy with invalid name should return an error",
				command: `CREATE RETENTION POLICY "." ON db0 DURATION 1d REPLICATION 1`,
				exp:     `{"results":[{"statement_id":0,"error":"invalid name"}]}`,
				once:    true,
			},
			&Query{
				name:    "create retention policy should succeed",
				command: `CREATE RETENTION POLICY rp0 ON db0 DURATION 1h REPLICATION 1`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "show retention policy should succeed",
				command: `SHOW RETENTION POLICIES ON db0`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["name","duration","shardGroupDuration","replicaN","futureWriteLimit","pastWriteLimit","default"],"values":[["rp0","1h0m0s","1h0m0s",1,"0s","0s",false]]}]}]}`,
			},
			&Query{
				name:    "alter retention policy should succeed",
				command: `ALTER RETENTION POLICY rp0 ON db0 DURATION 2h REPLICATION 3 DEFAULT`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "show retention policy should have new altered information",
				command: `SHOW RETENTION POLICIES ON db0`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["name","duration","shardGroupDuration","replicaN","futureWriteLimit","pastWriteLimit","default"],"values":[["rp0","2h0m0s","1h0m0s",3,"0s","0s",true]]}]}]}`,
			},
			&Query{
				name:    "show retention policy should still show policy",
				command: `SHOW RETENTION POLICIES ON db0`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["name","duration","shardGroupDuration","replicaN","futureWriteLimit","pastWriteLimit","default"],"values":[["rp0","2h0m0s","1h0m0s",3,"0s","0s",true]]}]}]}`,
			},
			&Query{
				name:    "create a second non-default retention policy",
				command: `CREATE RETENTION POLICY rp2 ON db0 DURATION 1h REPLICATION 1`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "show retention policy should show both",
				command: `SHOW RETENTION POLICIES ON db0`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["name","duration","shardGroupDuration","replicaN","futureWriteLimit","pastWriteLimit","default"],"values":[["rp0","2h0m0s","1h0m0s",3,"0s","0s",true],["rp2","1h0m0s","1h0m0s",1,"0s","0s",false]]}]}]}`,
			},
			&Query{
				name:    "dropping non-default retention policy succeed",
				command: `DROP RETENTION POLICY rp2 ON db0`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "create a third non-default retention policy",
				command: `CREATE RETENTION POLICY rp3 ON db0 DURATION 1h REPLICATION 1 SHARD DURATION 30m`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "create retention policy with default on",
				command: `CREATE RETENTION POLICY rp3 ON db0 DURATION 1h REPLICATION 1 SHARD DURATION 30m DEFAULT`,
				exp:     `{"results":[{"statement_id":0,"error":"retention policy conflicts with an existing policy"}]}`,
				once:    true,
			},
			&Query{
				name:    "show retention policy should show both with custom shard",
				command: `SHOW RETENTION POLICIES ON db0`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["name","duration","shardGroupDuration","replicaN","futureWriteLimit","pastWriteLimit","default"],"values":[["rp0","2h0m0s","1h0m0s",3,"0s","0s",true],["rp3","1h0m0s","1h0m0s",1,"0s","0s",false]]}]}]}`,
			},
			&Query{
				name:    "dropping non-default custom shard retention policy succeed",
				command: `DROP RETENTION POLICY rp3 ON db0`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "show retention policy should show just default",
				command: `SHOW RETENTION POLICIES ON db0`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["name","duration","shardGroupDuration","replicaN","futureWriteLimit","pastWriteLimit","default"],"values":[["rp0","2h0m0s","1h0m0s",3,"0s","0s",true]]}]}]}`,
			},
			&Query{
				name:    "Ensure retention policy with unacceptable retention cannot be created",
				command: `CREATE RETENTION POLICY rp4 ON db0 DURATION 1s REPLICATION 1`,
				exp:     `{"results":[{"statement_id":0,"error":"retention policy duration must be at least 1h0m0s"}]}`,
				once:    true,
			},
			&Query{
				name:    "Check error when deleting retention policy on non-existent database",
				command: `DROP RETENTION POLICY rp1 ON mydatabase`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "Ensure retention policy for non existing db is not created",
				command: `CREATE RETENTION POLICY rp0 ON nodb DURATION 1h REPLICATION 1`,
				exp:     `{"results":[{"statement_id":0,"error":"database not found: nodb"}]}`,
				once:    true,
			},
			&Query{
				name:    "drop rp0",
				command: `DROP RETENTION POLICY rp0 ON db0`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			// INF Shard Group Duration will normalize to the Retention Policy Duration Default
			&Query{
				name:    "create retention policy with inf shard group duration",
				command: `CREATE RETENTION POLICY rpinf ON db0 DURATION INF REPLICATION 1 SHARD DURATION 0s`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			// 0s Shard Group Duration will normalize to the Replication Policy Duration
			&Query{
				name:    "create retention policy with 0s shard group duration",
				command: `CREATE RETENTION POLICY rpzero ON db0 DURATION 1h REPLICATION 1 SHARD DURATION 0s`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			// 1s Shard Group Duration will normalize to the MinDefaultRetentionPolicyDuration
			&Query{
				name:    "create retention policy with 1s shard group duration",
				command: `CREATE RETENTION POLICY rponesecond ON db0 DURATION 2h REPLICATION 1 SHARD DURATION 1s`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "show retention policy: validate normalized shard group durations are working",
				command: `SHOW RETENTION POLICIES ON db0`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["name","duration","shardGroupDuration","replicaN","futureWriteLimit","pastWriteLimit","default"],"values":[["rpinf","0s","168h0m0s",1,"0s","0s",false],["rpzero","1h0m0s","1h0m0s",1,"0s","0s",false],["rponesecond","2h0m0s","1h0m0s",1,"0s","0s",false]]}]}]}`,
			},
		},
	}

	tests["retention_policy_auto_create"] = Test{
		queries: []*Query{
			&Query{
				name:    "create database should succeed",
				command: `CREATE DATABASE db0`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "show retention policies should return auto-created policy",
				command: `SHOW RETENTION POLICIES ON db0`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["name","duration","shardGroupDuration","replicaN","futureWriteLimit","pastWriteLimit","default"],"values":[["autogen","0s","168h0m0s",1,"0s","0s",true]]}]}]}`,
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
