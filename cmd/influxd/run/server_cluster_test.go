package run_test

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestCluster_CreateDatabase(t *testing.T) {
	t.Parallel()

	c, err := NewClusterWithDefaults(5)
	defer c.Close()
	if err != nil {
		t.Fatalf("error creating cluster: %s", err)
	}
}

func TestCluster_Write(t *testing.T) {
	t.Parallel()

	c, err := NewClusterWithDefaults(5)
	if err != nil {
		t.Fatalf("error creating cluster: %s", err)
	}
	defer c.Close()

	writes := []string{
		fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
	}

	_, err = c.Servers[0].Write("db", "default", strings.Join(writes, "\n"), nil)
	if err != nil {
		t.Fatal(err)
	}

	q := &Query{
		name:    "write",
		command: `SELECT * FROM db."default".cpu`,
		exp:     `{"results":[{"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["2000-01-01T00:00:00Z","serverA","uswest",23.2]]}]}]}`,
	}
	err = c.QueryAll(q)
	if err != nil {
		t.Fatal(err)
	}
}
func TestCluster_DatabaseCommands(t *testing.T) {
	t.Parallel()
	c, err := NewCluster(5)
	if err != nil {
		t.Fatalf("error creating cluster: %s", err)
	}

	defer c.Close()

	test := Test{
		queries: []*Query{
			&Query{
				name:    "create database should succeed",
				command: `CREATE DATABASE db0`,
				exp:     `{"results":[{}]}`,
				once:    true,
			},
			&Query{
				name:    "create database should error with bad name",
				command: `CREATE DATABASE 0xdb0`,
				exp:     `{"error":"error parsing query: found 0, expected identifier at line 1, char 17"}`,
			},
			&Query{
				name:    "show database should succeed",
				command: `SHOW DATABASES`,
				exp:     `{"results":[{"series":[{"name":"databases","columns":["name"],"values":[["db0"]]}]}]}`,
			},
			&Query{
				name:    "create database should error if it already exists",
				command: `CREATE DATABASE db0`,
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
				name:    "show database should succeed",
				command: `SHOW DATABASES`,
				exp:     `{"results":[{"series":[{"name":"databases","columns":["name"],"values":[["db0"],["db1"]]}]}]}`,
			},
			&Query{
				name:    "drop database db0 should succeed",
				command: `DROP DATABASE db0`,
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

	for _, query := range test.queries {
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		t.Logf("Running %s", query.name)
		if query.once {
			if _, err := c.Query(query); err != nil {
				t.Error(query.Error(err))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
			continue
		}
		if err := c.QueryAll(query); err != nil {
			t.Error(query.Error(err))
		}
	}
}

func TestCluster_Query_DropAndRecreateDatabase(t *testing.T) {
	t.Parallel()
	c, err := NewClusterWithDefaults(5)
	if err != nil {
		t.Fatalf("error creating cluster: %s", err)
	}
	defer c.Close()

	writes := []string{
		fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
	}

	_, err = c.Servers[0].Write("db", "default", strings.Join(writes, "\n"), nil)
	if err != nil {
		t.Fatal(err)
	}
	test := Test{
		queries: []*Query{
			&Query{
				name:    "Drop database after data write",
				command: `DROP DATABASE db`,
				exp:     `{"results":[{}]}`,
				once:    true,
			},
			&Query{
				name:    "Recreate database",
				command: `CREATE DATABASE db`,
				exp:     `{"results":[{}]}`,
				once:    true,
			},
			&Query{
				name:    "Recreate retention policy",
				command: `CREATE RETENTION POLICY rp0 ON db DURATION 365d REPLICATION 1 DEFAULT`,
				exp:     `{"results":[{}]}`,
				once:    true,
			},
			&Query{
				name:    "Show measurements after recreate",
				command: `SHOW MEASUREMENTS`,
				exp:     `{"results":[{}]}`,
				params:  url.Values{"db": []string{"db"}},
			},
			&Query{
				name:    "Query data after recreate",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{}]}`,
				params:  url.Values{"db": []string{"db"}},
			},
		},
	}

	for _, query := range test.queries {
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		t.Logf("Running %s", query.name)
		if query.once {
			if _, err := c.Query(query); err != nil {
				t.Error(query.Error(err))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
			continue
		}
		if err := c.QueryAll(query); err != nil {
			t.Error(query.Error(err))
		}
	}

}
