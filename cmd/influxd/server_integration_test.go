package main_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/messaging"

	"github.com/influxdb/influxdb/client"
	main "github.com/influxdb/influxdb/cmd/influxd"
)

const (
	// Use a prime batch size, so that internal batching code, which most likely
	// uses nice round batches, has to deal with leftover.
	batchSize = 4217
)

// tempfile returns a temporary path.
func tempfile() string {
	f, _ := ioutil.TempFile("", "influxdb-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}

// urlFor returns a URL with the path and query params correctly appended and set.
func urlFor(u *url.URL, path string, params url.Values) *url.URL {
	v, _ := url.Parse(u.String())
	v.Path = path
	v.RawQuery = params.Encode()
	return v
}

// rewriteDbRp returns a copy of old with occurrences of %DB% with the given database,
// and occurences of %RP with the given retention
func rewriteDbRp(old, database, retention string) string {
	return strings.Replace(strings.Replace(old, "%DB%", database, -1), "%RP%", retention, -1)
}

// Node represents a node under test, which is both a broker and data node.
type Node struct {
	broker *messaging.Broker
	server *influxdb.Server
	url    *url.URL
	leader bool
}

// Cluster represents a multi-node cluster.
type Cluster []*Node

// createCombinedNodeCluster creates a cluster of nServers nodes, each of which
// runs as both a Broker and Data node. If any part cluster creation fails,
// the testing is marked as failed.
//
// This function returns a slice of nodes, the first of which will be the leader.
func createCombinedNodeCluster(t *testing.T, testName, tmpDir string, nNodes, basePort int) Cluster {
	t.Logf("Creating cluster of %d nodes for test %s", nNodes, testName)
	if nNodes < 1 {
		t.Fatalf("Test %s: asked to create nonsense cluster", testName)
	}

	nodes := make([]*Node, 0)

	tmpBrokerDir := filepath.Join(tmpDir, "broker-integration-test")
	tmpDataDir := filepath.Join(tmpDir, "data-integration-test")
	t.Logf("Test %s: using tmp directory %q for brokers\n", testName, tmpBrokerDir)
	t.Logf("Test %s: using tmp directory %q for data nodes\n", testName, tmpDataDir)
	// Sometimes if a test fails, it's because of a log.Fatal() in the program.
	// This prevents the defer from cleaning up directories.
	// To be safe, nuke them always before starting
	_ = os.RemoveAll(tmpBrokerDir)
	_ = os.RemoveAll(tmpDataDir)

	// Create the first node, special case.
	c := main.NewConfig()
	c.Broker.Dir = filepath.Join(tmpBrokerDir, strconv.Itoa(basePort))
	c.Data.Dir = filepath.Join(tmpDataDir, strconv.Itoa(basePort))
	c.Broker.Port = basePort
	c.Data.Port = basePort
	c.Admin.Enabled = false
	c.ReportingDisabled = true

	b, s := main.Run(c, "", "x.x", os.Stderr)
	if b == nil {
		t.Fatalf("Test %s: failed to create broker on port %d", testName, basePort)
	}
	if s == nil {
		t.Fatalf("Test %s: failed to create leader data node on port %d", testName, basePort)
	}
	nodes = append(nodes, &Node{
		broker: b,
		server: s,
		url:    &url.URL{Scheme: "http", Host: "localhost:" + strconv.Itoa(basePort)},
		leader: true,
	})

	// Create subsequent nodes, which join to first node.
	for i := 1; i < nNodes; i++ {
		nextPort := basePort + i
		c.Broker.Dir = filepath.Join(tmpBrokerDir, strconv.Itoa(nextPort))
		c.Data.Dir = filepath.Join(tmpDataDir, strconv.Itoa(nextPort))
		c.Broker.Port = nextPort
		c.Data.Port = nextPort

		b, s := main.Run(c, "http://localhost:"+strconv.Itoa(basePort), "x.x", os.Stderr)
		if b == nil {
			t.Fatalf("Test %s: failed to create following broker on port %d", testName, basePort)
		}
		if s == nil {
			t.Fatalf("Test %s: failed to create following data node on port %d", testName, basePort)
		}

		nodes = append(nodes, &Node{
			broker: b,
			server: s,
			url:    &url.URL{Scheme: "http", Host: "localhost:" + strconv.Itoa(nextPort)},
		})
	}

	return nodes
}

// createDatabase creates a database, and verifies that the creation was successful.
func createDatabase(t *testing.T, testName string, nodes Cluster, database string) {
	t.Logf("Test: %s: creating database %s", testName, database)
	query(t, nodes[:1], "", "CREATE DATABASE "+database, `{"results":[{}]}`)
}

// createRetentionPolicy creates a retetention policy and verifies that the creation was successful.
// Replication factor is set to equal the number nodes in the cluster.
func createRetentionPolicy(t *testing.T, testName string, nodes Cluster, database, retention string) {
	t.Logf("Creating retention policy %s for database %s", retention, database)
	command := fmt.Sprintf("CREATE RETENTION POLICY %s ON %s DURATION 1h REPLICATION %d DEFAULT", retention, database, len(nodes))
	query(t, nodes[:1], "", command, `{"results":[{}]}`)
}

// deleteDatabase delete a database, and verifies that the deletion was successful.
func deleteDatabase(t *testing.T, testName string, nodes Cluster, database string) {
	t.Logf("Test: %s: deleting database %s", testName, database)
	query(t, nodes[:1], "", "DROP DATABASE "+database, `{"results":[{}]}`)
}

// writes writes the provided data to the cluster. It verfies that a 200 OK is returned by the server.
func write(t *testing.T, node *Node, data string) {
	u := urlFor(node.url, "write", url.Values{})

	resp, err := http.Post(u.String(), "application/json", bytes.NewReader([]byte(data)))
	if err != nil {
		t.Fatalf("Couldn't write data: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		t.Fatalf("Write to database failed.  Unexpected status code.  expected: %d, actual %d, %s", http.StatusOK, resp.StatusCode, string(body))
	}

	// Until races are solved.
	time.Sleep(3 * time.Second)
}

// query executes the given query against all nodes in the cluster, and verifies no errors occured, and
// ensures the returned data is as expected
func query(t *testing.T, nodes Cluster, urlDb, query, expected string) (string, bool) {
	v := url.Values{"q": []string{query}}
	if urlDb != "" {
		v.Set("db", urlDb)
	}

	// Query the data exists
	for _, n := range nodes {
		u := urlFor(n.url, "query", v)
		resp, err := http.Get(u.String())
		if err != nil {
			t.Fatalf("Failed to execute query '%s': %s", query, err.Error())
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("Couldn't read body of response: %s", err.Error())
		}

		if expected != string(body) {
			return string(body), false
		}
	}

	return "", true
}

// runTests_Errors tests some basic error cases.
func runTests_Errors(t *testing.T, nodes Cluster) {
	t.Logf("Running tests against %d-node cluster", len(nodes))

	tests := []struct {
		name     string
		write    string // If equal to the empty string, no data is written.
		query    string // If equal to the blank string, no query is executed.
		expected string // If 'query' is equal to the blank string, this is ignored.
	}{
		{
			name:     "simple SELECT from non-existent database",
			write:    "",
			query:    `SELECT * FROM "qux"."bar".cpu`,
			expected: `{"results":[{"error":"database not found: qux"}]}`,
		},
	}

	for _, tt := range tests {
		if tt.write != "" {
			write(t, nodes[0], tt.write)
		}

		if tt.query != "" {
			got, ok := query(t, nodes, "", tt.query, tt.expected)
			if !ok {
				t.Errorf("Test '%s' failed, expected: %s, got: %s", tt.name, tt.expected, got)
			}
		}
	}
}

// runTests tests write and query of data. Setting testNumbers allows only a subset of tests to be run.
func runTestsData(t *testing.T, testName string, nodes Cluster, database, retention string, testNums ...int) {
	t.Logf("Running tests against %d-node cluster", len(nodes))

	// Start by ensuring database and retention policy exist.
	createDatabase(t, testName, nodes, database)
	createRetentionPolicy(t, testName, nodes, database, retention)

	// The tests. Within these tests %DB% and %RP% will be replaced with the database and retention passed into
	// this function.
	tests := []struct {
		reset    bool   // Delete and recreate the database.
		name     string // Test name, for easy-to-read test log output.
		write    string // If equal to the empty string, no data is written.
		query    string // If equal to the blank string, no query is executed.
		queryDb  string // If set, is used as the "db" query param.
		expected string // If 'query' is equal to the blank string, this is ignored.
	}{
		// Data read and write tests
		{
			reset:    true,
			name:     "single point with timestamp",
			write:    `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "cpu", "timestamp": "2015-02-28T01:03:36.703820946Z", "tags": {"host": "server01"}, "fields": {"value": 100}}]}`,
			query:    `SELECT * FROM "%DB%"."%RP%".cpu`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		{
			name:     "single string point with timestamp",
			write:    `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "logs", "timestamp": "2015-02-28T01:03:36.703820946Z", "tags": {"host": "server01"}, "fields": {"value": "disk full"}}]}`,
			query:    `SELECT * FROM "%DB%"."%RP%".logs`,
			expected: `{"results":[{"series":[{"name":"logs","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z","disk full"]]}]}]}`,
		},
		{
			name:     "single bool point with timestamp",
			write:    `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "status", "timestamp": "2015-02-28T01:03:36.703820946Z", "tags": {"host": "server01"}, "fields": {"value": "true"}}]}`,
			query:    `SELECT * FROM "%DB%"."%RP%".status`,
			expected: `{"results":[{"series":[{"name":"status","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z","true"]]}]}]}`,
		},

		{
			name:     "single point, select with now()",
			query:    `SELECT * FROM "%DB%"."%RP%".cpu WHERE time < now()`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},

		{
			name:     "measurement not found",
			query:    `SELECT value FROM "%DB%"."%RP%".foobarbaz`,
			expected: `{"results":[{"error":"measurement not found"}]}`,
		},
		{
			name:     "field not found",
			query:    `SELECT abc FROM "%DB%"."%RP%".cpu WHERE time < now()`,
			expected: `{"results":[{"error":"field not found: abc"}]}`,
		},

		// WHERE fields queries
		{
			reset:    true,
			name:     "WHERE fields",
			write:    `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "cpu", "timestamp": "2015-02-28T01:03:36.703820946Z", "fields": {"alert_id": "alert", "tenant_id": "tenant"}}]}`,
			query:    `SELECT alert_id FROM "%DB%"."%RP%".cpu WHERE alert_id='alert'`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","alert_id"],"values":[["2015-02-28T01:03:36.703820946Z","alert"]]}]}]}`,
		},
		{
			name:     "WHERE fields with AND query, all fields in SELECT",
			write:    `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "cpu", "timestamp": "2015-02-28T01:03:36.703820946Z", "fields": {"alert_id": "alert", "tenant_id": "tenant"}}]}`,
			query:    `SELECT alert_id,tenant_id FROM "%DB%"."%RP%".cpu WHERE alert_id='alert' AND tenant_id='tenant'`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","alert_id","tenant_id"],"values":[["2015-02-28T01:03:36.703820946Z","alert","tenant"]]}]}]}`,
		},
		{
			name:     "WHERE fields with AND query, all fields in SELECT, one in parenthesis",
			write:    `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "cpu", "timestamp": "2015-02-28T01:03:36.703820946Z", "fields": {"alert_id": "alert", "tenant_id": "tenant"}}]}`,
			query:    `SELECT alert_id,tenant_id FROM "%DB%"."%RP%".cpu WHERE alert_id='alert' AND (tenant_id='tenant')`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","alert_id","tenant_id"],"values":[["2015-02-28T01:03:36.703820946Z","alert","tenant"]]}]}]}`,
		},
		{
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "cpu", "timestamp": "2009-11-10T23:00:02Z", "fields": {"load": 100}},
			                                                                      {"name": "cpu", "timestamp": "2009-11-10T23:01:02Z", "fields": {"load": 80}}]}`,
			query:    `select load from "%DB%"."%RP%".cpu where load > 100`,
			expected: `{"results":[{}]}`,
		},
		{
			query:    `select load from "%DB%"."%RP%".cpu where load >= 100`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","load"],"values":[["2009-11-10T23:00:02Z",100]]}]}]}`,
		},
		{
			query:    `select load from "%DB%"."%RP%".cpu where load = 100`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","load"],"values":[["2009-11-10T23:00:02Z",100]]}]}]}`,
		},
		{
			query:    `select load from "%DB%"."%RP%".cpu where load <= 100`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","load"],"values":[["2009-11-10T23:00:02Z",100],["2009-11-10T23:01:02Z",80]]}]}]}`,
		},
		{
			query:    `select load from "%DB%"."%RP%".cpu where load > 99`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","load"],"values":[["2009-11-10T23:00:02Z",100]]}]}]}`,
		},
		{
			query:    `select load from "%DB%"."%RP%".cpu where load = 99`,
			expected: `{"results":[{}]}`,
		},
		{
			query:    `select load from "%DB%"."%RP%".cpu where load < 99`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","load"],"values":[["2009-11-10T23:01:02Z",80]]}]}]}`,
		},
		{
			query:    `select load from "%DB%"."%RP%".cpu where load < 80`,
			expected: `{"results":[{}]}`,
		},
		{
			write:    `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "logs", "timestamp": "2009-11-10T23:00:02Z","fields": {"event": "disk full"}}]}`,
			query:    `select event from "%DB%"."%RP%".logs where event = 'disk full'`,
			expected: `{"results":[{"series":[{"name":"logs","columns":["time","event"],"values":[["2009-11-10T23:00:02Z","disk full"]]}]}]}`,
		},
		{
			write:    `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "logs", "timestamp": "2009-11-10T23:00:02Z","fields": {"event": "disk full"}}]}`,
			query:    `select event from "%DB%"."%RP%".logs where event = 'nonsense'`,
			expected: `{"results":[{}]}`,
		},
		{
			name:     "missing measurement with `GROUP BY *`",
			query:    `select load from "%DB%"."%RP%".missing group by *`,
			expected: `{"results":[{"error":"measurement not found: \"mydb\".\"myrp\".\"missing\""}]}`,
		},

		// Metadata display tests

		{
			reset: true,
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
		{"name": "cpu", "tags": {"host": "server01"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "cpu", "tags": {"host": "server01", "region": "uswest"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "cpu", "tags": {"host": "server01", "region": "useast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "cpu", "tags": {"host": "server02", "region": "useast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "gpu", "tags": {"host": "server02", "region": "useast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "gpu", "tags": {"host": "server03", "region": "caeast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}}
		]}`,
			query:    "SHOW SERIES",
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","columns":["id","host","region"],"values":[[1,"server01",""],[2,"server01","uswest"],[3,"server01","useast"],[4,"server02","useast"]]},{"name":"gpu","columns":["id","host","region"],"values":[[5,"server02","useast"],[6,"server03","caeast"]]}]}]}`,
		},
		{
			query:    "SHOW SERIES FROM cpu",
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","columns":["id","host","region"],"values":[[1,"server01",""],[2,"server01","uswest"],[3,"server01","useast"],[4,"server02","useast"]]}]}]}`,
		},
		{
			query:    "SHOW SERIES WHERE region = 'uswest'",
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","columns":["id","host","region"],"values":[[2,"server01","uswest"]]}]}]}`,
		},
		{
			query:    "SHOW SERIES WHERE region =~ /ca.*/",
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"gpu","columns":["id","host","region"],"values":[[6,"server03","caeast"]]}]}]}`,
		},
		{
			query:    "SHOW SERIES WHERE host !~ /server0[12]/",
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"gpu","columns":["id","host","region"],"values":[[6,"server03","caeast"]]}]}]}`,
		},
		{
			query:    "SHOW SERIES FROM cpu WHERE region = 'useast'",
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","columns":["id","host","region"],"values":[[3,"server01","useast"],[4,"server02","useast"]]}]}]}`,
		},

		{
			reset: true,
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
		{"name": "cpu", "tags": {"host": "server01"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "cpu", "tags": {"host": "server01", "region": "uswest"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "cpu", "tags": {"host": "server01", "region": "useast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "cpu", "tags": {"host": "server02", "region": "useast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "gpu", "tags": {"host": "server02", "region": "useast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "gpu", "tags": {"host": "server02", "region": "caeast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "other", "tags": {"host": "server03", "region": "caeast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}}
		]}`,
			query:    "SHOW MEASUREMENTS LIMIT 2",
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"measurements","columns":["name"],"values":[["cpu"],["gpu"]]}]}]}`,
		},
		{
			query:    "SHOW MEASUREMENTS WHERE region =~ /ca.*/",
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"measurements","columns":["name"],"values":[["gpu"],["other"]]}]}]}`,
		},
		{
			query:    "SHOW MEASUREMENTS WHERE region !~ /ca.*/",
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"measurements","columns":["name"],"values":[["cpu"]]}]}]}`,
		},

		{
			reset: true,
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
		{"name": "cpu", "tags": {"host": "server01"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "cpu", "tags": {"host": "server01", "region": "uswest"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "cpu", "tags": {"host": "server01", "region": "useast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "cpu", "tags": {"host": "server02", "region": "useast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "gpu", "tags": {"host": "server02", "region": "useast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "gpu", "tags": {"host": "server03", "region": "caeast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}}
		]}`,
			query:    "SHOW TAG KEYS",
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","columns":["tagKey"],"values":[["host"],["region"]]},{"name":"gpu","columns":["tagKey"],"values":[["host"],["region"]]}]}]}`,
		},
		{
			query:    "SHOW TAG KEYS FROM cpu",
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","columns":["tagKey"],"values":[["host"],["region"]]}]}]}`,
		},
		{
			query:    "SHOW TAG KEYS FROM bad",
			queryDb:  "%DB%",
			expected: `{"results":[{"error":"measurement \"bad\" not found"}]}`,
		},

		{
			reset: true,
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
		{"name": "cpu", "tags": {"host": "server01"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "cpu", "tags": {"host": "server01", "region": "uswest"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "cpu", "tags": {"host": "server01", "region": "useast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "cpu", "tags": {"host": "server02", "region": "useast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "gpu", "tags": {"host": "server02", "region": "useast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}},
		{"name": "gpu", "tags": {"host": "server03", "region": "caeast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"value": 100}}
		]}`,
			query:    "SHOW TAG VALUES WITH KEY = host",
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"hostTagValues","columns":["host"],"values":[["server01"],["server02"],["server03"]]}]}]}`,
		},
		{
			query:    `SHOW TAG VALUES WITH KEY = "host"`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"hostTagValues","columns":["host"],"values":[["server01"],["server02"],["server03"]]}]}]}`,
		},
		{
			query:    `SHOW TAG VALUES FROM cpu WITH KEY = host WHERE region = 'uswest'`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"hostTagValues","columns":["host"],"values":[["server01"]]}]}]}`,
		},
		{
			query:    `SHOW TAG VALUES WITH KEY = host WHERE region =~ /ca.*/`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"hostTagValues","columns":["host"],"values":[["server03"]]}]}]}`,
		},
		{
			query:    `SHOW TAG VALUES WITH KEY = region WHERE host !~ /server0[12]/`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"regionTagValues","columns":["region"],"values":[["caeast"]]}]}]}`,
		},
		{
			query:    `SHOW TAG VALUES FROM cpu WITH KEY IN (host, region) WHERE region = 'uswest'`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"hostTagValues","columns":["host"],"values":[["server01"]]},{"name":"regionTagValues","columns":["region"],"values":[["uswest"]]}]}]}`,
		},

		{
			reset: true,
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
		{"name": "cpu", "tags": {"host": "server01"},"timestamp": "2009-11-10T23:00:00Z","fields": {"field1": 100}},
		{"name": "cpu", "tags": {"host": "server01", "region": "uswest"},"timestamp": "2009-11-10T23:00:00Z","fields": {"field1": 200, "field2": 300, "field3": 400}},
		{"name": "cpu", "tags": {"host": "server01", "region": "useast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"field1": 200, "field2": 300, "field3": 400}},
		{"name": "cpu", "tags": {"host": "server02", "region": "useast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"field1": 200, "field2": 300, "field3": 400}},
		{"name": "gpu", "tags": {"host": "server01", "region": "useast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"field4": 200, "field5": 300}},
		{"name": "gpu", "tags": {"host": "server03", "region": "caeast"},"timestamp": "2009-11-10T23:00:00Z","fields": {"field6": 200, "field7": 300}}
		]}`,
			query:    `SHOW FIELD KEYS`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","columns":["fieldKey"],"values":[["field1"],["field2"],["field3"]]},{"name":"gpu","columns":["fieldKey"],"values":[["field4"],["field5"],["field6"],["field7"]]}]}]}`,
		},
		{
			query:    `SHOW FIELD KEYS FROM cpu`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","columns":["fieldKey"],"values":[["field1"],["field2"],["field3"]]}]}]}`,
		},

		// User control tests
		{
			name:     "show users, no actual users",
			query:    `SHOW USERS`,
			expected: `{"results":[{"series":[{"columns":["user","admin"]}]}]}`,
		},
		{
			query:    `CREATE USER jdoe WITH PASSWORD '1337'`,
			expected: `{"results":[{}]}`,
		},
		{
			name:     "show users, 1 existing user",
			query:    `SHOW USERS`,
			expected: `{"results":[{"series":[{"columns":["user","admin"],"values":[["jdoe",false]]}]}]}`,
		},
		{
			query:    `GRANT ALL PRIVILEGES TO jdoe`,
			expected: `{"results":[{}]}`,
		},
		{
			name:     "show users, existing user as admin",
			query:    `SHOW USERS`,
			expected: `{"results":[{"series":[{"columns":["user","admin"],"values":[["jdoe",true]]}]}]}`,
		},
		{
			name:     "grant DB privileges to user",
			query:    `GRANT READ ON %DB% TO jdoe`,
			expected: `{"results":[{}]}`,
		},
		{
			query:    `REVOKE ALL PRIVILEGES FROM jdoe`,
			expected: `{"results":[{}]}`,
		},
		{
			name:     "bad create user request",
			query:    `CREATE USER 0xBAD WITH PASSWORD pwd1337`,
			expected: `{"error":"error parsing query: found 0, expected identifier at line 1, char 13"}`,
		},
		{
			name:     "bad create user request, no name",
			query:    `CREATE USER WITH PASSWORD pwd1337`,
			expected: `{"error":"error parsing query: found WITH, expected identifier at line 1, char 13"}`,
		},
		{
			name:     "bad create user request, no password",
			query:    `CREATE USER jdoe`,
			expected: `{"error":"error parsing query: found EOF, expected WITH at line 1, char 18"}`,
		},
		{
			query:    `DROP USER jdoe`,
			expected: `{"results":[{}]}`,
		},
		{
			name:     "delete non existing user",
			query:    `DROP USER noone`,
			expected: `{"results":[{"error":"user not found"}]}`,
		},

		// Continuous query control.
		{
			name:     "create continuous query",
			query:    `CREATE CONTINUOUS QUERY myquery ON %DB% BEGIN SELECT count() INTO measure1 FROM myseries GROUP BY time(10m) END`,
			expected: `{"results":[{}]}`,
		},
		{
			query:    `SHOW CONTINUOUS QUERIES`,
			expected: `{"results":[{"series":[{"name":"%DB%","columns":["name","query"],"values":[["myquery","CREATE CONTINUOUS QUERY myquery ON %DB% BEGIN SELECT count() INTO measure1 FROM myseries GROUP BY time(10m) END"]]}]}]}`,
		},
	}

	for i, tt := range tests {
		// If tests were explicitly requested, only run those tests.
		if len(testNums) > 0 {
			var found bool
			for _, t := range testNums {
				if i == t {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		name := tt.name
		if name == "" {
			name = tt.query
		}
		t.Logf("Running test %d: %s", i, name)

		if tt.reset {
			t.Logf(`reseting for test "%s"`, name)
			deleteDatabase(t, testName, nodes, database)
			createDatabase(t, testName, nodes, database)
			createRetentionPolicy(t, testName, nodes, database, retention)
		}

		if tt.write != "" {
			write(t, nodes[0], rewriteDbRp(tt.write, database, retention))
		}

		if tt.query != "" {
			urlDb := ""
			if tt.queryDb != "" {
				urlDb = tt.queryDb
			}
			got, ok := query(t, nodes, rewriteDbRp(urlDb, database, retention), rewriteDbRp(tt.query, database, retention), rewriteDbRp(tt.expected, database, retention))
			if !ok {
				t.Errorf(`Test "%s" failed, expected: %s, got: %s`, name, rewriteDbRp(tt.expected, database, retention), got)
			}
		}
	}
}

func TestSingleServer(t *testing.T) {
	testName := "single server integration"
	if testing.Short() {
		t.Skip(fmt.Sprintf("skipping '%s'", testName))
	}
	dir := tempfile()
	defer func() {
		os.RemoveAll(dir)
	}()

	nodes := createCombinedNodeCluster(t, testName, dir, 1, 8090)

	runTestsData(t, testName, nodes, "mydb", "myrp")
}

func Test3NodeServer(t *testing.T) {
	t.Skip()
	testName := "3-node server integration"
	if testing.Short() {
		t.Skip(fmt.Sprintf("skipping '%s'", testName))
	}
	dir := tempfile()
	defer func() {
		os.RemoveAll(dir)
	}()

	nodes := createCombinedNodeCluster(t, testName, dir, 3, 8190)

	runTestsData(t, testName, nodes, "mydb", "myrp")
}

func TestClientLibrary(t *testing.T) {
	testName := "single server integration via client library"
	if testing.Short() {
		t.Skip(fmt.Sprintf("skipping '%s'", testName))
	}
	dir := tempfile()
	defer func() {
		os.RemoveAll(dir)
	}()

	database := "mydb"
	retentionPolicy := "myrp"
	now := time.Now().UTC()

	nodes := createCombinedNodeCluster(t, testName, dir, 1, 8290)
	createDatabase(t, testName, nodes, database)
	createRetentionPolicy(t, testName, nodes, database, retentionPolicy)

	tests := []struct {
		name                         string
		bp                           client.BatchPoints
		results                      client.Results
		query                        client.Query
		writeExpected, queryExpected string
		writeErr, queryErr           string
	}{
		{
			name:          "empty batchpoint",
			writeErr:      "database is required",
			writeExpected: `{"error":"database is required"}`,
		},
		{
			name:          "no points",
			writeExpected: `{}`,
			bp:            client.BatchPoints{Database: "mydb"},
		},
		{
			name: "one point",
			bp: client.BatchPoints{
				Database: "mydb",
				Points: []client.Point{
					{Name: "cpu", Fields: map[string]interface{}{"value": 1.1}, Timestamp: now},
				},
			},
			writeExpected: `{}`,
			query:         client.Query{Command: `select * from "mydb"."myrp".cpu`},
			queryExpected: fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["%s",1.1]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
	}

	c, e := client.NewClient(client.Config{URL: *nodes[0].url})
	if e != nil {
		t.Fatalf("error creating client: %s", e)
	}

	for _, test := range tests {
		t.Logf("testing %s - %s\n", testName, test.name)
		writeResult, err := c.Write(test.bp)
		if test.writeErr != errToString(err) {
			t.Errorf("unexpected error. expected: %s, got %v", test.writeErr, err)
		}
		jsonResult := mustMarshalJSON(writeResult)
		if test.writeExpected != jsonResult {
			t.Logf("write expected result: %s\n", test.writeExpected)
			t.Logf("write got result:      %s\n", jsonResult)
			t.Error("unexpected results")
		}

		if test.query.Command != "" {
			time.Sleep(50 * time.Millisecond)
			queryResult, err := c.Query(test.query)
			if test.queryErr != errToString(err) {
				t.Errorf("unexpected error. expected: %s, got %v", test.queryErr, err)
			}
			jsonResult := mustMarshalJSON(queryResult)
			if test.queryExpected != jsonResult {
				t.Logf("query expected result: %s\n", test.queryExpected)
				t.Logf("query got result:      %s\n", jsonResult)
				t.Error("unexpected results")
			}

		}
	}
}

// helper funcs

func errToString(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

func mustMarshalJSON(v interface{}) string {
	b, e := json.Marshal(v)
	if e != nil {
		panic(e)
	}
	return string(b)
}
