package main_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb/client"
	main "github.com/influxdb/influxdb/cmd/influxd"
)

func init() {
	fmt.Printf("System Info: CPU=%d GOMAXPROCS=%d\n", runtime.NumCPU(), runtime.GOMAXPROCS(0))
}

const (
	// Use a prime batch size, so that internal batching code, which most likely
	// uses nice round batches, has to deal with leftover.
	batchSize = 4217

	openTSDBTestTimeout = 5 * time.Second
	graphiteTestTimeout = 5 * time.Second
)

type writeFn func(t *testing.T, node *TestNode, database, retention string)

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
type TestNode struct {
	node   *main.Node
	url    *url.URL
	leader bool
}

// Cluster represents a multi-node cluster.
type Cluster []*TestNode

func (c *Cluster) Close() {
	for _, n := range *c {
		if err := n.node.Close(); err != nil {
			log.Fatalf("failed to close node: %s", err)
		}
	}
}

// createCombinedNodeCluster creates a cluster of nServers nodes, each of which
// runs as both a Broker and Data node. If any part cluster creation fails,
// the testing is marked as failed.
//
// This function returns a slice of nodes, the first of which will be the leader.
func createCombinedNodeCluster(t *testing.T, testName, tmpDir string, nNodes int, baseConfig *main.Config) Cluster {
	t.Logf("Creating cluster of %d nodes for test %s", nNodes, testName)
	if nNodes < 1 {
		t.Fatalf("Test %s: asked to create nonsense cluster", testName)
	}

	nodes := make([]*TestNode, 0)

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
	c := baseConfig
	if c == nil {
		c, _ = main.NewTestConfig()
	}
	basePort := 0
	c.Broker.Dir = filepath.Join(tmpBrokerDir, strconv.Itoa(basePort))
	c.Data.Dir = filepath.Join(tmpDataDir, strconv.Itoa(basePort))
	c.Port = basePort
	c.Admin.Port = 0
	c.Admin.Enabled = false
	c.ReportingDisabled = true
	c.Snapshot.Enabled = false
	c.Logging.HTTPAccess = false

	cmd := main.NewRunCommand()
	baseNode := cmd.Open(c, "")
	b := baseNode.Broker
	s := baseNode.DataNode

	if b == nil {
		t.Fatalf("Test %s: failed to create broker on port %d", testName, basePort)
	}
	if s == nil {
		t.Fatalf("Test %s: failed to create leader data node on port %d", testName, basePort)
	}
	nodes = append(nodes, &TestNode{
		node:   baseNode,
		url:    baseNode.ClusterURL(),
		leader: true,
	})
	t.Log(baseNode.ClusterURL())

	// Create subsequent nodes, which join to first node.
	for i := 1; i < nNodes; i++ {
		c.Broker.Dir = filepath.Join(tmpBrokerDir, strconv.Itoa(i))
		c.Data.Dir = filepath.Join(tmpDataDir, strconv.Itoa(i))
		c.Port = 0

		cmd := main.NewRunCommand()
		node := cmd.Open(c, baseNode.ClusterURL().String())
		if node.Broker == nil {
			t.Fatalf("Test %s: failed to create following broker on addr %s", testName, node.ClusterURL().String())
		}
		if node.DataNode == nil {
			t.Fatalf("Test %s: failed to create following data node on addr %s", testName, node.ClusterURL().String())
		}

		nodes = append(nodes, &TestNode{
			node: node,
			url:  node.ClusterURL(),
		})
		t.Log(node.ClusterURL())

	}

	// Sanity check that we created a cluster and data nodes have unique ids
	ids := make(map[int]bool)
	for _, n := range nodes {
		ids[int(n.node.DataNode.ID())] = true
	}
	if len(ids) != len(nodes) {
		t.Fatalf("failed to create valid cluster. some data nodes have the same id: %v", ids)
	}

	return nodes
}

// createDatabase creates a database, and verifies that the creation was successful.
func createDatabase(t *testing.T, testName string, nodes Cluster, database string) {
	t.Logf("Test: %s: creating database %s", testName, database)
	query(t, nodes[:1], "", "CREATE DATABASE "+database, `{"results":[{}]}`, "")
}

// createRetentionPolicy creates a retetention policy and verifies that the creation was successful.
// Replication factor is set to equal the number nodes in the cluster.
func createRetentionPolicy(t *testing.T, testName string, nodes Cluster, database, retention string, replicationFactor int) {
	t.Logf("Creating retention policy %s for database %s", retention, database)
	command := fmt.Sprintf("CREATE RETENTION POLICY %s ON %s DURATION 1h REPLICATION %d DEFAULT", retention, database, replicationFactor)
	query(t, nodes[:1], "", command, `{"results":[{}]}`, "")
}

// deleteDatabase delete a database, and verifies that the deletion was successful.
func deleteDatabase(t *testing.T, testName string, nodes Cluster, database string) {
	t.Logf("Test: %s: deleting database %s", testName, database)
	query(t, nodes[:1], "", "DROP DATABASE "+database, `{"results":[{}]}`, "")
}

// dumpClusterDiags dumps the diagnostics of each node.
func dumpClusterDiags(t *testing.T, testName string, nodes Cluster) {
	t.Logf("Test: %s: dumping node diagnostics", testName)
	for _, n := range nodes {
		r, _, _ := query(t, Cluster{n}, "", "SHOW DIAGNOSTICS", "", "")
		t.Log(r)
	}
}

// dumpClusterStats dumps the statistics of each node.
func dumpClusterStats(t *testing.T, testName string, nodes Cluster) {
	t.Logf("Test: %s: dumping node stats", testName)
	for _, n := range nodes {
		r, _, _ := query(t, Cluster{n}, "", "SHOW STATS", "", "")
		t.Log(r)
	}
}

// writes writes the provided data to the cluster. It verfies that a 200 OK is returned by the server.
func write(t *testing.T, node *TestNode, data string) {
	u := urlFor(node.url, "write", url.Values{})

	resp, err := http.Post(u.String(), "application/json", bytes.NewReader([]byte(data)))
	if err != nil {
		t.Fatalf("Couldn't write data: %s", err)
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	if string(body) != "" {
		t.Log("BODY: ", string(body))
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		t.Fatalf("Write to database failed.  Unexpected status code.  expected: %d, actual %d, %s", http.StatusOK, resp.StatusCode, string(body))
	}
}

// query executes the given query against all nodes in the cluster, and verifies no errors occured, and
// ensures the returned data is as expected
func query(t *testing.T, nodes Cluster, urlDb, query, expected, expectPattern string) (string, bool, int) {
	v := url.Values{"q": []string{query}}
	if urlDb != "" {
		v.Set("db", urlDb)
	}

	var actual string
	// Query the data exists
	for i, n := range nodes {
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
		actual = string(body)

		if expected != "" && expected != actual {
			return actual, false, i
		} else if expectPattern != "" {
			re := regexp.MustCompile(expectPattern)
			if !re.MatchString(actual) {
				return actual, false, i
			}
		}
	}

	return actual, true, len(nodes)
}

// queryAndWait executes the given query against all nodes in the cluster, and verifies no errors occured, and
// ensures the returned data is as expected until the timeout occurs
func queryAndWait(t *testing.T, nodes Cluster, urlDb, q, expected, expectPattern string, timeout time.Duration) (string, bool, int) {
	v := url.Values{"q": []string{q}}
	if urlDb != "" {
		v.Set("db", urlDb)
	}

	sleep := 100 * time.Millisecond
	// Check to see if they set the env for duration sleep
	if sleepRaw := os.Getenv("TEST_SLEEP"); sleepRaw != "" {
		d, err := time.ParseDuration(sleepRaw)
		if err != nil {
			t.Fatal("Invalid TEST_SLEEP value:", err)
		}
		// this will limit the http log noise in the test output
		sleep = d
		timeout = d + 1
	}

	var done <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		done = timer.C
		defer timer.Stop()
	}

	nQueriedOK := 0
	for {
		got, ok, n := query(t, nodes, urlDb, q, expected, expectPattern)
		if n > nQueriedOK {
			nQueriedOK = n
		}
		if ok {
			return got, ok, nQueriedOK
		}
		select {
		case <-done:
			return got, false, nQueriedOK
		case <-time.After(sleep):
		}
	}
}

// mergeMany ensures that when merging many series together and some of them have a different number
// of points than others in a group by interval the results are correct
var mergeMany = func(t *testing.T, node *TestNode, database, retention string) {
	for i := 1; i < 11; i++ {
		for j := 1; j < 5+i%3; j++ {
			data := fmt.Sprintf(`{"database": "%s", "retentionPolicy": "%s", "points": [{"name": "cpu", "timestamp": "%s", "tags": {"host": "server_%d"}, "fields": {"value": 22}}]}`,
				database, retention, time.Unix(int64(j), int64(0)).UTC().Format(time.RFC3339), i)
			write(t, node, data)
		}
	}
}

var limitAndOffset = func(t *testing.T, node *TestNode, database, retention string) {
	for i := 1; i < 10; i++ {
		data := fmt.Sprintf(`{"database": "%s", "retentionPolicy": "%s", "points": [{"name": "cpu", "timestamp": "%s", "tags": {"region": "us-east", "host": "server-%d"}, "fields": {"value": %d}}]}`,
			database, retention, time.Unix(int64(i), int64(0)).Format(time.RFC3339), i, i)
		write(t, node, data)
	}
}

func runTest_rawDataReturnsInOrder(t *testing.T, testName string, nodes Cluster, database, retention string, replicationFactor int) {
	// skip this test if they're just looking to run some of thd data tests
	if os.Getenv("TEST_PREFIX") != "" {
		return
	}
	t.Logf("Running %s:rawDataReturnsInOrder against %d-node cluster", testName, len(nodes))

	// Start by ensuring database and retention policy exist.
	createDatabase(t, testName, nodes, database)
	createRetentionPolicy(t, testName, nodes, database, retention, replicationFactor)
	numPoints := 500
	var expected string

	for i := 1; i < numPoints; i++ {
		data := fmt.Sprintf(`{"database": "%s", "retentionPolicy": "%s", "points": [{"name": "cpu", "timestamp": "%s", "tags": {"region": "us-east", "host": "server-%d"}, "fields": {"value": %d}}]}`,
			database, retention, time.Unix(int64(i), int64(0)).Format(time.RFC3339), i%10, i)
		write(t, nodes[0], data)
	}

	expected = fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",%d]]}]}]}`, numPoints-1)
	got, ok, nOK := queryAndWait(t, nodes, database, `SELECT count(value) FROM cpu`, expected, "", 120*time.Second)
	if !ok {
		t.Errorf("test %s:rawDataReturnsInOrder failed, SELECT count() query returned unexpected data\nexp: %s\ngot: %s\n%d nodes responded correctly", testName, expected, got, nOK)
		dumpClusterDiags(t, testName, nodes)
		dumpClusterStats(t, testName, nodes)
	}

	// Create expected JSON string dynamically.
	expectedValues := make([]string, 0)
	for i := 1; i < numPoints; i++ {
		expectedValues = append(expectedValues, fmt.Sprintf(`["%s",%d]`, time.Unix(int64(i), int64(0)).UTC().Format(time.RFC3339), i))
	}
	expected = fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[%s]}]}]}`, strings.Join(expectedValues, ","))
	got, ok, nOK = query(t, nodes, database, `SELECT value FROM cpu`, expected, "")
	if !ok {
		t.Errorf("test %s failed, SELECT query returned unexpected data\nexp: %s\ngot: %s\n%d nodes responded correctly", testName, expected, got, nOK)
		dumpClusterDiags(t, testName, nodes)
		dumpClusterStats(t, testName, nodes)
	}
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
			got, ok, nOK := query(t, nodes, "", tt.query, tt.expected, "")
			if !ok {
				t.Errorf("Test '%s' failed, expected: %s, got: %s, %d nodes responded correctly", tt.name, tt.expected, got, nOK)
			}
		}
	}
}

// runTests tests write and query of data. Setting testNumbers allows only a subset of tests to be run.
func runTestsData(t *testing.T, testName string, nodes Cluster, database, retention string, replicationFactor int) {
	t.Logf("Running tests against %d-node cluster", len(nodes))

	yesterday := time.Now().Add(-1 * time.Hour * 24).UTC()
	now := time.Now().UTC()
	maxFloat64, _ := json.Marshal(math.MaxFloat64)

	// Start by ensuring database and retention policy exist.
	createDatabase(t, testName, nodes, database)
	createRetentionPolicy(t, testName, nodes, database, retention, replicationFactor)

	// The tests. Within these tests %DB% and %RP% will be replaced with the database and retention passed into
	// this function.
	tests := []struct {
		skip          bool    // Skip the test.
		reset         bool    // Delete and recreate the database.
		name          string  // Test name, for easy-to-read test log output.
		write         string  // If equal to the empty string, no data is written.
		writeFn       writeFn // If non-nil, called after 'write' data (if any) is written.
		query         string  // If equal to the blank string, no query is executed.
		queryDb       string  // If set, is used as the "db" query param.
		queryOne      bool    // If set, only 1 node is queried.
		expected      string  // If 'query' is equal to the blank string, this is ignored.
		expectPattern string  // Regexp alternative to expected field. (ignored if expected is set)
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
			name:     "single point count query with timestamp",
			query:    `SELECT count(value) FROM "%DB%"."%RP%".cpu`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
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
			query:    `SELECT * FROM "%DB%"."%RP%".cpu WHERE time < NOW()`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		{
			name:     "single point, select with now(), two queries",
			query:    `SELECT * FROM "%DB%"."%RP%".cpu WHERE time < now();SELECT * FROM "%DB%"."%RP%".cpu WHERE time < now()`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",100]]}]},{"series":[{"name":"cpu","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},

		{
			name:          "measurement not found",
			query:         `SELECT value FROM "%DB%"."%RP%".foobarbaz`,
			expectPattern: `.*measurement not found.*`,
		},
		{
			name:     "field not found",
			query:    `SELECT abc FROM "%DB%"."%RP%".cpu WHERE time < now()`,
			expected: `{"results":[{"error":"unknown field or tag name in select clause: abc"}]}`,
		},
		{
			name:     "empty result",
			query:    `SELECT value FROM cpu WHERE time >= '3000-01-01 00:00:05'`,
			queryDb:  "%DB%",
			expected: `{"results":[{}]}`,
		},

		{
			reset: true,
			name:  "two points with timestamp",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "cpu", "timestamp": "2015-02-28T01:04:36.703820946Z", "fields": {"value": 100}},
		                                                                         {"name": "cpu", "timestamp": "2015-02-28T01:03:36.703820946Z", "fields": {"value": 100}}
		                                                                        ]}`,
			query:    `SELECT * FROM "%DB%"."%RP%".cpu`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",100],["2015-02-28T01:04:36.703820946Z",100]]}]}]}`,
		},

		{
			reset: true,
			name:  "two points with negative values",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "cpu", "timestamp": "2015-02-28T01:04:36.703820946Z", "fields": {"value": -200}},
                                                                                 {"name": "cpu", "timestamp": "2015-02-28T01:03:36.703820946Z", "fields": {"value": -100}}
                                                                                ]}`,
			query:    `SELECT * FROM "%DB%"."%RP%".cpu`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",-100],["2015-02-28T01:04:36.703820946Z",-200]]}]}]}`,
		},

		// Data read and write tests using relative time
		{
			reset:    true,
			name:     "single point with timestamp pre-calculated for past time queries yesterday",
			write:    `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "cpu", "timestamp": "` + yesterday.Format(time.RFC3339Nano) + `", "tags": {"host": "server01"}, "fields": {"value": 100}}]}`,
			query:    `SELECT * FROM "%DB%"."%RP%".cpu where time >= '` + yesterday.Add(-1*time.Minute).Format(time.RFC3339Nano) + `'`,
			expected: fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["%s",100]]}]}]}`, yesterday.Format(time.RFC3339Nano)),
		},
		{
			reset:    true,
			name:     "single point with timestamp pre-calculated for relative time queries now",
			write:    `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "cpu", "timestamp": "` + now.Format(time.RFC3339Nano) + `", "tags": {"host": "server01"}, "fields": {"value": 100}}]}`,
			query:    `SELECT * FROM "%DB%"."%RP%".cpu where time >= now() - 1m`,
			expected: fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["%s",100]]}]}]}`, now.Format(time.RFC3339Nano)),
		},

		// Merge tests.
		{
			reset:    true,
			name:     "merge many",
			writeFn:  mergeMany,
			query:    `SELECT count(value) FROM cpu WHERE time >= '1970-01-01T00:00:01Z' AND time <= '1970-01-01T00:00:06Z' GROUP BY time(1s)`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:01Z",10],["1970-01-01T00:00:02Z",10],["1970-01-01T00:00:03Z",10],["1970-01-01T00:00:04Z",10],["1970-01-01T00:00:05Z",7],["1970-01-01T00:00:06Z",3]]}]}]}`,
		},

		// Limit and offset
		{
			reset:    true,
			name:     "limit and offset",
			writeFn:  limitAndOffset,
			query:    `SELECT count(value) FROM cpu GROUP BY * SLIMIT 2 SOFFSET 1`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","tags":{"host":"server-2","region":"us-east"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]},{"name":"cpu","tags":{"host":"server-3","region":"us-east"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		},
		{
			query:    `SELECT count(value) FROM cpu GROUP BY * SLIMIT 2 SOFFSET 3`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","tags":{"host":"server-4","region":"us-east"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]},{"name":"cpu","tags":{"host":"server-5","region":"us-east"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		},
		{
			query:    `SELECT count(value) FROM cpu GROUP BY * SLIMIT 3 SOFFSET 8`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","tags":{"host":"server-9","region":"us-east"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		},

		// FROM regex tests
		{
			reset: true,
			name:  "FROM regex using default db and rp",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
				{"name": "cpu1", "timestamp": "2015-02-28T01:03:36.703820946Z", "tags": {"host": "server01"}, "fields": {"value": 10}},
				{"name": "cpu2", "timestamp": "2015-02-28T01:03:36.703820946Z", "tags": {"host": "server01"}, "fields": {"value": 20}},
				{"name": "cpu3", "timestamp": "2015-02-28T01:03:36.703820946Z", "tags": {"host": "server01"}, "fields": {"value": 30}}
			]}`,
			query:    `SELECT * FROM /cpu[13]/`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu1","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",10]]},{"name":"cpu3","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",30]]}]}]}`,
		},
		{
			name:     `FROM regex specifying db and rp`,
			query:    `SELECT * FROM "%DB%"."%RP%"./cpu[13]/`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu1","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",10]]},{"name":"cpu3","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",30]]}]}]}`,
		},
		{
			name:     `FROM regex using specified db and default rp`,
			query:    `SELECT * FROM "%DB%"../cpu[13]/`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu1","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",10]]},{"name":"cpu3","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",30]]}]}]}`,
		},
		{
			name:     `FROM regex using default db and specified rp`,
			query:    `SELECT * FROM "%RP%"./cpu[13]/`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu1","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",10]]},{"name":"cpu3","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",30]]}]}]}`,
		},

		// Aggregations
		{
			reset: true,
			name:  "stddev with just one point",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
				{"name": "cpu", "timestamp": "2015-04-20T14:27:41Z", "fields": {"value": 45}}
			]}`,
			query:    `SELECT STDDEV(value) FROM cpu`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","stddev"],"values":[["1970-01-01T00:00:00Z",null]]}]}]}`,
		},
		{
			reset: true,
			name:  "large mean and stddev",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
				{"name": "cpu", "timestamp": "2015-04-20T14:27:40Z", "fields": {"value": ` + string(maxFloat64) + `}},
				{"name": "cpu", "timestamp": "2015-04-20T14:27:41Z", "fields": {"value": ` + string(maxFloat64) + `}}
			]}`,
			query:    `SELECT MEAN(value), STDDEV(value) FROM cpu`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","mean","stddev"],"values":[["1970-01-01T00:00:00Z",` + string(maxFloat64) + `,0]]}]}]}`,
		},
		{
			reset: true,
			name:  "mean and stddev",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
				{"name": "cpu", "timestamp": "2000-01-01T00:00:00Z", "fields": {"value": 2}},
				{"name": "cpu", "timestamp": "2000-01-01T00:00:10Z", "fields": {"value": 4}},
				{"name": "cpu", "timestamp": "2000-01-01T00:00:20Z", "fields": {"value": 4}},
				{"name": "cpu", "timestamp": "2000-01-01T00:00:30Z", "fields": {"value": 4}},
				{"name": "cpu", "timestamp": "2000-01-01T00:00:40Z", "fields": {"value": 5}},
				{"name": "cpu", "timestamp": "2000-01-01T00:00:50Z", "fields": {"value": 5}},
				{"name": "cpu", "timestamp": "2000-01-01T00:01:00Z", "fields": {"value": 7}},
				{"name": "cpu", "timestamp": "2000-01-01T00:01:10Z", "fields": {"value": 9}}
			]}`,
			query:    `SELECT MEAN(value), STDDEV(value) FROM cpu WHERE time >= '2000-01-01' AND time < '2000-01-01T00:02:00Z' GROUP BY time(10m)`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","mean","stddev"],"values":[["2000-01-01T00:00:00Z",5,2.138089935299395]]}]}]}`,
		},
		{
			name: "median - even sample size",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
				{"name": "cpu-even", "timestamp": "2000-01-01T00:00:00Z", "tags": {"region": "us-east"}, "fields": {"value": 200}},
				{"name": "cpu-even", "timestamp": "2000-01-01T00:00:10Z", "tags": {"region": "us-east"}, "fields": {"value": 30}},
				{"name": "cpu-even", "timestamp": "2000-01-01T00:00:20Z", "tags": {"region": "us-east"}, "fields": {"value": 40}},
				{"name": "cpu-even", "timestamp": "2000-01-01T00:00:30Z", "tags": {"region": "us-west"}, "fields": {"value": 100}}
			]}`,
			query:    `SELECT median(value) FROM "cpu-even"`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu-even","columns":["time","median"],"values":[["1970-01-01T00:00:00Z",70]]}]}]}`,
		},
		{
			name: "median - odd sample size",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
				{"name": "cpu-odd", "timestamp": "2000-01-01T00:00:00Z", "tags": {"region": "us-east"}, "fields": {"value": 200}},
				{"name": "cpu-odd", "timestamp": "2000-01-01T00:00:10Z", "tags": {"region": "us-east"}, "fields": {"value": 30}},
				{"name": "cpu-odd", "timestamp": "2000-01-01T00:00:20Z", "tags": {"region": "us-west"}, "fields": {"value": 100}}
			]}`,
			query:    `SELECT median(value) FROM "cpu-odd"`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu-odd","columns":["time","median"],"values":[["1970-01-01T00:00:00Z",100]]}]}]}`,
		},
		{
			reset: true,
			name:  "aggregations",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
				{"name": "cpu", "timestamp": "2000-01-01T00:00:00Z", "tags": {"region": "us-east"}, "fields": {"value": 20}},
				{"name": "cpu", "timestamp": "2000-01-01T00:00:10Z", "tags": {"region": "us-east"}, "fields": {"value": 30}},
				{"name": "cpu", "timestamp": "2000-01-01T00:00:00Z", "tags": {"region": "us-west"}, "fields": {"value": 100}}
			]}`,
			query:    `SELECT value FROM cpu WHERE time >= '2000-01-01 00:00:05'`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:10Z",30]]}]}]}`,
		},
		{
			name:     "sum aggregation",
			query:    `SELECT SUM(value) FROM cpu WHERE time >= '2000-01-01 00:00:05' AND time <= '2000-01-01T00:00:10Z' GROUP BY time(10s), region`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","tags":{"region":"us-east"},"columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",null],["2000-01-01T00:00:10Z",30]]}]}]}`,
		},
		{
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
				{"name": "cpu", "timestamp": "2000-01-01T00:00:03Z", "tags": {"region": "us-east"}, "fields": {"otherVal": 20}}
			]}`,
			name:     "aggregation with a null field value",
			query:    `SELECT SUM(value) FROM cpu GROUP BY region`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","tags":{"region":"us-east"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",50]]},{"name":"cpu","tags":{"region":"us-west"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",100]]}]}]}`,
		},
		{
			name:     "multiple aggregations",
			query:    `SELECT SUM(value), MEAN(value) FROM cpu GROUP BY region`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","tags":{"region":"us-east"},"columns":["time","sum","mean"],"values":[["1970-01-01T00:00:00Z",50,25]]},{"name":"cpu","tags":{"region":"us-west"},"columns":["time","sum","mean"],"values":[["1970-01-01T00:00:00Z",100,100]]}]}]}`,
		},
		{
			query:    `SELECT sum(value) / mean(value) as div FROM cpu GROUP BY region`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","tags":{"region":"us-east"},"columns":["time","div"],"values":[["1970-01-01T00:00:00Z",2]]},{"name":"cpu","tags":{"region":"us-west"},"columns":["time","div"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		},
		{
			name: "group by multiple dimensions",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
				{"name": "load", "timestamp": "2000-01-01T00:00:00Z", "tags": {"region": "us-east", "host": "serverA"}, "fields": {"value": 20}},
				{"name": "load", "timestamp": "2000-01-01T00:00:10Z", "tags": {"region": "us-east", "host": "serverB"}, "fields": {"value": 30}},
				{"name": "load", "timestamp": "2000-01-01T00:00:00Z", "tags": {"region": "us-west", "host": "serverC"}, "fields": {"value": 100}}
			]}`,
			query:    `SELECT sum(value) FROM load GROUP BY region, host`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"load","tags":{"host":"serverA","region":"us-east"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",20]]},{"name":"load","tags":{"host":"serverB","region":"us-east"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",30]]},{"name":"load","tags":{"host":"serverC","region":"us-west"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",100]]}]}]}`,
		},
		{
			name: "WHERE with AND",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
				{"name": "cpu", "timestamp": "2000-01-01T00:00:03Z", "tags": {"region": "uk", "host": "serverZ", "service": "redis"}, "fields": {"value": 20}},
				{"name": "cpu", "timestamp": "2000-01-01T00:00:03Z", "tags": {"region": "uk", "host": "serverZ", "service": "mysql"}, "fields": {"value": 30}}
			]}`,
			query:    `SELECT sum(value) FROM cpu WHERE region='uk' AND host='serverZ'`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",50]]}]}]}`,
		},

		// Precision-specified writes
		{
			name:     "single string point with second precision timestamp",
			write:    `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "cpu_s_precision", "timestamp": 1, "precision": "s", "fields": {"value": 100}}]}`,
			query:    `SELECT * FROM "%DB%"."%RP%".cpu_s_precision`,
			expected: `{"results":[{"series":[{"name":"cpu_s_precision","columns":["time","value"],"values":[["1970-01-01T00:00:01Z",100]]}]}]}`,
		},
		{
			name:     "single string point with millisecond precision timestamp",
			write:    `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "cpu_ms_precision", "timestamp": 1000, "precision": "ms", "fields": {"value": 100}}]}`,
			query:    `SELECT * FROM "%DB%"."%RP%".cpu_ms_precision`,
			expected: `{"results":[{"series":[{"name":"cpu_ms_precision","columns":["time","value"],"values":[["1970-01-01T00:00:01Z",100]]}]}]}`,
		},
		{
			name:     "single string point with nanosecond precision timestamp",
			write:    `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "cpu_n_precision", "timestamp": 2000000000, "precision": "n", "fields": {"value": 100}}]}`,
			query:    `SELECT * FROM "%DB%"."%RP%".cpu_n_precision`,
			expected: `{"results":[{"series":[{"name":"cpu_n_precision","columns":["time","value"],"values":[["1970-01-01T00:00:02Z",100]]}]}]}`,
		},
		{
			name:     "single point count query with nanosecond precision timestamp",
			query:    `SELECT count(value) FROM "%DB%"."%RP%".cpu_n_precision`,
			expected: `{"results":[{"series":[{"name":"cpu_n_precision","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		},

		// Wildcard queries
		{
			reset: true,
			name:  "wildcard queries",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
				{"name": "cpu", "timestamp": "2000-01-01T00:00:00Z", "tags": {"region": "us-east"}, "fields": {"value": 10}},
				{"name": "cpu", "timestamp": "2000-01-01T00:00:10Z", "tags": {"region": "us-east"}, "fields": {"val-x": 20}},
				{"name": "cpu", "timestamp": "2000-01-01T00:00:20Z", "tags": {"region": "us-east"}, "fields": {"value": 30, "val-x": 40}}
			]}`,
			query:    `SELECT * FROM cpu`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","val-x","value"],"values":[["2000-01-01T00:00:00Z",null,10],["2000-01-01T00:00:10Z",20,null],["2000-01-01T00:00:20Z",40,30]]}]}]}`,
		},
		{
			reset: true,
			name:  "wildcard GROUP BY queries",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
				{"name": "cpu", "timestamp": "2000-01-01T00:00:00Z", "tags": {"region": "us-east"}, "fields": {"value": 10}},
				{"name": "cpu", "timestamp": "2000-01-01T00:00:10Z", "tags": {"region": "us-east"}, "fields": {"value": 20}},
				{"name": "cpu", "timestamp": "2000-01-01T00:00:20Z", "tags": {"region": "us-west"}, "fields": {"value": 30}}
			]}`,
			query:    `SELECT mean(value) FROM cpu GROUP BY *`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","tags":{"region":"us-east"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",15]]},{"name":"cpu","tags":{"region":"us-west"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",30]]}]}]}`,
		},
		{
			name:     "wildcard GROUP BY queries with time",
			query:    `SELECT mean(value) FROM cpu WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:01:00Z' GROUP BY *,TIME(1m)`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","tags":{"region":"us-east"},"columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",15]]},{"name":"cpu","tags":{"region":"us-west"},"columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",30]]}]}]}`,
		},

		// WHERE tag queries
		{
			reset: true,
			name:  "WHERE tags SELECT single field (EQ tag value1)",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "cpu", "timestamp": "2015-02-28T01:03:36.703820946Z", "tags": {"host": "server01", "region": "us-west"}, "fields": {"value": 100}},
			                                                                     {"name": "cpu", "timestamp": "2010-02-28T01:03:37.703820946Z", "tags": {"host": "server02"}, "fields": {"value": 200}},
			                                                                     {"name": "cpu", "timestamp": "2012-02-28T01:03:38.703820946Z", "tags": {"host": "server03"}, "fields": {"value": 300}}]}`,
			query:    `SELECT value FROM "%DB%"."%RP%".cpu WHERE host = 'server01'`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		{
			name:     "WHERE tags SELECT single field (2 EQ tags)",
			query:    `SELECT value FROM "%DB%"."%RP%".cpu WHERE host = 'server01' AND region = 'us-west'`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		{
			name:     "WHERE tags SELECT single field (OR different tags)",
			query:    `SELECT value FROM "%DB%"."%RP%".cpu WHERE host = 'server03' OR region = 'us-west'`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2012-02-28T01:03:38.703820946Z",300],["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		{
			name:     "WHERE tags SELECT single field (OR same tags)",
			query:    `SELECT value FROM "%DB%"."%RP%".cpu WHERE host = 'server01' OR host = 'server02'`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2010-02-28T01:03:37.703820946Z",200],["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		{
			name:     "WHERE tags SELECT single field (OR with non-existent tag value)",
			query:    `SELECT value FROM "%DB%"."%RP%".cpu WHERE host = 'server01' OR host = 'server66'`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		{
			name:     "WHERE tags SELECT single field (OR with all tag values)",
			query:    `SELECT value FROM "%DB%"."%RP%".cpu WHERE host = 'server01' OR host = 'server02' OR host = 'server03'`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2010-02-28T01:03:37.703820946Z",200],["2012-02-28T01:03:38.703820946Z",300],["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		{
			name:     "WHERE tags SELECT single field (1 EQ and 1 NEQ tag)",
			query:    `SELECT value FROM "%DB%"."%RP%".cpu WHERE host = 'server01' AND region != 'us-west'`,
			expected: `{"results":[{}]}`,
		},
		{
			name:     "WHERE tags SELECT single field (EQ tag value2)",
			query:    `SELECT value FROM "%DB%"."%RP%".cpu WHERE host = 'server02'`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2010-02-28T01:03:37.703820946Z",200]]}]}]}`,
		},
		{
			name:     "WHERE tags SELECT single field (NEQ tag value1)",
			query:    `SELECT value FROM "%DB%"."%RP%".cpu WHERE host != 'server01'`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2010-02-28T01:03:37.703820946Z",200],["2012-02-28T01:03:38.703820946Z",300]]}]}]}`,
		},
		{
			name:     "WHERE tags SELECT single field (NEQ tag value1 AND NEQ tag value2)",
			query:    `SELECT value FROM "%DB%"."%RP%".cpu WHERE host != 'server01' AND host != 'server02'`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2012-02-28T01:03:38.703820946Z",300]]}]}]}`,
		},
		{
			name:     "WHERE tags SELECT single field (NEQ tag value1 OR NEQ tag value2)",
			query:    `SELECT value FROM "%DB%"."%RP%".cpu WHERE host != 'server01' OR host != 'server02'`, // Yes, this is always true, but that's the point.
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2010-02-28T01:03:37.703820946Z",200],["2012-02-28T01:03:38.703820946Z",300],["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		{
			name:     "WHERE tags SELECT single field (NEQ tag value1 AND NEQ tag value2 AND NEQ tag value3)",
			query:    `SELECT value FROM "%DB%"."%RP%".cpu WHERE host != 'server01' AND host != 'server02' AND host != 'server03'`,
			expected: `{"results":[{}]}`,
		},
		{
			reset: true,
			name:  "WHERE tags SELECT single field (NEQ tag value1, point without any tags)",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "cpu", "timestamp": "2015-02-28T01:03:36.703820946Z", "tags": {"host": "server01"}, "fields": {"value": 100}},
                                                                                 {"name": "cpu", "timestamp": "2012-02-28T01:03:38.703820946Z", "fields": {"value": 200}}]}`,
			query:    `SELECT value FROM "%DB%"."%RP%".cpu WHERE host != 'server01'`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2012-02-28T01:03:38.703820946Z",200]]}]}]}`,
		},
		{
			reset: true,
			name:  "WHERE tags SELECT single field (regex tag no match)",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "cpu", "timestamp": "2015-02-28T01:03:36.703820946Z", "tags": {"host": "server01"}, "fields": {"value": 100}},
                                                                                 {"name": "cpu", "timestamp": "2012-02-28T01:03:38.703820946Z", "fields": {"value": 200}}]}`,
			query:    `SELECT value FROM "%DB%"."%RP%".cpu WHERE host !~ /server01/`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2012-02-28T01:03:38.703820946Z",200]]}]}]}`,
		},
		{
			name:     "WHERE tags SELECT single field (regex tag match)",
			query:    `SELECT value FROM "%DB%"."%RP%".cpu WHERE host =~ /server01/`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		{
			name:     "WHERE tags SELECT single field (regex tag match)",
			query:    `SELECT value FROM "%DB%"."%RP%".cpu WHERE host !~ /server[23]/`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2012-02-28T01:03:38.703820946Z",200],["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},

		// WHERE fields queries
		{
			reset:    true,
			name:     "WHERE fields SELECT single field",
			write:    `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "cpu", "timestamp": "2015-02-28T01:03:36.703820946Z", "fields": {"alert_id": "alert", "tenant_id": "tenant", "_cust": "acme"}}]}`,
			query:    `SELECT alert_id FROM "%DB%"."%RP%".cpu WHERE alert_id='alert'`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","alert_id"],"values":[["2015-02-28T01:03:36.703820946Z","alert"]]}]}]}`,
		},
		{
			name:     "WHERE fields with AND query, all fields in SELECT",
			query:    `SELECT alert_id,tenant_id,_cust FROM "%DB%"."%RP%".cpu WHERE alert_id='alert' AND tenant_id='tenant'`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","alert_id","tenant_id","_cust"],"values":[["2015-02-28T01:03:36.703820946Z","alert","tenant","acme"]]}]}]}`,
		},
		{
			name:     "WHERE fields with AND query, all fields in SELECT, one in parenthesis",
			query:    `SELECT alert_id,tenant_id FROM "%DB%"."%RP%".cpu WHERE alert_id='alert' AND (tenant_id='tenant')`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","alert_id","tenant_id"],"values":[["2015-02-28T01:03:36.703820946Z","alert","tenant"]]}]}]}`,
		},
		{
			name:     "WHERE fields with underscored field",
			query:    `SELECT alert_id FROM "%DB%"."%RP%".cpu WHERE _cust='acme'`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","alert_id"],"values":[["2015-02-28T01:03:36.703820946Z","alert"]]}]}]}`,
		},
		{
			name: "select where field greater than some value",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "cpu", "timestamp": "2009-11-10T23:00:02Z", "fields": {"load": 100}},
			                                                                      {"name": "cpu", "timestamp": "2009-11-10T23:01:02Z", "fields": {"load": 80}}]}`,
			query:    `select load from "%DB%"."%RP%".cpu where load > 100`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","load"]}]}]}`,
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
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","load"]}]}]}`,
		},
		{
			query:    `select load from "%DB%"."%RP%".cpu where load < 99`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","load"],"values":[["2009-11-10T23:01:02Z",80]]}]}]}`,
		},
		{
			query:    `select load from "%DB%"."%RP%".cpu where load < 80`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","load"]}]}]}`,
		},
		{
			query:    `select load from "%DB%"."%RP%".cpu where load != 100`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","load"],"values":[["2009-11-10T23:01:02Z",80]]}]}]}`,
		},
		{
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "logs", "timestamp": "2009-11-10T23:00:02Z","fields": {"event": "disk full"}},
			                                                                        {"name": "logs", "timestamp": "2009-11-10T23:02:02Z","fields": {"event": "disk not full"}}]}`,
			query:    `select event from "%DB%"."%RP%".logs where event = 'disk full'`,
			expected: `{"results":[{"series":[{"name":"logs","columns":["time","event"],"values":[["2009-11-10T23:00:02Z","disk full"]]}]}]}`,
		},
		{
			write:    `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [{"name": "logs", "timestamp": "2009-11-10T23:00:02Z","fields": {"event": "disk full"}}]}`,
			query:    `select event from "%DB%"."%RP%".logs where event = 'nonsense'`,
			expected: `{"results":[{"series":[{"name":"logs","columns":["time","event"]}]}]}`,
		},
		{
			name:          "missing measurement with `GROUP BY *`",
			query:         `select load from "%DB%"."%RP%".missing group by *`,
			expectPattern: `.*measurement not found: .*missing.*`,
		},
		{
			name: "where on a tag, field and time",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
				{"name": "where_events", "timestamp": "2009-11-10T23:00:02Z","fields": {"foo": "bar"}, "tags": {"tennant": "paul"}},
				{"name": "where_events", "timestamp": "2009-11-10T23:00:03Z","fields": {"foo": "baz"}, "tags": {"tennant": "paul"}},
				{"name": "where_events", "timestamp": "2009-11-10T23:00:04Z","fields": {"foo": "bat"}, "tags": {"tennant": "paul"}},
				{"name": "where_events", "timestamp": "2009-11-10T23:00:05Z","fields": {"foo": "bar"}, "tags": {"tennant": "todd"}}
			]}`,
			query:    `select foo from "%DB%"."%RP%".where_events where tennant = 'paul' AND time > 1s AND (foo = 'bar' OR foo = 'baz')`,
			expected: `{"results":[{"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"],["2009-11-10T23:00:03Z","baz"]]}]}]}`,
		},
		{
			name:          "where on tag that should be double quoted but isn't",
			queryDb:       "%DB%",
			query:         `show series where data-center = 'foo'`,
			expectPattern: `invalid expression: .*`,
		},

		// LIMIT and OFFSET tests

		{
			name: "limit1 on points",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
				{"name": "limit", "timestamp": "2009-11-10T23:00:02Z","fields": {"foo": 2}, "tags": {"tennant": "paul"}},
				{"name": "limit", "timestamp": "2009-11-10T23:00:03Z","fields": {"foo": 3}, "tags": {"tennant": "paul"}},
				{"name": "limit", "timestamp": "2009-11-10T23:00:04Z","fields": {"foo": 4}, "tags": {"tennant": "paul"}},
				{"name": "limit", "timestamp": "2009-11-10T23:00:05Z","fields": {"foo": 5}, "tags": {"tennant": "todd"}}
			]}`,
			query:    `select foo from "%DB%"."%RP%"."limit" LIMIT 2`,
			expected: `{"results":[{"series":[{"name":"limit","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z",2],["2009-11-10T23:00:03Z",3]]}]}]}`,
		},
		{
			name:     "limit higher than the number of data points",
			query:    `select foo from "%DB%"."%RP%"."limit" LIMIT 20`,
			expected: `{"results":[{"series":[{"name":"limit","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z",2],["2009-11-10T23:00:03Z",3],["2009-11-10T23:00:04Z",4],["2009-11-10T23:00:05Z",5]]}]}]}`,
		},
		{
			name:     "limit and offset",
			query:    `select foo from "%DB%"."%RP%"."limit" LIMIT 2 OFFSET 1`,
			expected: `{"results":[{"series":[{"name":"limit","columns":["time","foo"],"values":[["2009-11-10T23:00:03Z",3],["2009-11-10T23:00:04Z",4]]}]}]}`,
		},
		{
			name:     "limit + offset equal to total number of points",
			query:    `select foo from "%DB%"."%RP%"."limit" LIMIT 3 OFFSET 3`,
			expected: `{"results":[{"series":[{"name":"limit","columns":["time","foo"],"values":[["2009-11-10T23:00:05Z",5]]}]}]}`,
		},
		{
			name:     "limit - offset higher than number of points",
			query:    `select foo from "%DB%"."%RP%"."limit" LIMIT 2 OFFSET 20`,
			expected: `{"results":[{"series":[{"name":"limit","columns":["time","foo"]}]}]}`,
		},
		{
			name:     "limit on points with group by time",
			query:    `select mean(foo) from "%DB%"."%RP%"."limit" WHERE time >= '2009-11-10T23:00:02Z' AND time < '2009-11-10T23:00:06Z' GROUP BY TIME(1s) LIMIT 2`,
			expected: `{"results":[{"series":[{"name":"limit","columns":["time","mean"],"values":[["2009-11-10T23:00:02Z",2],["2009-11-10T23:00:03Z",3]]}]}]}`,
		},
		{
			name:     "limit higher than the number of data points with group by time",
			query:    `select mean(foo) from "%DB%"."%RP%"."limit" WHERE time >= '2009-11-10T23:00:02Z' AND time < '2009-11-10T23:00:06Z' GROUP BY TIME(1s) LIMIT 20`,
			expected: `{"results":[{"series":[{"name":"limit","columns":["time","mean"],"values":[["2009-11-10T23:00:02Z",2],["2009-11-10T23:00:03Z",3],["2009-11-10T23:00:04Z",4],["2009-11-10T23:00:05Z",5]]}]}]}`,
		},
		{
			name:     "limit and offset with group by time",
			query:    `select mean(foo) from "%DB%"."%RP%"."limit" WHERE time >= '2009-11-10T23:00:02Z' AND time < '2009-11-10T23:00:06Z' GROUP BY TIME(1s) LIMIT 2 OFFSET 1`,
			expected: `{"results":[{"series":[{"name":"limit","columns":["time","mean"],"values":[["2009-11-10T23:00:03Z",3],["2009-11-10T23:00:04Z",4]]}]}]}`,
		},
		{
			name:     "limit + offset equal to the  number of points with group by time",
			query:    `select mean(foo) from "%DB%"."%RP%"."limit" WHERE time >= '2009-11-10T23:00:02Z' AND time < '2009-11-10T23:00:06Z' GROUP BY TIME(1s) LIMIT 3 OFFSET 3`,
			expected: `{"results":[{"series":[{"name":"limit","columns":["time","mean"],"values":[["2009-11-10T23:00:05Z",5]]}]}]}`,
		},
		{
			name:     "limit - offset higher than number of points with group by time",
			query:    `select mean(foo) from "%DB%"."%RP%"."limit" WHERE time >= '2009-11-10T23:00:02Z' AND time < '2009-11-10T23:00:06Z' GROUP BY TIME(1s) LIMIT 2 OFFSET 20`,
			expected: `{"results":[{}]}`,
		},
		{
			name:     "limit higher than the number of data points should error",
			query:    `select mean(foo)  from "%DB%"."%RP%"."limit"  where  time > '2000-01-01T00:00:00Z' group by time(1s), * fill(0)  limit 2147483647`,
			expected: `{"results":[{"error":"too many points in the group by interval. maybe you forgot to specify a where time clause?"}]}`,
		},
		{
			name:     "limit1 higher than MaxGroupBy but the number of data points is less than MaxGroupBy",
			query:    `select mean(foo)  from "%DB%"."%RP%"."limit"  where  time >= '2009-11-10T23:00:02Z' and time < '2009-11-10T23:00:03Z' group by time(1s), * fill(0)  limit 2147483647`,
			expected: `{"results":[{"series":[{"name":"limit","tags":{"tennant":"paul"},"columns":["time","mean"],"values":[["2009-11-10T23:00:02Z",2]]}]}]}`,
		},

		// Fill tests
		{
			name: "fill with value",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
				{"name": "fills", "timestamp": "2009-11-10T23:00:02Z","fields": {"val": 3}},
				{"name": "fills", "timestamp": "2009-11-10T23:00:03Z","fields": {"val": 5}},
				{"name": "fills", "timestamp": "2009-11-10T23:00:06Z","fields": {"val": 4}},
				{"name": "fills", "timestamp": "2009-11-10T23:00:16Z","fields": {"val": 10}}
			]}`,
			query:    `select mean(val) from "%DB%"."%RP%".fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s) FILL(1)`,
			expected: `{"results":[{"series":[{"name":"fills","columns":["time","mean"],"values":[["2009-11-10T23:00:00Z",4],["2009-11-10T23:00:05Z",4],["2009-11-10T23:00:10Z",1],["2009-11-10T23:00:15Z",10]]}]}]}`,
		},
		{
			name:     "fill with previous",
			query:    `select mean(val) from "%DB%"."%RP%".fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s) FILL(previous)`,
			expected: `{"results":[{"series":[{"name":"fills","columns":["time","mean"],"values":[["2009-11-10T23:00:00Z",4],["2009-11-10T23:00:05Z",4],["2009-11-10T23:00:10Z",4],["2009-11-10T23:00:15Z",10]]}]}]}`,
		},
		{
			name:     "fill with none, i.e. clear out nulls",
			query:    `select mean(val) from "%DB%"."%RP%".fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s) FILL(none)`,
			expected: `{"results":[{"series":[{"name":"fills","columns":["time","mean"],"values":[["2009-11-10T23:00:00Z",4],["2009-11-10T23:00:05Z",4],["2009-11-10T23:00:15Z",10]]}]}]}`,
		},
		{
			name:     "fill defaults to null",
			query:    `select mean(val) from "%DB%"."%RP%".fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s)`,
			expected: `{"results":[{"series":[{"name":"fills","columns":["time","mean"],"values":[["2009-11-10T23:00:00Z",4],["2009-11-10T23:00:05Z",4],["2009-11-10T23:00:10Z",null],["2009-11-10T23:00:15Z",10]]}]}]}`,
		},
		{
			name:     "fill with count aggregate defaults to null",
			query:    `select count(val) from "%DB%"."%RP%".fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s)`,
			expected: `{"results":[{"series":[{"name":"fills","columns":["time","count"],"values":[["2009-11-10T23:00:00Z",2],["2009-11-10T23:00:05Z",1],["2009-11-10T23:00:10Z",null],["2009-11-10T23:00:15Z",1]]}]}]}`,
		},
		{
			name:     "fill with count aggregate specific value",
			query:    `select count(val) from "%DB%"."%RP%".fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s) fill(1234)`,
			expected: `{"results":[{"series":[{"name":"fills","columns":["time","count"],"values":[["2009-11-10T23:00:00Z",2],["2009-11-10T23:00:05Z",1],["2009-11-10T23:00:10Z",1234],["2009-11-10T23:00:15Z",1]]}]}]}`,
		},

		// Drop Measurement, series tags preserved tests
		{
			reset: true,
			name:  "Drop Measurement, series tags preserved tests",
			write: `{"database" : "%DB%", "retentionPolicy" : "%RP%", "points": [
				{"name": "cpu", "timestamp": "2000-01-01T00:00:00Z", "tags": {"host": "serverA", "region": "uswest"}, "fields": {"val": 23.2}},
				{"name": "memory", "timestamp": "2000-01-01T00:00:01Z", "tags": {"host": "serverB", "region": "uswest"}, "fields": {"val": 33.2}}
			]}`,
			query:    `SHOW MEASUREMENTS`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"measurements","columns":["name"],"values":[["cpu"],["memory"]]}]}]}`,
		},
		{
			query:    `SHOW SERIES`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","columns":["_id","host","region"],"values":[[1,"serverA","uswest"]]},{"name":"memory","columns":["_id","host","region"],"values":[[2,"serverB","uswest"]]}]}]}`,
		},
		{
			name:     "ensure we can query for memory with both tags",
			query:    `SELECT * FROM memory where region='uswest' and host='serverB'`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"memory","columns":["time","val"],"values":[["2000-01-01T00:00:01Z",33.2]]}]}]}`,
		},
		{
			query:    `DROP MEASUREMENT cpu`,
			queryDb:  "%DB%",
			queryOne: true,
			expected: `{"results":[{}]}`,
		},
		{
			query:    `SHOW MEASUREMENTS`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"measurements","columns":["name"],"values":[["memory"]]}]}]}`,
		},
		{
			query:    `SHOW SERIES`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"memory","columns":["_id","host","region"],"values":[[2,"serverB","uswest"]]}]}]}`,
		},
		{
			query:         `SELECT * FROM cpu`,
			queryDb:       "%DB%",
			expectPattern: `measurement not found: .*cpu.*`,
		},
		{
			query:    `SELECT * FROM memory where host='serverB'`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"memory","columns":["time","val"],"values":[["2000-01-01T00:00:01Z",33.2]]}]}]}`,
		},
		{
			query:    `SELECT * FROM memory where region='uswest'`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"memory","columns":["time","val"],"values":[["2000-01-01T00:00:01Z",33.2]]}]}]}`,
		},
		{
			query:    `SELECT * FROM memory where region='uswest' and host='serverB'`,
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"memory","columns":["time","val"],"values":[["2000-01-01T00:00:01Z",33.2]]}]}]}`,
		},

		// Drop non-existant measurement tests
		{
			reset:         true,
			name:          "Drop non-existant measurement",
			query:         `DROP MEASUREMENT doesntexist`,
			queryDb:       "%DB%",
			queryOne:      true,
			expectPattern: `measurement not found: doesntexist.*`,
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
			expected: `{"results":[{"series":[{"name":"cpu","columns":["_id","host","region"],"values":[[1,"server01",""],[2,"server01","uswest"],[3,"server01","useast"],[4,"server02","useast"]]},{"name":"gpu","columns":["_id","host","region"],"values":[[5,"server02","useast"],[6,"server03","caeast"]]}]}]}`,
		},
		{
			query:    "SHOW SERIES FROM cpu",
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","columns":["_id","host","region"],"values":[[1,"server01",""],[2,"server01","uswest"],[3,"server01","useast"],[4,"server02","useast"]]}]}]}`,
		},
		{
			query:    "SHOW SERIES WHERE region = 'uswest'",
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","columns":["_id","host","region"],"values":[[2,"server01","uswest"]]}]}]}`,
		},
		{
			query:    "SHOW SERIES WHERE region =~ /ca.*/",
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"gpu","columns":["_id","host","region"],"values":[[6,"server03","caeast"]]}]}]}`,
		},
		{
			query:    "SHOW SERIES WHERE host !~ /server0[12]/",
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"gpu","columns":["_id","host","region"],"values":[[6,"server03","caeast"]]}]}]}`,
		},
		{
			query:    "SHOW SERIES FROM cpu WHERE region = 'useast'",
			queryDb:  "%DB%",
			expected: `{"results":[{"series":[{"name":"cpu","columns":["_id","host","region"],"values":[[3,"server01","useast"],[4,"server02","useast"]]}]}]}`,
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
			query:         "SHOW TAG KEYS FROM bad",
			queryDb:       "%DB%",
			expectPattern: `measurement not found: bad.*`,
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

		// Database control tests
		{
			reset:    true,
			name:     "Create database for default retention policy tests",
			query:    `CREATE DATABASE mydatabase`,
			queryOne: true,
			expected: `{"results":[{}]}`,
		},
		{
			name:     "show databases",
			query:    `SHOW DATABASES`,
			expected: `{"results":[{"series":[{"name":"databases","columns":["name"],"values":[["mydatabase"],["mydb"]]}]}]}`,
		},
		{
			name:     "Check for default retention policy",
			query:    `SHOW RETENTION POLICIES mydatabase`,
			expected: `{"results":[{"series":[{"columns":["name","duration","replicaN","default"],"values":[["default","0",1,true]]}]}]}`,
		},
		{
			name:     "Ensure retention policy with infinite retention can be created",
			query:    `CREATE RETENTION POLICY rp1 ON mydatabase DURATION INF REPLICATION 1`,
			queryOne: true,
			expected: `{"results":[{}]}`,
		},
		{
			name:     "Ensure default retention policy can be changed",
			query:    `ALTER RETENTION POLICY rp1 ON mydatabase DEFAULT`,
			queryOne: true,
			expected: `{"results":[{}]}`,
		},
		{
			name:     "Make sure default retention policy actually changed",
			query:    `SHOW RETENTION POLICIES mydatabase`,
			expected: `{"results":[{"series":[{"columns":["name","duration","replicaN","default"],"values":[["default","0",1,false],["rp1","0",1,true]]}]}]}`,
		},
		{
			name:     "Ensure retention policy with acceptable retention can be created",
			query:    `CREATE RETENTION POLICY rp2 ON mydatabase DURATION 30d REPLICATION 1`,
			queryOne: true,
			expected: `{"results":[{}]}`,
		},
		{
			name:     "Ensure retention policy with unacceptable retention cannot be created",
			query:    `CREATE RETENTION POLICY rp3 ON mydatabase DURATION 1s REPLICATION 1`,
			queryOne: true,
			expected: `{"results":[{"error":"retention policy duration needs to be at least 1h0m0s"}]}`,
		},
		{
			name:     "Ensure database with default retention policy can be deleted",
			query:    `DROP DATABASE mydatabase`,
			queryOne: true,
			expected: `{"results":[{}]}`,
		},
		{
			name:          "Check error when dropping non-existent database",
			query:         `DROP DATABASE mydatabase`,
			queryOne:      true,
			expectPattern: `database not found: mydatabase.*`,
		},
		{
			name:          "Check error when creating retention policy on non-existent database",
			query:         `CREATE RETENTION POLICY rp1 ON mydatabase DURATION INF REPLICATION 1`,
			queryOne:      true,
			expectPattern: `database not found: mydatabase.*`,
		},
		{
			name:          "Check error when deleting retention policy on non-existent database",
			query:         `DROP RETENTION POLICY rp1 ON mydatabase`,
			queryOne:      true,
			expectPattern: `database not found: mydatabase.*`,
		},

		// User control tests
		{
			name:     "show users, no actual users",
			query:    `SHOW USERS`,
			expected: `{"results":[{"series":[{"columns":["user","admin"]}]}]}`,
		},
		{
			query:    `CREATE USER jdoe WITH PASSWORD '1337'`,
			queryOne: true,
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
			queryOne: true,
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
			query:    `CREATE CONTINUOUS QUERY "my.query" ON %DB% BEGIN SELECT count(value) INTO measure1 FROM myseries GROUP BY time(10m) END`,
			queryOne: true,
			expected: `{"results":[{}]}`,
		},
		{
			name:     `show continuous queries`,
			query:    `SHOW CONTINUOUS QUERIES`,
			expected: `{"results":[{"series":[{"name":"%DB%","columns":["name","query"],"values":[["my.query","CREATE CONTINUOUS QUERY \"my.query\" ON %DB% BEGIN SELECT count(value) INTO \"%DB%\".\"%RP%\".measure1 FROM \"%DB%\".\"%RP%\".myseries GROUP BY time(10m) END"]]}]}]}`,
		},
	}

	// See if we should run a subset of this test
	testPrefix := os.Getenv("INFLUXDB_TEST_PREFIX")
	if testPrefix != "" {
		t.Logf("Skipping all tests that do not match the prefix of %q\n", testPrefix)
	}

	// Get env var to filter which tests we run.
	var filter *regexp.Regexp
	if pattern := os.Getenv("INFLUXDB_TEST_RUN"); pattern != "" {
		fmt.Printf("pattern = %#v\n", pattern)
		filter = regexp.MustCompile(pattern)
	}

	for i, tt := range tests {
		name := tt.name
		if name == "" {
			name = tt.query
		}

		// If a prefix was specified throught the INFLUXDB_TEST_PREFIX env var, filter by that.
		if testPrefix != "" && !strings.HasPrefix(name, testPrefix) {
			tt.skip = true
		}

		// If a regex pattern was specified through the INFLUXDB_TEST_RUN env var, filter by that.
		if filter != nil && !filter.MatchString(tt.query) {
			tt.skip = true
		}

		if tt.skip {
			t.Logf("SKIPPING TEST %s", tt.name)
			continue
		}

		fmt.Printf("TEST %d '%s:%s'\n", i, testName, name)
		t.Logf("Running test %d '%s:%s'", i, testName, name)

		if tt.reset {
			t.Logf(`reseting for test "%s"`, name)
			deleteDatabase(t, testName, nodes, database)
			createDatabase(t, testName, nodes, database)
			createRetentionPolicy(t, testName, nodes, database, retention, replicationFactor)
		}

		if tt.write != "" {
			write(t, nodes[0], rewriteDbRp(tt.write, database, retention))
		}

		if tt.writeFn != nil {
			tt.writeFn(t, nodes[0], database, retention)
		}

		if tt.query != "" {
			qNodes := nodes
			if tt.queryOne {
				qNodes = qNodes[:1]
			}

			urlDb := ""
			if tt.queryDb != "" {
				urlDb = tt.queryDb
			}
			qry := rewriteDbRp(tt.query, database, retention)
			got, ok, nOK := queryAndWait(t, qNodes, rewriteDbRp(urlDb, database, retention), qry, rewriteDbRp(tt.expected, database, retention), rewriteDbRp(tt.expectPattern, database, retention), 30*time.Second)
			if !ok {
				if tt.expected != "" {
					t.Errorf("Test #%d: \"%s:%s\" failed\n  query: %s\n  exp: %s\n  got: %s\n%d nodes responded correctly", i, testName, name, qry, rewriteDbRp(tt.expected, database, retention), got, nOK)
				} else {
					t.Errorf("Test #%d: \"%s:%s\" failed\n  query: %s\n  exp: %s\n  got: %s\n%d nodes responded correctly", i, testName, name, qry, rewriteDbRp(tt.expectPattern, database, retention), got, nOK)
				}
				dumpClusterDiags(t, name, nodes)
				dumpClusterStats(t, name, nodes)
			}
		}
	}
}

// Ensures that diagnostics can be written to the internal database.
func TestServerDiags(t *testing.T) {
	t.Parallel()
	testName := "single server integration diagnostics"
	if testing.Short() {
		t.Skip(fmt.Sprintf("skipping '%s'", testName))
	}
	dir := tempfile()
	defer os.RemoveAll(dir)

	config := main.NewConfig()
	config.Monitoring.Enabled = true
	config.Monitoring.WriteInterval = main.Duration(100 * time.Millisecond)
	nodes := createCombinedNodeCluster(t, testName, dir, 3, config)
	defer nodes.Close()

	// Ensure some data shards also exist.
	createDatabase(t, testName, nodes, "mydb")
	createRetentionPolicy(t, testName, nodes, "mydb", "myrp", len(nodes))
	write(t, nodes[0], `{"database" : "mydb", "retentionPolicy" : "myrp", "points": [{"name": "cpu", "fields": {"value": 100}}]}`)

	time.Sleep(1 * time.Second)
}

func TestSingleServer(t *testing.T) {
	t.Parallel()
	testName := "single server integration"
	if testing.Short() {
		t.Skip(fmt.Sprintf("skipping '%s'", testName))
	}
	dir := tempfile()
	defer os.RemoveAll(dir)

	nodes := createCombinedNodeCluster(t, testName, dir, 1, nil)
	defer nodes.Close()

	runTestsData(t, testName, nodes, "mydb", "myrp", len(nodes))
	runTest_rawDataReturnsInOrder(t, testName, nodes, "mydb", "myrp", len(nodes))
}

func Test3NodeServer(t *testing.T) {
	t.Parallel()
	testName := "3-node server integration"

	if testing.Short() {
		t.Skip(fmt.Sprintf("skipping '%s'", testName))
	}
	dir := tempfile()
	defer os.RemoveAll(dir)

	nodes := createCombinedNodeCluster(t, testName, dir, 3, nil)
	defer nodes.Close()

	runTestsData(t, testName, nodes, "mydb", "myrp", len(nodes))
	runTest_rawDataReturnsInOrder(t, testName, nodes, "mydb", "myrp", len(nodes))
}

func Test3NodeServerFailover(t *testing.T) {
	t.Parallel()
	testName := "3-node server failover integration"

	if testing.Short() {
		t.Skip(fmt.Sprintf("skipping '%s'", testName))
	}
	dir := tempfile()
	defer os.RemoveAll(dir)

	nodes := createCombinedNodeCluster(t, testName, dir, 3, nil)

	// kill the last node, cluster should expect it to be there
	nodes[2].node.Close()
	nodes = nodes[:len(nodes)-1]

	runTestsData(t, testName, nodes, "mydb", "myrp", len(nodes))
	runTest_rawDataReturnsInOrder(t, testName, nodes, "mydb", "myrp", len(nodes))
	nodes.Close()
}

// ensure that all queries work if there are more nodes in a cluster than the replication factor
// and there is more than 1 shards
func Test5NodeClusterPartiallyReplicated(t *testing.T) {
	t.Parallel()
	testName := "5-node server integration partial replication"
	if testing.Short() {
		t.Skip(fmt.Sprintf("skipping '%s'", testName))
	}
	dir := tempfile()
	defer os.RemoveAll(dir)

	nodes := createCombinedNodeCluster(t, testName, dir, 5, nil)
	defer nodes.Close()

	runTestsData(t, testName, nodes, "mydb", "myrp", 2)
	runTest_rawDataReturnsInOrder(t, testName, nodes, "mydb", "myrp", 2)
}

func TestClientLibrary(t *testing.T) {
	t.Parallel()
	testName := "single server integration via client library"
	if testing.Short() {
		t.Skip(fmt.Sprintf("skipping '%s'", testName))
	}
	dir := tempfile()
	defer os.RemoveAll(dir)

	now := time.Now().UTC()

	nodes := createCombinedNodeCluster(t, testName, dir, 1, nil)
	defer nodes.Close()

	type write struct {
		bp       client.BatchPoints
		expected string
		err      string
	}
	type query struct {
		query    client.Query
		expected string
		err      string
	}
	type test struct {
		name    string
		db      string
		rp      string
		writes  []write
		queries []query
	}

	tests := []test{
		{
			name: "empty batchpoint",
			writes: []write{
				{
					err:      "database is required",
					expected: `{"error":"database is required"}`,
				},
			},
		},
		{
			name: "no points",
			writes: []write{
				{
					expected: `null`,
					bp:       client.BatchPoints{Database: "mydb"},
				},
			},
		},
		{
			name: "one point",
			writes: []write{
				{
					bp: client.BatchPoints{
						Database: "mydb",
						Points: []client.Point{
							{Name: "cpu", Fields: map[string]interface{}{"value": 1.1}, Timestamp: now},
						},
					},
					expected: `null`,
				},
			},
			queries: []query{
				{
					query:    client.Query{Command: `select * from "mydb"."myrp".cpu`},
					expected: fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["%s",1.1]]}]}]}`, now.Format(time.RFC3339Nano)),
				},
			},
		},
		{
			name: "mulitple points, multiple values",
			writes: []write{
				{bp: client.BatchPoints{Database: "mydb", Points: []client.Point{{Name: "network", Fields: map[string]interface{}{"rx": 1.1, "tx": 2.1}, Timestamp: now}}}, expected: `null`},
				{bp: client.BatchPoints{Database: "mydb", Points: []client.Point{{Name: "network", Fields: map[string]interface{}{"rx": 1.2, "tx": 2.2}, Timestamp: now.Add(time.Nanosecond)}}}, expected: `null`},
				{bp: client.BatchPoints{Database: "mydb", Points: []client.Point{{Name: "network", Fields: map[string]interface{}{"rx": 1.3, "tx": 2.3}, Timestamp: now.Add(2 * time.Nanosecond)}}}, expected: `null`},
			},
			queries: []query{
				{
					query:    client.Query{Command: `select * from "mydb"."myrp".network`},
					expected: fmt.Sprintf(`{"results":[{"series":[{"name":"network","columns":["time","rx","tx"],"values":[["%s",1.1,2.1],["%s",1.2,2.2],["%s",1.3,2.3]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(time.Nanosecond).Format(time.RFC3339Nano), now.Add(2*time.Nanosecond).Format(time.RFC3339Nano)),
				},
			},
		},
	}

	c, e := client.NewClient(client.Config{URL: *nodes[0].url})
	if e != nil {
		t.Fatalf("error creating client: %s", e)
	}

	for _, test := range tests {
		if test.db == "" {
			test.db = "mydb"
		}
		if test.rp == "" {
			test.rp = "myrp"
		}
		createDatabase(t, testName, nodes, test.db)
		createRetentionPolicy(t, testName, nodes, test.db, test.rp, len(nodes))
		t.Logf("testing %s - %s\n", testName, test.name)
		for _, w := range test.writes {
			writeResult, err := c.Write(w.bp)
			if w.err != errToString(err) {
				t.Errorf("unexpected error. expected: %s, got %v", w.err, err)
			}
			jsonResult := mustMarshalJSON(writeResult)
			if w.expected != jsonResult {
				t.Logf("write expected result: %s\n", w.expected)
				t.Logf("write got result:      %s\n", jsonResult)
				t.Error("unexpected results")
			}
		}

		for _, q := range test.queries {
			if q.query.Command != "" {
				time.Sleep(500 * time.Millisecond)
				queryResponse, err := c.Query(q.query)
				if q.err != errToString(err) {
					t.Errorf("unexpected error. expected: %s, got %v", q.err, err)
				}
				jsonResult := mustMarshalJSON(queryResponse)
				if q.expected != jsonResult {
					t.Logf("query expected result: %s\n", q.expected)
					t.Logf("query got result:      %s\n", jsonResult)
					t.Error("unexpected results")
				}
			}
		}
		deleteDatabase(t, testName, nodes, test.db)
	}
}

func Test_ServerSingleGraphiteIntegration_Default(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip()
	}
	nNodes := 1
	testName := "graphite integration"
	dir := tempfile()
	now := time.Now().UTC().Round(time.Second)
	c, _ := main.NewTestConfig()
	c.Port = 0
	c.Admin.Enabled = false
	g := main.Graphite{
		Enabled:     true,
		Database:    "graphite",
		Protocol:    "TCP",
		BindAddress: "127.0.0.1",
		Port:        0,
	}
	c.Graphites = append(c.Graphites, g)

	t.Logf("Graphite Connection String: %s\n", g.ConnectionString())
	nodes := createCombinedNodeCluster(t, testName, dir, nNodes, c)
	defer nodes.Close()

	createDatabase(t, testName, nodes, "graphite")
	createRetentionPolicy(t, testName, nodes, "graphite", "raw", len(nodes))

	// Get the address for the graphite endpoint that we just spun up
	host := nodes[0].node.GraphiteServers[0].Host()
	// Connect to the graphite endpoint we just spun up
	conn, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatal(err)
		return
	}

	t.Log("Writing data")
	data := []byte(`cpu 23.456 `)
	data = append(data, []byte(fmt.Sprintf("%d", now.Unix()))...)
	data = append(data, '\n')
	_, err = conn.Write(data)
	conn.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	expected := fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","cpu"],"values":[["%s",23.456]]}]}]}`, now.Format(time.RFC3339Nano))

	// query and wait for results
	got, ok, _ := queryAndWait(t, nodes, "graphite", `select * from "graphite"."raw".cpu`, expected, "", graphiteTestTimeout)
	if !ok {
		t.Errorf(`Test "%s" failed, expected: %s, got: %s`, testName, expected, got)
	}
}

func Test_ServerSingleGraphiteIntegration_FractionalTime(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip()
	}
	nNodes := 1
	testName := "graphite integration fractional time"
	dir := tempfile()
	now := time.Now().UTC().Round(time.Second).Add(500 * time.Millisecond)
	c, _ := main.NewTestConfig()
	c.Port = 0
	c.Admin.Enabled = false
	g := main.Graphite{
		Enabled:     true,
		Database:    "graphite",
		Protocol:    "TCP",
		BindAddress: "127.0.0.1",
		Port:        0,
	}
	c.Graphites = append(c.Graphites, g)

	t.Logf("Graphite Connection String: %s\n", g.ConnectionString())
	nodes := createCombinedNodeCluster(t, testName, dir, nNodes, c)
	defer nodes.Close()

	createDatabase(t, testName, nodes, "graphite")
	createRetentionPolicy(t, testName, nodes, "graphite", "raw", len(nodes))

	// Get the address for the graphite endpoint that we just spun up
	host := nodes[0].node.GraphiteServers[0].Host()
	// Connect to the graphite endpoint we just spun up
	conn, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatal(err)
		return
	}

	t.Log("Writing data")
	data := []byte(`cpu 23.456 `)
	data = append(data, []byte(fmt.Sprintf("%d", now.Unix()))...)
	data = append(data, []byte(".5")...)
	data = append(data, '\n')
	_, err = conn.Write(data)
	conn.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	expected := fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","cpu"],"values":[["%s",23.456]]}]}]}`, now.Format(time.RFC3339Nano))

	// query and wait for results
	got, ok, _ := queryAndWait(t, nodes, "graphite", `select * from "graphite"."raw".cpu`, expected, "", graphiteTestTimeout)
	if !ok {
		t.Errorf(`Test "%s" failed, expected: %s, got: %s`, testName, expected, got)
	}
}

func Test_ServerSingleGraphiteIntegration_ZeroDataPoint(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip()
	}
	nNodes := 1
	testName := "graphite integration"
	dir := tempfile()
	now := time.Now().UTC().Round(time.Second)
	c, _ := main.NewTestConfig()
	c.Port = 0
	c.Admin.Enabled = false
	g := main.Graphite{
		Enabled:     true,
		Database:    "graphite",
		Protocol:    "TCP",
		BindAddress: "127.0.0.1",
		Port:        0,
	}
	c.Graphites = append(c.Graphites, g)

	t.Logf("Graphite Connection String: %s\n", g.ConnectionString())
	nodes := createCombinedNodeCluster(t, testName, dir, nNodes, c)
	defer nodes.Close()

	createDatabase(t, testName, nodes, "graphite")
	createRetentionPolicy(t, testName, nodes, "graphite", "raw", len(nodes))

	// Get the address for the graphite endpoint that we just spun up
	host := nodes[0].node.GraphiteServers[0].Host()
	// Connect to the graphite endpoint we just spun up
	conn, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatal(err)
		return
	}

	t.Log("Writing data")
	data := []byte(`cpu 0.000 `)
	data = append(data, []byte(fmt.Sprintf("%d", now.Unix()))...)
	data = append(data, '\n')
	_, err = conn.Write(data)
	conn.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	expected := fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","cpu"],"values":[["%s",0]]}]}]}`, now.Format(time.RFC3339Nano))

	// query and wait for results
	got, ok, _ := queryAndWait(t, nodes, "graphite", `select * from "graphite"."raw".cpu`, expected, "", graphiteTestTimeout)
	if !ok {
		t.Errorf(`Test "%s" failed, expected: %s, got: %s`, testName, expected, got)
	}
}

func Test_ServerSingleGraphiteIntegration_NoDatabase(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip()
	}
	nNodes := 1
	testName := "graphite integration"
	dir := tempfile()
	now := time.Now().UTC().Round(time.Second)
	c, _ := main.NewTestConfig()
	c.Port = 0
	c.Admin.Enabled = false
	g := main.Graphite{
		Enabled:     true,
		Protocol:    "TCP",
		BindAddress: "127.0.0.1",
		Port:        0,
	}
	c.Graphites = append(c.Graphites, g)
	c.Logging.WriteTracing = true
	t.Logf("Graphite Connection String: %s\n", g.ConnectionString())
	nodes := createCombinedNodeCluster(t, testName, dir, nNodes, c)
	defer nodes.Close()

	// Get the address for the graphite endpoint that we just spun up
	host := nodes[0].node.GraphiteServers[0].Host()
	// Connect to the graphite endpoint we just spun up
	conn, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatal(err)
		return
	}

	// Need to wait for the database to be created
	expected := `{"results":[{"series":[{"name":"databases","columns":["name"],"values":[["graphite"]]}]}]}`
	got, ok, _ := queryAndWait(t, nodes, "graphite", `show databases`, expected, "", 2*time.Second)
	if !ok {
		t.Errorf(`Test "%s" failed, expected: %s, got: %s`, testName, expected, got)
	}

	// Need to wait for the database to get a default retention policy
	expected = `{"results":[{"series":[{"columns":["name","duration","replicaN","default"],"values":[["default","0",1,true]]}]}]}`
	got, ok, _ = queryAndWait(t, nodes, "graphite", `show retention policies graphite`, expected, "", graphiteTestTimeout)
	if !ok {
		t.Errorf(`Test "%s" failed, expected: %s, got: %s`, testName, expected, got)
	}

	t.Log("Writing data")
	data := []byte(`cpu 23.456 `)
	data = append(data, []byte(fmt.Sprintf("%d", now.Unix()))...)
	data = append(data, '\n')
	_, err = conn.Write(data)
	conn.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	// Wait for data to show up
	expected = fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","cpu"],"values":[["%s",23.456]]}]}]}`, now.Format(time.RFC3339Nano))
	got, ok, _ = queryAndWait(t, nodes, "graphite", `select * from "graphite"."default".cpu`, expected, "", 2*time.Second)
	if !ok {
		t.Errorf(`Test "%s" failed, expected: %s, got: %s`, testName, expected, got)
	}
}

func Test_ServerOpenTSDBIntegration(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip()
	}
	nNodes := 1
	testName := "opentsdb integration"
	dir := tempfile()
	now := time.Now().UTC().Round(time.Second)
	c, _ := main.NewTestConfig()
	o := main.OpenTSDB{
		Addr:            "127.0.0.1",
		Port:            0,
		Enabled:         true,
		Database:        "opentsdb",
		RetentionPolicy: "raw",
	}
	c.OpenTSDB = o

	t.Logf("OpenTSDB Connection String: %s\n", o.ListenAddress())
	nodes := createCombinedNodeCluster(t, testName, dir, nNodes, c)
	defer nodes.Close()

	createDatabase(t, testName, nodes, "opentsdb")
	createRetentionPolicy(t, testName, nodes, "opentsdb", "raw", len(nodes))

	// Connect to the openTSDB endpoint we just spun up
	host := nodes[0].node.OpenTSDBServer.Addr().String()
	conn, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatal(err)
		return
	}

	t.Log("Writing data")
	data := []byte(`put cpu `)
	data = append(data, []byte(fmt.Sprintf("%d", now.Unix()))...)
	data = append(data, []byte(" 10\n")...)
	_, err = conn.Write(data)
	conn.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	expected := fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["%s",10]]}]}]}`, now.Format(time.RFC3339Nano))

	// query and wait for results
	got, ok, _ := queryAndWait(t, nodes, "opentsdb", `select * from "opentsdb"."raw".cpu`, expected, "", openTSDBTestTimeout)
	if !ok {
		t.Errorf(`Test "%s" failed, expected: %s, got: %s`, testName, expected, got)
	}
}

func Test_ServerOpenTSDBIntegration_WithTags(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip()
	}
	nNodes := 1
	testName := "opentsdb integration"
	dir := tempfile()
	now := time.Now().UTC().Round(time.Second)
	c, _ := main.NewTestConfig()
	c.Port = 0
	c.Admin.Enabled = false
	o := main.OpenTSDB{
		Addr:            "127.0.0.1",
		Port:            0,
		Enabled:         true,
		Database:        "opentsdb",
		RetentionPolicy: "raw",
	}
	c.OpenTSDB = o

	t.Logf("OpenTSDB Connection String: %s\n", o.ListenAddress())
	nodes := createCombinedNodeCluster(t, testName, dir, nNodes, c)
	defer nodes.Close()

	createDatabase(t, testName, nodes, "opentsdb")
	createRetentionPolicy(t, testName, nodes, "opentsdb", "raw", len(nodes))

	// Connect to the openTSDB endpoint we just spun up
	host := nodes[0].node.OpenTSDBServer.Addr().String()
	conn, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatal(err)
		return
	}

	t.Log("Writing data")
	data := []byte(`put cpu `)
	data = append(data, []byte(fmt.Sprintf("%d", now.Unix()))...)
	data = append(data, []byte(" 10 tag1=val1 tag2=val2\n")...)
	data = append(data, `put cpu `...)
	data = append(data, []byte(fmt.Sprintf("%d", now.Unix()))...)
	data = append(data, []byte(" 20 tag1=val3 tag2=val4\n")...)
	_, err = conn.Write(data)
	conn.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	expected := fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["%s",20]]}]}]}`, now.Format(time.RFC3339Nano))

	// query and wait for results
	got, ok, _ := queryAndWait(t, nodes, "opentsdb", `select * from "opentsdb"."raw".cpu where tag1='val3'`, expected, "", openTSDBTestTimeout)
	if !ok {
		t.Errorf(`Test "%s" failed, expected: %s, got: %s`, testName, expected, got)
	}
}

func Test_ServerOpenTSDBIntegration_BadData(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip()
	}
	nNodes := 1
	testName := "opentsdb integration"
	dir := tempfile()
	now := time.Now().UTC().Round(time.Second)
	c, _ := main.NewTestConfig()
	c.Port = 0
	c.Admin.Enabled = false
	o := main.OpenTSDB{
		Addr:            "127.0.0.1",
		Port:            0,
		Enabled:         true,
		Database:        "opentsdb",
		RetentionPolicy: "raw",
	}
	c.OpenTSDB = o

	t.Logf("OpenTSDB Connection String: %s\n", o.ListenAddress())
	nodes := createCombinedNodeCluster(t, testName, dir, nNodes, c)
	defer nodes.Close()

	createDatabase(t, testName, nodes, "opentsdb")
	createRetentionPolicy(t, testName, nodes, "opentsdb", "raw", len(nodes))

	// Connect to the openTSDB endpoint we just spun up
	host := nodes[0].node.OpenTSDBServer.Addr().String()
	conn, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatal(err)
		return
	}

	t.Log("Writing data")
	data := []byte("This is bad and invalid input\n")
	data = append(data, `put cpu `...)
	data = append(data, []byte(fmt.Sprintf("%d", now.Unix()))...)
	data = append(data, []byte(" 10 tag1=val1 tag2=val2\n")...)
	_, err = conn.Write(data)
	conn.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	expected := fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["%s",10]]}]}]}`, now.Format(time.RFC3339Nano))

	// query and wait for results
	got, ok, _ := queryAndWait(t, nodes, "opentsdb", `select * from "opentsdb"."raw".cpu`, expected, "", openTSDBTestTimeout)
	if !ok {
		t.Errorf(`Test "%s" failed, expected: %s, got: %s`, testName, expected, got)
	}
}

func TestSeparateBrokerDataNode(t *testing.T) {
	t.Parallel()
	testName := "TestSeparateBrokerDataNode"
	if testing.Short() {
		t.Skip("Skipping", testName)
	}

	tmpDir := tempfile()
	tmpBrokerDir := filepath.Join(tmpDir, "broker-integration-test")
	tmpDataDir := filepath.Join(tmpDir, "data-integration-test")
	t.Logf("Test %s: using tmp directory %q for brokers\n", testName, tmpBrokerDir)
	t.Logf("Test %s: using tmp directory %q for data nodes\n", testName, tmpDataDir)
	// Sometimes if a test fails, it's because of a log.Fatal() in the program.
	// This prevents the defer from cleaning up directories.
	// To be safe, nuke them always before starting
	_ = os.RemoveAll(tmpBrokerDir)
	_ = os.RemoveAll(tmpDataDir)

	brokerConfig := main.NewConfig()
	brokerConfig.Port = 0
	brokerConfig.Admin.Enabled = false
	brokerConfig.Data.Enabled = false
	brokerConfig.Broker.Dir = filepath.Join(tmpBrokerDir, strconv.Itoa(brokerConfig.Port))
	brokerConfig.ReportingDisabled = true

	dataConfig := main.NewConfig()
	dataConfig.Port = 0
	dataConfig.Admin.Enabled = false
	dataConfig.Broker.Enabled = false
	dataConfig.Data.Dir = filepath.Join(tmpDataDir, strconv.Itoa(dataConfig.Port))
	dataConfig.ReportingDisabled = true

	brokerCmd := main.NewRunCommand()
	broker := brokerCmd.Open(brokerConfig, "")
	defer broker.Close()

	if broker.Broker == nil {
		t.Fatalf("Test %s: failed to create broker on port %d", testName, brokerConfig.Port)
	}

	u := broker.Broker.URL()
	dataCmd := main.NewRunCommand()

	data := dataCmd.Open(dataConfig, (&u).String())
	defer data.Close()

	if data.DataNode == nil {
		t.Fatalf("Test %s: failed to create leader data node on port %d", testName, dataConfig.Port)
	}
}

func TestSeparateBrokerTwoDataNodes(t *testing.T) {
	t.Parallel()
	testName := "TestSeparateBrokerTwoDataNodes"
	if testing.Short() {
		t.Skip("Skipping", testName)
	}

	tmpDir := tempfile()
	tmpBrokerDir := filepath.Join(tmpDir, "broker-integration-test")
	tmpDataDir := filepath.Join(tmpDir, "data-integration-test")
	t.Logf("Test %s: using tmp directory %q for brokers\n", testName, tmpBrokerDir)
	t.Logf("Test %s: using tmp directory %q for data nodes\n", testName, tmpDataDir)
	// Sometimes if a test fails, it's because of a log.Fatal() in the program.
	// This prevents the defer from cleaning up directories.
	// To be safe, nuke them always before starting
	_ = os.RemoveAll(tmpBrokerDir)
	_ = os.RemoveAll(tmpDataDir)

	// Start a single broker node
	brokerConfig := main.NewConfig()
	brokerConfig.Port = 0
	brokerConfig.Admin.Enabled = false
	brokerConfig.Data.Enabled = false
	brokerConfig.Broker.Dir = filepath.Join(tmpBrokerDir, "1")
	brokerConfig.ReportingDisabled = true

	brokerCmd := main.NewRunCommand()
	broker := brokerCmd.Open(brokerConfig, "")
	defer broker.Close()

	if broker.Broker == nil {
		t.Fatalf("Test %s: failed to create broker on port %d", testName, brokerConfig.Port)
	}

	u := broker.Broker.URL()
	brokerURL := (&u).String()

	// Star the first data node and join the broker
	dataConfig1 := main.NewConfig()
	dataConfig1.Port = 0
	dataConfig1.Admin.Enabled = false
	dataConfig1.Broker.Enabled = false
	dataConfig1.Data.Dir = filepath.Join(tmpDataDir, "1")
	dataConfig1.ReportingDisabled = true

	dataCmd1 := main.NewRunCommand()

	data1 := dataCmd1.Open(dataConfig1, brokerURL)
	defer data1.Close()

	if data1.DataNode == nil {
		t.Fatalf("Test %s: failed to create leader data node on port %d", testName, dataConfig1.Port)
	}

	// Join data node 2 to single broker and first data node
	dataConfig2 := main.NewConfig()
	dataConfig2.Port = 0
	dataConfig2.Admin.Enabled = false
	dataConfig2.Broker.Enabled = false
	dataConfig2.Data.Dir = filepath.Join(tmpDataDir, "2")
	dataConfig2.ReportingDisabled = true

	dataCmd2 := main.NewRunCommand()

	data2 := dataCmd2.Open(dataConfig2, brokerURL)

	defer data2.Close()
	if data2.DataNode == nil {
		t.Fatalf("Test %s: failed to create leader data node on port %d", testName, dataConfig2.Port)
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

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }
