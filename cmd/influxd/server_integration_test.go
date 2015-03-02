package main_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/messaging"

	main "github.com/influxdb/influxdb/cmd/influxd"
)

const (
	// Use a prime batch size, so that internal batching code, which most likely
	// uses nice round batches, has to deal with leftover.
	batchSize = 4217
)

// urlFor returns a URL with the path and query params correctly appended and set.
func urlFor(u *url.URL, path string, params url.Values) *url.URL {
	v, _ := url.Parse(u.String())
	v.Path = path
	v.RawQuery = params.Encode()
	return v
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
	query(t, nodes[:1], "CREATE DATABASE "+database, `{"results":[{}]}`)
}

// createRetentionPolicy creates a retetention policy and verifies that the creation was successful.
func createRetentionPolicy(t *testing.T, testName string, nodes Cluster, database, retention string) {
	t.Log("Creating retention policy")
	command := fmt.Sprintf("CREATE RETENTION POLICY %s ON %s DURATION 1h REPLICATION %d DEFAULT", retention, database, len(nodes))
	query(t, nodes[:1], command, `{"results":[{}]}`)
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
func query(t *testing.T, nodes Cluster, query, expected string) (string, bool) {
	// Query the data exists
	for _, n := range nodes {
		u := urlFor(n.url, "query", url.Values{"q": []string{query}})
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
			got, ok := query(t, nodes, tt.query, tt.expected)
			if !ok {
				t.Errorf("Test '%s' failed, expected: %s, got: %s", tt.name, tt.expected, got)
			}
		}
	}
}

// runTests tests write and query of data.
func runTestsData(t *testing.T, nodes Cluster) {
	t.Logf("Running tests against %d-node cluster", len(nodes))

	tests := []struct {
		name     string
		write    string // If equal to the empty string, no data is written.
		query    string // If equal to the blank string, no query is executed.
		expected string // If 'query' is equal to the blank string, this is ignored.
	}{
		{
			name:     "singe point with timestamp",
			write:    `{"database" : "mydb", "retentionPolicy" : "myrp", "points": [{"name": "cpu", "timestamp": "2015-02-28T01:03:36.703820946Z", "tags": {"host": "server01"}, "fields": {"value": 100}}]}`,
			query:    `SELECT * FROM "mydb"."myrp".cpu`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		{
			name:     "singe point, select with now()",
			query:    `SELECT * FROM "mydb"."myrp".cpu WHERE time < now()`,
			expected: `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
	}

	for _, tt := range tests {
		if tt.write != "" {
			write(t, nodes[0], tt.write)
		}

		if tt.query != "" {
			got, ok := query(t, nodes, tt.query, tt.expected)
			if !ok {
				t.Errorf("Test '%s' failed, expected: %s, got: %s", tt.name, tt.expected, got)
			}
		}
	}
}

func TestSingleServer(t *testing.T) {
	testName := "single server integration"
	if testing.Short() {
		t.Skip(fmt.Sprintf("skipping '%s'", testName))
	}
	dir := os.TempDir()
	defer func() {
		os.RemoveAll(dir)
	}()

	nodes := createCombinedNodeCluster(t, testName, dir, 1, 8090)

	createDatabase(t, testName, nodes, "mydb")
	createRetentionPolicy(t, testName, nodes, "mydb", "myrp")

	runTests_Errors(t, nodes)
	runTestsData(t, nodes)
}

func Test3NodeServer(t *testing.T) {
	t.Skip()
	testName := "3-node server integration"
	if testing.Short() {
		t.Skip(fmt.Sprintf("skipping '%s'", testName))
	}
	dir := os.TempDir()
	defer func() {
		os.RemoveAll(dir)
	}()

	nodes := createCombinedNodeCluster(t, testName, dir, 3, 8190)

	createDatabase(t, testName, nodes, "mydb")
	createRetentionPolicy(t, testName, nodes, "mydb", "myrp")
	runTests_Errors(t, nodes)
	runTestsData(t, nodes)
}
