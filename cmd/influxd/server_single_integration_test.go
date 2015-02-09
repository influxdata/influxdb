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
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/influxql"

	main "github.com/influxdb/influxdb/cmd/influxd"
)

// createCombinedNodeCluster creates a cluster of nServers nodes, each of which
// runs as both a Broker and Data node. If any part cluster creation fails,
// the testing is marked as failed.
func createCombinedNodeCluster(t *testing.T, testName string, nNodes, basePort int) {
	t.Logf("Creating cluster of %d nodes for test %s", nNodes, testName)
	if nNodes < 1 {
		t.Fatalf("Test %s: asked to create nonsense cluster", testName)
	}

	tmpDir := os.TempDir()
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

	s := main.Run(c, "", "x.x", os.Stderr)
	if s == nil {
		t.Fatalf("Test %s: failed to create leader node on port %d", testName, basePort)
	}

	// Create subsequent nodes, which join to first node.
	for i := 1; i < nNodes; i++ {
		nextPort := basePort + i
		c.Broker.Dir = filepath.Join(tmpBrokerDir, strconv.Itoa(nextPort))
		c.Data.Dir = filepath.Join(tmpDataDir, strconv.Itoa(nextPort))
		c.Broker.Port = nextPort
		c.Data.Port = nextPort

		s := main.Run(c, "http://localhost:"+strconv.Itoa(basePort), "x.x", os.Stderr)
		if s == nil {
			t.Fatalf("Test %s: failed to create following node on port %d", testName, basePort)
		}

		time.Sleep(3 * time.Second)
	}
}

// simpleWriteAndQuery creates a simple database, retention policy, and replicates
// the data across all nodes. It then ensures a series of writes and queries are OK.
func simpleWriteAndQuery(t *testing.T, testname string, serverURL *url.URL, nNodes int) {
	now := time.Now().UTC()

	// Create a database
	t.Log("Creating database")
	u := urlFor(serverURL, "query", url.Values{"q": []string{"CREATE DATABASE foo"}})
	resp, err := http.Get(u.String())
	if err != nil {
		t.Fatalf("Couldn't create database: %s", err)
	}
	defer resp.Body.Close()

	var results client.Results
	err = json.NewDecoder(resp.Body).Decode(&results)
	if err != nil {
		t.Fatalf("Couldn't decode results: %v", err)
	}

	if results.Error() != nil {
		t.Logf("results.Error(): %q", results.Error().Error())
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Create database failed.  Unexpected status code.  expected: %d, actual %d", http.StatusOK, resp.StatusCode)
	}

	if len(results.Results) != 1 {
		t.Fatalf("Create database failed.  Unexpected results length.  expected: %d, actual %d", 1, len(results.Results))
	}

	// Query the database exists
	u = urlFor(serverURL, "query", url.Values{"q": []string{"SHOW DATABASES"}})
	resp, err = http.Get(u.String())
	if err != nil {
		t.Fatalf("Couldn't query databases: %s", err)
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&results)
	if err != nil {
		t.Fatalf("Couldn't decode results: %v", err)
	}

	if results.Error() != nil {
		t.Logf("results.Error(): %q", results.Error().Error())
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("show databases failed.  Unexpected status code.  expected: %d, actual %d", http.StatusOK, resp.StatusCode)
	}

	expectedResults := client.Results{
		Results: []client.Result{
			{Rows: []influxql.Row{
				influxql.Row{
					Columns: []string{"name"},
					Values:  [][]interface{}{{"foo"}},
				},
			}},
		},
	}
	if !reflect.DeepEqual(results, expectedResults) {
		t.Fatalf("show databases failed.  Unexpected results.  expected: %+v, actual %+v", expectedResults, results)
	}

	// Create a retention policy
	t.Log("Creating retention policy")
	replication := fmt.Sprintf("CREATE RETENTION POLICY bar ON foo DURATION 1h REPLICATION %d DEFAULT", nNodes)
	u = urlFor(serverURL, "query", url.Values{"q": []string{replication}})
	resp, err = http.Get(u.String())
	if err != nil {
		t.Fatalf("Couldn't create retention policy: %s", err)
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&results)
	if err != nil {
		t.Fatalf("Couldn't decode results: %v", err)
	}

	if results.Error() != nil {
		t.Logf("results.Error(): %q", results.Error().Error())
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Create retention policy failed.  Unexpected status code.  expected: %d, actual %d", http.StatusOK, resp.StatusCode)
	}

	if len(results.Results) != 1 {
		t.Fatalf("Create retention policy failed.  Unexpected results length.  expected: %d, actual %d", 1, len(results.Results))
	}

	// TODO corylanou: Query the retention policy exists

	// Write Data
	t.Log("Write data")
	u = urlFor(serverURL, "write", url.Values{})

	buf := []byte(fmt.Sprintf(`{"database" : "foo", "retentionPolicy" : "bar", "points": [{"name": "cpu", "tags": {"host": "server01"},"timestamp": %d, "precision":"n","values": {"value": 100}}]}`, now.UnixNano()))
	t.Logf("Writing raw data: %s", string(buf))
	resp, err = http.Post(u.String(), "application/json", bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("Couldn't write data: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Write to database failed.  Unexpected status code.  expected: %d, actual %d", http.StatusOK, resp.StatusCode)
	}

	// Need some time for server to get consensus and write data
	// TODO corylanou query the status endpoint for the server and wait for the index to update to know the write was applied
	time.Sleep(time.Duration(nNodes) * time.Second)

	// Query the data exists
	t.Log("Query data")
	u = urlFor(serverURL, "query", url.Values{"q": []string{`select value from "foo"."bar".cpu`}, "db": []string{"foo"}})
	resp, err = http.Get(u.String())
	if err != nil {
		t.Fatalf("Couldn't query databases: %s", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Couldn't read body of response: %s", err)
	}
	t.Logf("resp.Body: %s\n", string(body))

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	err = dec.Decode(&results)
	if err != nil {
		t.Fatalf("Couldn't decode results: %v", err)
	}

	if results.Error() != nil {
		t.Logf("results.Error(): %q", results.Error().Error())
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("query databases failed.  Unexpected status code.  expected: %d, actual %d", http.StatusOK, resp.StatusCode)
	}

	expectedResults = client.Results{
		Results: []client.Result{
			{Rows: []influxql.Row{
				{
					Name:    "cpu",
					Columns: []string{"time", "value"},
					Values: [][]interface{}{
						[]interface{}{now.Format(time.RFC3339Nano), json.Number("100")},
					},
				}}},
		},
	}

	if !reflect.DeepEqual(results, expectedResults) {
		t.Logf("Expected:\n")
		t.Logf("%#v\n", expectedResults)
		t.Logf("Actual:\n")
		t.Logf("%#v\n", results)
		t.Fatalf("query databases failed.  Unexpected results.")
	}
}

func Test_ServerSingleIntegration(t *testing.T) {
	nNodes := 1
	createCombinedNodeCluster(t, "single node", nNodes, 8090)

	serverURL := &url.URL{
		Scheme: "http",
		Host:   "localhost:8090",
	}
	simpleWriteAndQuery(t, "single node", serverURL, nNodes)
}

func Test_Server3NodeIntegration(t *testing.T) {
	nNodes := 3
	createCombinedNodeCluster(t, "3 node", nNodes, 8090)

	serverURL := &url.URL{
		Scheme: "http",
		Host:   "localhost:8090",
	}
	simpleWriteAndQuery(t, "3 node", serverURL, nNodes)
}

func Test_Server5NodeIntegration(t *testing.T) {
	nNodes := 5
	createCombinedNodeCluster(t, "5 node", nNodes, 8090)

	serverURL := &url.URL{
		Scheme: "http",
		Host:   "localhost:8090",
	}
	simpleWriteAndQuery(t, "5 node", serverURL, nNodes)
}

func urlFor(u *url.URL, path string, params url.Values) *url.URL {
	u.Path = path
	u.RawQuery = params.Encode()
	return u
}
