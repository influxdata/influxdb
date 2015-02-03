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

func TestNewServer(t *testing.T) {
	var (
		join    = ""
		version = "x.x"
	)

	tmpDir := os.TempDir()
	tmpBrokerDir := filepath.Join(tmpDir, "broker")
	tmpDataDir := filepath.Join(tmpDir, "data")
	t.Logf("Using tmp directorie %q for broker\n", tmpBrokerDir)
	t.Logf("Using tmp directorie %q for data\n", tmpDataDir)

	c := main.NewConfig()
	c.Broker.Dir = tmpBrokerDir
	c.Data.Dir = tmpDataDir

	now := time.Now()

	s := main.Run(c, join, version, os.Stderr)

	defer func() {
		s.Close()
		t.Log("Shutting down server and cleaning up tmp directories")
		err := os.RemoveAll(tmpBrokerDir)
		if err != nil {
			t.Logf("Failed to clean up %q: %s\n", tmpBrokerDir, err)
		}
		err = os.RemoveAll(tmpDataDir)
		if err != nil {
			t.Logf("Failed to clean up %q: %s\n", tmpDataDir, err)
		}
	}()

	// Create a database
	t.Log("Creating database")

	u := urlFor(c.BrokerURL(), "query", url.Values{"q": []string{"CREATE DATABASE foo"}})
	httpClient := http.Client{Timeout: 100 * time.Millisecond}

	resp, err := httpClient.Get(u.String())
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
	u = urlFor(c.BrokerURL(), "query", url.Values{"q": []string{"SHOW DATABASES"}})

	resp, err = httpClient.Get(u.String())
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

	u = urlFor(c.BrokerURL(), "query", url.Values{"q": []string{"CREATE RETENTION POLICY bar ON foo DURATION 1h REPLICATION 1 DEFAULT"}})

	resp, err = httpClient.Get(u.String())
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

	u = urlFor(c.BrokerURL(), "write", url.Values{})

	buf := []byte(fmt.Sprintf(`{"database" : "foo", "retentionPolicy" : "bar", "points": [{"name": "cpu", "tags": {"host": "server01"},"timestamp": %d, "precision":"n","values": {"value": 100}}]}`, now.UnixNano()))
	t.Logf("Writing raw data: %s", string(buf))

	resp, err = httpClient.Post(u.String(), "application/json", bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("Couldn't write data: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Write to database failed.  Unexpected status code.  expected: %d, actual %d", http.StatusOK, resp.StatusCode)
	}

	// Need some time for server to get consensus and write data
	// TODO corylanou query the status endpoint for the server and wait for the index to update to know the write was applied
	time.Sleep(100 * time.Millisecond)

	// Query the data exists
	t.Log("Query data")
	u = urlFor(c.BrokerURL(), "query", url.Values{"q": []string{`select value from "foo"."bar".cpu`}, "db": []string{"foo"}})

	resp, err = httpClient.Get(u.String())
	if err != nil {
		t.Fatalf("Couldn't query databases: %s", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Coulnd't read body of response: %s", err)
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

	strNow := strconv.FormatInt(now.UnixNano(), 10)
	expectedResults = client.Results{
		Results: []client.Result{
			{Rows: []influxql.Row{
				{
					Name:    "cpu",
					Columns: []string{"time", "value"},
					Values: [][]interface{}{
						[]interface{}{json.Number(strNow), json.Number("100")},
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

func urlFor(u *url.URL, path string, params url.Values) *url.URL {
	u.Path = path
	u.RawQuery = params.Encode()
	return u
}
