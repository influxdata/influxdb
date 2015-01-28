package main_test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/influxql"

	main "github.com/influxdb/influxdb/cmd/influxd"
)

func TestNewServer(t *testing.T) {
	// Uncomment this to see the test fail when running for a second time in a row
	t.Skip()
	tmpBrokerDir, err := ioutil.TempDir("", ".influxdb_broker")
	if err != nil {
		t.Fatalf("Couldn't create temporary broker directory: %s", err)
	}
	defer func() {
		os.Remove(tmpBrokerDir)
	}()
	tmpDataDir, err := ioutil.TempDir("", ".influxdb_data")
	if err != nil {
		t.Fatalf("Couldn't create temporary data directory: %s", err)
	}
	defer func() {
		os.Remove(tmpDataDir)
	}()
	var (
		join    = ""
		version = "x.x"
	)

	c := main.NewConfig()
	c.Broker.Dir = tmpBrokerDir
	c.Data.Dir = tmpDataDir

	now := time.Now()
	var spinupTime time.Duration

	main.Run(nil, join, version)

	// In the interst of getting basic integration tests going, we are sleeping for
	// now until we have a "ready" state in the system
	//time.Sleep(4 * time.Second)

	ready := make(chan bool, 1)
	go func() {
		for {
			resp, err := http.Get(c.BrokerURL().String() + "/ping")
			if err != nil {
				t.Fatalf("failed to spin up server: %s", err)
			}
			if resp.StatusCode != http.StatusNoContent {
				t.Log(resp.StatusCode)
				time.Sleep(2 * time.Millisecond)
			} else {
				ready <- true
				break
			}
		}
	}()

	// wait for the server to spin up
	func() {
		for {
			select {
			case <-ready:
				spinupTime = time.Since(now)
				t.Logf("Spinup time of server was %v\n", spinupTime)
				return
			case <-time.After(3 * time.Second):
				if spinupTime == 0 {
					ellapsed := time.Since(now)
					t.Fatalf("server failed to spin up in time %v", ellapsed)
				}
			}
		}
	}()

	// Createa a database
	t.Log("Creating database")

	u := urlFor(c.BrokerURL(), "query", url.Values{"q": []string{"CREATE DATABASE foo"}})
	client := http.Client{Timeout: 100 * time.Millisecond}

	resp, err := client.Get(u.String())
	if err != nil {
		t.Fatalf("Couldn't create database: %s", err)
	}
	defer resp.Body.Close()

	var results influxdb.Results
	err = json.NewDecoder(resp.Body).Decode(&results)
	if err != nil {
		t.Fatalf("Couldn't decode results: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Create database failed.  Unexpected status code.  expected: %d, actual %d", http.StatusOK, resp.StatusCode)
	}
	if len(results.Results) != 1 {
		t.Fatalf("Create database failed.  Unexpected results length.  expected: %d, actual %d", 1, len(results.Results))
	}

	// Query the database exists
	u = urlFor(c.BrokerURL(), "query", url.Values{"q": []string{"SHOW DATABASES"}})

	resp, err = client.Get(u.String())
	if err != nil {
		t.Fatalf("Couldn't query databases: %s", err)
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&results)
	if err != nil {
		t.Fatalf("Couldn't decode results: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("show databases failed.  Unexpected status code.  expected: %d, actual %d", http.StatusOK, resp.StatusCode)
	}
	if len(results.Results) != 1 {
		t.Fatalf("show databases failed.  Unexpected results length.  expected: %d, actual %d", 1, len(results.Results))
	}

	rows := results.Results[0].Rows
	if len(rows) != 1 {
		t.Fatalf("show databases failed.  Unexpected rows length.  expected: %d, actual %d", 1, len(rows))
	}
	row := rows[0]
	expectedRow := &influxql.Row{
		Columns: []string{"Name"},
		Values:  [][]interface{}{{"foo"}},
	}
	if !reflect.DeepEqual(row, expectedRow) {
		t.Fatalf("show databases failed.  Unexpected row.  expected: %+v, actual %+v", expectedRow, row)
	}
	if row.Columns[0] != "Name" {
		t.Fatalf("show databases failed.  Unexpected row.Columns[0].  expected: %s, actual %s", "Name", row.Columns[0])
	}

	// Create a retention policy

	// Query the retention polocy exists

	// Write Data

	// Query the data exists

}

func urlFor(u *url.URL, path string, params url.Values) *url.URL {
	u.Path = path
	u.RawQuery = params.Encode()
	return u
}
