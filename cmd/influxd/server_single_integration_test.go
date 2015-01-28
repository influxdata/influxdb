package main_test

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
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

	var (
		join    = ""
		version = "x.x"
	)

	//tmpBrokerDir, _ := ioutil.TempDir("", "")
	//tmpDataDir, _ := ioutil.TempDir("", "")
	//defer func() {
	//os.Remove(tmpBrokerDir)
	//os.Remove(tmpDataDir)
	//}()

	c := main.NewConfig()
	//c.Broker.Dir = tmpBrokerDir
	//c.Data.Dir = tmpDataDir

	now := time.Now()
	var spinupTime time.Duration

	main.Run(c, join, version)

	ready := make(chan bool, 1)
	go func() {
		for {
			resp, err := http.Get(c.BrokerURL().String() + "/ping")
			if err != nil {
				t.Fatalf("failed to spin up server: %s", err)
			}
			if resp.StatusCode != http.StatusNoContent {
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

	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		t.Logf("json: %s", string(b))
		t.Fatalf("Create database failed.  Unexpected status code.  expected: %d, actual %d", http.StatusOK, resp.StatusCode)
	}

	var results influxdb.Results
	err = json.NewDecoder(resp.Body).Decode(&results)
	if err != nil {
		t.Fatalf("Couldn't decode results: %v", err)
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

	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		t.Logf("json: %s", string(b))
		t.Fatalf("show databases failed.  Unexpected status code.  expected: %d, actual %d", http.StatusOK, resp.StatusCode)
	}

	err = json.NewDecoder(resp.Body).Decode(&results)
	if err != nil {
		t.Fatalf("Couldn't decode results: %v", err)
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
	t.Log("Creating retention policy")

	u = urlFor(c.BrokerURL(), "query", url.Values{"q": []string{"CREATE RETENTION POLICY bar ON foo DURATION 1h REPLICATION 1 DEFAULT"}})

	resp, err = client.Get(u.String())
	if err != nil {
		t.Fatalf("Couldn't create retention policy: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		t.Logf("json: %s", string(b))
		t.Fatalf("Create retention policy failed.  Unexpected status code.  expected: %d, actual %d", http.StatusOK, resp.StatusCode)
	}

	err = json.NewDecoder(resp.Body).Decode(&results)
	if err != nil {
		t.Fatalf("Couldn't decode results: %v", err)
	}
	if len(results.Results) != 1 {
		t.Fatalf("Create retention policy failed.  Unexpected results length.  expected: %d, actual %d", 1, len(results.Results))
	}

	// TODO corylanou: Query the retention policy exists

	// Write Data
	t.Log("Write data")

	u = urlFor(c.BrokerURL(), "write", url.Values{})

	buf := bytes.NewReader([]byte(`{"database" : "foo", "retentionPolicy" : "bar", "points": [{"name": "cpu", "tags": {"host": "server01"},"timestamp": "2015-01-26T22:01:11.703Z","values": {"value": 100}}]}`))
	resp, err = client.Post(u.String(), "application/json", buf)
	if err != nil {
		t.Fatalf("Couldn't write data: %s", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("Write to database failed.  Unexpected status code.  expected: %d, actual %d", http.StatusCreated, resp.StatusCode)
	}

	// Query the data exists
	t.Log("Query data")
	u = urlFor(c.BrokerURL(), "query", url.Values{"q": []string{`select mean(value) from "foo"."bar".cpu`}, "db": []string{"foo"}})

	resp, err = client.Get(u.String())
	if err != nil {
		t.Fatalf("Couldn't query databases: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		t.Logf("json: %s", string(b))
		t.Fatalf("query databases failed.  Unexpected status code.  expected: %d, actual %d", http.StatusOK, resp.StatusCode)
	}

	err = json.NewDecoder(resp.Body).Decode(&results)
	if err != nil {
		t.Fatalf("Couldn't decode results: %v", err)
	}
	if len(results.Results) != 1 {
		t.Fatalf("query databases failed.  Unexpected results length.  expected: %d, actual %d", 1, len(results.Results))
	}

	rows = results.Results[0].Rows
	if len(rows) != 1 {
		t.Fatalf("query databases failed.  Unexpected rows length.  expected: %d, actual %d", 1, len(rows))
	}
	row = rows[0]
	t.Fatalf("query databases failed.  Unexpected row.  expected: %+v, actual %+v", expectedRow, row)
	expectedRow = &influxql.Row{
		Columns: []string{"Name"},
	}
	if !reflect.DeepEqual(row, expectedRow) {
		t.Fatalf("query databases failed.  Unexpected row.  expected: %+v, actual %+v", expectedRow, row)
	}
	if row.Columns[0] != "Name" {
		t.Fatalf("show databases failed.  Unexpected row.Columns[0].  expected: %s, actual %s", "Name", row.Columns[0])
	}

}

func urlFor(u *url.URL, path string, params url.Values) *url.URL {
	u.Path = path
	u.RawQuery = params.Encode()
	return u
}
