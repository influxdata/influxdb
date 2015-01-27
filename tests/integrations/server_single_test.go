package integrations_test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/influxd"
)

func TestNewServer(t *testing.T) {
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

	c := influxd.NewConfig()
	c.Broker.Dir = tmpBrokerDir
	c.Data.Dir = tmpDataDir

	now := time.Now()
	var spinupTime time.Duration

	influxd.Run(nil, join, version)

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
	u := c.BrokerURL()
	u.Path = "query"

	v := u.Query()
	v.Set("q", "CREATE DATABASE foo")
	u.RawQuery = v.Encode()

	resp, err := http.Get(u.String())
	if err != nil {
		t.Fatalf("Couldn't create database: %s", err)
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	t.Fatalf("%s", string(body))

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

	// Create a retention policy

	// Query the retention polocy exists

	// Write Data

	// Query the data exists

}
