package main_test

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdb/influxdb/client"
	main "github.com/influxdb/influxdb/cmd/influx"
	influxd "github.com/influxdb/influxdb/cmd/influxd"
)

func TestParseCommand_CommandsExist(t *testing.T) {
	c := main.CommandLine{}
	tests := []struct {
		cmd string
	}{
		{cmd: "gopher"},
		{cmd: "connect"},
		{cmd: "help"},
		{cmd: "pretty"},
		{cmd: "use"},
		{cmd: ""}, // test that a blank command just returns
	}
	for _, test := range tests {
		if !c.ParseCommand(test.cmd) {
			t.Fatalf(`Command failed for %q.`, test.cmd)
		}
	}
}

func TestParseCommand_TogglePretty(t *testing.T) {
	c := main.CommandLine{}
	if c.Pretty {
		t.Fatalf(`Pretty should be false.`)
	}
	c.ParseCommand("pretty")
	if !c.Pretty {
		t.Fatalf(`Pretty should be true.`)
	}
	c.ParseCommand("pretty")
	if c.Pretty {
		t.Fatalf(`Pretty should be false.`)
	}
}

func TestParseCommand_Exit(t *testing.T) {
	c := main.CommandLine{}
	tests := []struct {
		cmd string
	}{
		{cmd: "exit"},
		{cmd: " exit"},
		{cmd: "exit "},
		{cmd: "Exit "},
	}

	for _, test := range tests {
		if c.ParseCommand(test.cmd) {
			t.Fatalf(`Command "exit" failed for %q.`, test.cmd)
		}
	}
}

func TestParseCommand_Use(t *testing.T) {
	c := main.CommandLine{}
	tests := []struct {
		cmd string
	}{
		{cmd: "use db"},
		{cmd: " use db"},
		{cmd: "use db "},
		{cmd: "Use db"},
	}

	for _, test := range tests {
		if !c.ParseCommand(test.cmd) {
			t.Fatalf(`Command "use" failed for %q.`, test.cmd)
		}
	}
}

func TestQuery_NoAuth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestQuery_NoAuth")
	}

	// Create root path to server.
	// Defer it to clean up for successful tests
	path := tempfile()
	defer os.Remove(path)

	config, _ := influxd.NewTestConfig()

	// Start server.
	node, err := newNode(config, path)
	if err != nil {
		t.Fatal(err)
	}

	c := main.CommandLine{
		Format: "column",
	}

	u := url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%d", "localhost", 8086),
	}
	cl, err := client.NewClient(
		client.Config{
			URL: u,
		})

	if err != nil {
		t.Fatal(err)
	}

	c.Client = cl

	ok := c.ParseCommand("CREATE USER admin WITH PASSWORD 'password'")
	if !ok {
		t.Fatal("Failed to create user")
	}
	node.Close()
}

func TestQuery_Auth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestQuery_Auth")
	}

	// Create root path to server.
	// Defer it to clean up for successful tests
	path := tempfile()
	defer os.Remove(path)

	// Create the cli
	c := main.CommandLine{Format: "column"}
	cl, err := client.NewClient(
		client.Config{
			URL: url.URL{
				Scheme: "http",
				Host:   fmt.Sprintf("%s:%d", "localhost", 8086),
			}})
	if err != nil {
		t.Fatal(err)
	}

	c.Client = cl

	// spin up a server
	config, _ := influxd.NewTestConfig()
	config.Authentication.Enabled = true
	node, err := newNode(config, path)
	if err != nil {
		t.Fatal(err)
	}

	// Check to make sure we can't do anything
	err = c.ExecuteQuery("CREATE USER admin WITH PASSWORD 'password'")
	if err == nil {
		t.Fatal("Should have failed to create user")
	}

	// spin down server
	node.Close()

	// disable auth and spin back up
	config.Authentication.Enabled = false
	node, err = newNode(config, path)
	if err != nil {
		t.Fatal(err)
	}

	// create the user
	err = c.ExecuteQuery("CREATE USER admin WITH PASSWORD 'password'")
	if err != nil {
		t.Fatalf("Should have created user: %s\n", err)
	}
	// Make cluster admin
	err = c.ExecuteQuery("GRANT ALL PRIVILEGES TO admin")
	if err != nil {
		t.Fatalf("Should have made cluster admin: %s\n", err)
	}

	// spin down again
	node.Close()

	// enable auth, spin back up
	config.Authentication.Enabled = true
	node, err = newNode(config, path)
	if err != nil {
		t.Fatal(err)
	}

	// Check to make sure we still can't do anything
	err = c.ExecuteQuery("CREATE USER admin WITH PASSWORD 'password'")
	if err == nil {
		t.Fatal("Should have failed to create user")
	}

	c.SetAuth("auth admin password")

	// Check to make sure we can't do anything
	err = c.ExecuteQuery("CREATE DATABASE foo")
	if err != nil {
		t.Fatalf("Failed to create database: %s\n", err)
	}
	node.Close()
}

// tempfile returns a temporary path.
func tempfile() string {
	f, _ := ioutil.TempFile("", "influxdb-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}

func newNode(config *influxd.Config, path string) (*influxd.Node, error) {
	config.Broker.Dir = filepath.Join(path, "broker")
	config.Data.Dir = filepath.Join(path, "data")

	// Start server.
	cmd := influxd.NewRunCommand()

	node := cmd.Open(config, "")
	if node.Broker == nil {
		return nil, fmt.Errorf("cannot run broker")
	} else if node.DataNode == nil {
		return nil, fmt.Errorf("cannot run server")
	}
	return node, nil
}
