package main_test

import (
	"strconv"
	"strings"
	"testing"

	main "github.com/influxdb/influxdb/cmd/influx"
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

func TestDisplayURLWithUsernamePassword(t *testing.T) {
	c := main.CommandLine{}
	c.Host = "some-host"
	c.Port = 1234
	c.Username = "someuser"
	c.Password = "somepass"

	output := c.DisplayURL()

	if !strings.Contains(output, c.Username) {
		t.Fatalf(`DisplayURL() [%s] should show the username`, output)
	}

	if !strings.Contains(output, c.Host) {
		t.Fatalf(`DisplayURL() [%s] should show the host`, output)
	}

	if !strings.Contains(output, strconv.Itoa(c.Port)) {
		t.Fatalf(`DisplayURL() [%s] should show the port`, output)
	}

	if strings.Contains(output, c.Password) {
		t.Fatalf(`DisplayURL() [%s] should not reveal the password`, output)
	}
}

func TestDisplayURLWithOutUsernamePassword(t *testing.T) {
	c := main.CommandLine{}
	c.Host = "some-host"
	c.Port = 1234

	output := c.DisplayURL()

	if strings.Contains(output, "@") {
		t.Fatalf(`DisplayURL() [%s] should not include a '@' without a username/password`, output)
	}

	if !strings.Contains(output, c.Host) {
		t.Fatalf(`DisplayURL() [%s] should show the host`, output)
	}

	if !strings.Contains(output, strconv.Itoa(c.Port)) {
		t.Fatalf(`DisplayUrl() [%s] should show the port`, output)
	}
}
