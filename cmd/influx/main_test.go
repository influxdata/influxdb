package main_test

import (
	"testing"

	main "github.com/influxdb/influxdb/cmd/influx"
)

func TestParseCommand_CommandsExist(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
