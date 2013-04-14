package raft

import (
	"testing"
	"io/ioutil"
	"os"
)

//------------------------------------------------------------------------------
//
// Setup
//
//------------------------------------------------------------------------------

func setupLogFile() string {
	f, _ := ioutil.TempFile("", "raft-log-")
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

type TestCommand1 struct {
	Val string `json:"val"`
	I int `json:"i"`
}

func (c TestCommand1) Name() string {
	return "cmd_1"
}

//------------------------------------------------------------------------------
//
// Tests
//
//------------------------------------------------------------------------------

// Ensure that we can encode log entries.
func TestLogNewLog(t *testing.T) {
	path := setupLogFile()
	log := NewLog()
	log.AddCommandType(&TestCommand1{})
	if err := log.Open(path); err != nil {
		t.Fatalf("Unable to open log: %v", err)
	}
	defer log.Close()
	defer os.Remove(path)
	
	err := log.Append(NewLogEntry(log, 1, 2, &TestCommand1{"foo", 20}))
	if err != nil {
		t.Fatalf("Unable to append: %v", err)
	}
	
	expected := `a9f602d5 00000001 00000002 cmd_1 {"val":"foo","i":20}`+"\n"
	actual, _ := ioutil.ReadFile(path)
	if string(actual) != expected {
		t.Fatalf("Unexpected buffer:\nexp: %s\ngot: %s", expected, string(actual))
	}
}
