package raft

import (
	"testing"
	"io/ioutil"
	"os"
	"reflect"
)

//------------------------------------------------------------------------------
//
// Setup
//
//------------------------------------------------------------------------------

func getLogPath() string {
	f, _ := ioutil.TempFile("", "raft-log-")
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

func setupLog(content string) string {
	f, _ := ioutil.TempFile("", "raft-log-")
	f.Write([]byte(content))
	f.Close()
	return f.Name()
}

type TestCommand1 struct {
	Val string `json:"val"`
	I int `json:"i"`
}

func (c TestCommand1) Name() string {
	return "cmd_1"
}

type TestCommand2 struct {
	X int `json:"x"`
}

func (c TestCommand2) Name() string {
	return "cmd_2"
}

//------------------------------------------------------------------------------
//
// Tests
//
//------------------------------------------------------------------------------

// Ensure that we can append to a new log.
func TestLogNewLog(t *testing.T) {
	path := getLogPath()
	log := NewLog()
	log.AddCommandType(&TestCommand1{})
	log.AddCommandType(&TestCommand2{})
	if err := log.Open(path); err != nil {
		t.Fatalf("Unable to open log: %v", err)
	}
	defer log.Close()
	defer os.Remove(path)
	
	if err := log.Append(NewLogEntry(log, 1, 1, &TestCommand1{"foo", 20})); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}
	if err := log.Append(NewLogEntry(log, 2, 1, &TestCommand2{100})); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}
	if err := log.Append(NewLogEntry(log, 3, 2, &TestCommand1{"bar", 0})); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}
	
	// Partial commit.
	if err := log.SetCommitIndex(2); err != nil {
		t.Fatalf("Unable to partially commit: %v", err)
	}
	expected := 
		`cf4aab23 0000000000000001 0000000000000001 cmd_1 {"val":"foo","i":20}`+"\n" +
		`4c08d91f 0000000000000002 0000000000000001 cmd_2 {"x":100}`+"\n"
	actual, _ := ioutil.ReadFile(path)
	if string(actual) != expected {
		t.Fatalf("Unexpected buffer:\nexp:\n%s\ngot:\n%s", expected, string(actual))
	}

	// Full commit.
	if err := log.SetCommitIndex(3); err != nil {
		t.Fatalf("Unable to commit: %v", err)
	}
	expected = 
		`cf4aab23 0000000000000001 0000000000000001 cmd_1 {"val":"foo","i":20}`+"\n" +
		`4c08d91f 0000000000000002 0000000000000001 cmd_2 {"x":100}`+"\n" +
		`6ac5807c 0000000000000003 0000000000000002 cmd_1 {"val":"bar","i":0}`+"\n"
	actual, _ = ioutil.ReadFile(path)
	if string(actual) != expected {
		t.Fatalf("Unexpected buffer:\nexp:\n%s\ngot:\n%s", expected, string(actual))
	}
}

// Ensure that we can decode and encode to an existing log.
func TestLogExistingLog(t *testing.T) {
	path := setupLog(
		`cf4aab23 0000000000000001 0000000000000001 cmd_1 {"val":"foo","i":20}`+"\n" +
		`4c08d91f 0000000000000002 0000000000000001 cmd_2 {"x":100}`+"\n" +
		`6ac5807c 0000000000000003 0000000000000002 cmd_1 {"val":"bar","i":0}`+"\n")
	log := NewLog()
	log.AddCommandType(&TestCommand1{})
	log.AddCommandType(&TestCommand2{})
	if err := log.Open(path); err != nil {
		t.Fatalf("Unable to open log: %v", err)
	}
	defer log.Close()
	defer os.Remove(path)

	// Validate existing log entries.
	if len(log.entries) != 3 {
		t.Fatalf("Expected 3 entries, got %d", len(log.entries))
	}
	if !reflect.DeepEqual(log.entries[0], NewLogEntry(log, 1, 1, &TestCommand1{"foo", 20})) {
		t.Fatalf("Unexpected entry[0]: %v", log.entries[0])
	}
	if !reflect.DeepEqual(log.entries[1], NewLogEntry(log, 2, 1, &TestCommand2{100})) {
		t.Fatalf("Unexpected entry[1]: %v", log.entries[1])
	}
	if !reflect.DeepEqual(log.entries[2], NewLogEntry(log, 3, 2, &TestCommand1{"bar", 0})) {
		t.Fatalf("Unexpected entry[2]: %v", log.entries[2])
	}
}

// Ensure that we can recover from an incomplete/corrupt log and continue logging.
func TestLogRecovery(t *testing.T) {
	path := setupLog(
		`cf4aab23 0000000000000001 0000000000000001 cmd_1 {"val":"foo","i":20}`+"\n" +
		`4c08d91f 0000000000000002 0000000000000001 cmd_2 {"x":100}`+"\n" +
		`6ac5807c 0000000000000003 00000000000`)
	log := NewLog()
	log.AddCommandType(&TestCommand1{})
	log.AddCommandType(&TestCommand2{})
	if err := log.Open(path); err != nil {
		t.Fatalf("Unable to open log: %v", err)
	}
	defer log.Close()
	defer os.Remove(path)

	if err := log.Append(NewLogEntry(log, 3, 2, &TestCommand1{"bat", -5})); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}

	// Validate existing log entries.
	if len(log.entries) != 3 {
		t.Fatalf("Expected 2 entries, got %d", len(log.entries))
	}
	if !reflect.DeepEqual(log.entries[0], NewLogEntry(log, 1, 1, &TestCommand1{"foo", 20})) {
		t.Fatalf("Unexpected entry[0]: %v", log.entries[0])
	}
	if !reflect.DeepEqual(log.entries[1], NewLogEntry(log, 2, 1, &TestCommand2{100})) {
		t.Fatalf("Unexpected entry[1]: %v", log.entries[1])
	}
	if !reflect.DeepEqual(log.entries[2], NewLogEntry(log, 3, 2, &TestCommand1{"bat", -5})) {
		t.Fatalf("Unexpected entry[2]: %v", log.entries[2])
	}

	// Validate precommit log contents.
	expected :=
		`cf4aab23 0000000000000001 0000000000000001 cmd_1 {"val":"foo","i":20}`+"\n" +
		`4c08d91f 0000000000000002 0000000000000001 cmd_2 {"x":100}`+"\n"
	actual, _ := ioutil.ReadFile(path)
	if string(actual) != expected {
		t.Fatalf("Unexpected buffer:\nexp:\n%s\ngot:\n%s", expected, string(actual))
	}

	// Validate committed log contents.
	if err := log.SetCommitIndex(3); err != nil {
		t.Fatalf("Unable to partially commit: %v", err)
	}
	expected =
		`cf4aab23 0000000000000001 0000000000000001 cmd_1 {"val":"foo","i":20}`+"\n" +
		`4c08d91f 0000000000000002 0000000000000001 cmd_2 {"x":100}`+"\n" +
		`3f3f884c 0000000000000003 0000000000000002 cmd_1 {"val":"bat","i":-5}`+"\n"
	actual, _ = ioutil.ReadFile(path)
	if string(actual) != expected {
		t.Fatalf("Unexpected buffer:\nexp:\n%s\ngot:\n%s", expected, string(actual))
	}
}
