package raft

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

//------------------------------------------------------------------------------
//
// Tests
//
//------------------------------------------------------------------------------

//--------------------------------------
// Append
//--------------------------------------

// Ensure that we can append to a new log.
func TestLogNewLog(t *testing.T) {
	path := getLogPath()
	log := NewLog()
	log.ApplyFunc = func(c Command) (interface{}, error) {
		return nil, nil
	}
	if err := log.Open(path); err != nil {
		t.Fatalf("Unable to open log: %v", err)
	}
	defer log.Close()
	defer os.Remove(path)

	if err := log.AppendEntry(NewLogEntry(log, 1, 1, &TestCommand1{"foo", 20})); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}
	if err := log.AppendEntry(NewLogEntry(log, 2, 1, &TestCommand2{100})); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}
	if err := log.AppendEntry(NewLogEntry(log, 3, 2, &TestCommand1{"bar", 0})); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}

	// Partial commit.
	if err := log.SetCommitIndex(2); err != nil {
		t.Fatalf("Unable to partially commit: %v", err)
	}
	expected := `cf4aab23 0000000000000001 0000000000000001 cmd_1 {"val":"foo","i":20}` + "\n" +
		`4c08d91f 0000000000000002 0000000000000001 cmd_2 {"x":100}` + "\n"
	actual, _ := ioutil.ReadFile(path)
	if string(actual) != expected {
		t.Fatalf("Unexpected buffer:\nexp:\n%s\ngot:\n%s", expected, string(actual))
	}
	if index, term := log.CommitInfo(); index != 2 || term != 1 {
		t.Fatalf("Invalid commit info [IDX=%v, TERM=%v]", index, term)
	}

	// Full commit.
	if err := log.SetCommitIndex(3); err != nil {
		t.Fatalf("Unable to commit: %v", err)
	}
	expected = `cf4aab23 0000000000000001 0000000000000001 cmd_1 {"val":"foo","i":20}` + "\n" +
		`4c08d91f 0000000000000002 0000000000000001 cmd_2 {"x":100}` + "\n" +
		`6ac5807c 0000000000000003 0000000000000002 cmd_1 {"val":"bar","i":0}` + "\n"
	actual, _ = ioutil.ReadFile(path)
	if string(actual) != expected {
		t.Fatalf("Unexpected buffer:\nexp:\n%s\ngot:\n%s", expected, string(actual))
	}
	if index, term := log.CommitInfo(); index != 3 || term != 2 {
		t.Fatalf("Invalid commit info [IDX=%v, TERM=%v]", index, term)
	}
}

// Ensure that we can decode and encode to an existing log.
func TestLogExistingLog(t *testing.T) {
	log, path := setupLog(`cf4aab23 0000000000000001 0000000000000001 cmd_1 {"val":"foo","i":20}` + "\n" +
		`4c08d91f 0000000000000002 0000000000000001 cmd_2 {"x":100}` + "\n" +
		`6ac5807c 0000000000000003 0000000000000002 cmd_1 {"val":"bar","i":0}` + "\n")
	defer log.Close()
	defer os.Remove(path)

	// Validate existing log entries.
	if len(log.entries) != 3 {
		t.Fatalf("Expected 3 entries, got %d", len(log.entries))
	}
	if log.entries[0].Index != 1 || log.entries[0].Term != 1 || !reflect.DeepEqual(log.entries[0].Command, &TestCommand1{"foo", 20}) {
		t.Fatalf("Unexpected entry[0]: %v", log.entries[0])
	}
	if log.entries[1].Index != 2 || log.entries[1].Term != 1 || !reflect.DeepEqual(log.entries[1].Command, &TestCommand2{100}) {
		t.Fatalf("Unexpected entry[1]: %v", log.entries[1])
	}
	if log.entries[2].Index != 3 || log.entries[2].Term != 2 || !reflect.DeepEqual(log.entries[2].Command, &TestCommand1{"bar", 0}) {
		t.Fatalf("Unexpected entry[2]: %v", log.entries[2])
	}
}

// Ensure that we can check the contents of the log by index/term.
func TestLogContainsEntries(t *testing.T) {
	log, path := setupLog(`cf4aab23 0000000000000001 0000000000000001 cmd_1 {"val":"foo","i":20}` + "\n" +
		`4c08d91f 0000000000000002 0000000000000001 cmd_2 {"x":100}` + "\n" +
		`6ac5807c 0000000000000003 0000000000000002 cmd_1 {"val":"bar","i":0}` + "\n")
	defer log.Close()
	defer os.Remove(path)

	if log.ContainsEntry(0, 0) {
		t.Fatalf("Zero-index entry should not exist in log.")
	}
	if log.ContainsEntry(1, 0) {
		t.Fatalf("Entry with mismatched term should not exist")
	}
	if log.ContainsEntry(4, 0) {
		t.Fatalf("Out-of-range entry should not exist")
	}
	if !log.ContainsEntry(2, 1) {
		t.Fatalf("Entry 2/1 should exist")
	}
	if !log.ContainsEntry(3, 2) {
		t.Fatalf("Entry 2/1 should exist")
	}
}

// Ensure that we can recover from an incomplete/corrupt log and continue logging.
func TestLogRecovery(t *testing.T) {
	path := setupLogFile(`cf4aab23 0000000000000001 0000000000000001 cmd_1 {"val":"foo","i":20}` + "\n" +
		`4c08d91f 0000000000000002 0000000000000001 cmd_2 {"x":100}` + "\n" +
		`6ac5807c 0000000000000003 00000000000`)
	log := NewLog()
	log.ApplyFunc = func(c Command) (interface{}, error) {
		return nil, nil
	}
	if err := log.Open(path); err != nil {
		t.Fatalf("Unable to open log: %v", err)
	}
	defer log.Close()
	defer os.Remove(path)

	if err := log.AppendEntry(NewLogEntry(log, 3, 2, &TestCommand1{"bat", -5})); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}

	// Validate existing log entries.
	if len(log.entries) != 3 {
		t.Fatalf("Expected 2 entries, got %d", len(log.entries))
	}
	if log.entries[0].Index != 1 || log.entries[0].Term != 1 || !reflect.DeepEqual(log.entries[0].Command, &TestCommand1{"foo", 20}) {
		t.Fatalf("Unexpected entry[0]: %v", log.entries[0])
	}
	if log.entries[1].Index != 2 || log.entries[1].Term != 1 || !reflect.DeepEqual(log.entries[1].Command, &TestCommand2{100}) {
		t.Fatalf("Unexpected entry[1]: %v", log.entries[1])
	}
	if log.entries[2].Index != 3 || log.entries[2].Term != 2 || !reflect.DeepEqual(log.entries[2].Command, &TestCommand1{"bat", -5}) {
		t.Fatalf("Unexpected entry[2]: %v", log.entries[2])
	}

	// Validate precommit log contents.
	expected := `cf4aab23 0000000000000001 0000000000000001 cmd_1 {"val":"foo","i":20}` + "\n" +
		`4c08d91f 0000000000000002 0000000000000001 cmd_2 {"x":100}` + "\n"
	actual, _ := ioutil.ReadFile(path)
	if string(actual) != expected {
		t.Fatalf("Unexpected buffer:\nexp:\n%s\ngot:\n%s", expected, string(actual))
	}

	// Validate committed log contents.
	if err := log.SetCommitIndex(3); err != nil {
		t.Fatalf("Unable to partially commit: %v", err)
	}
	expected = `cf4aab23 0000000000000001 0000000000000001 cmd_1 {"val":"foo","i":20}` + "\n" +
		`4c08d91f 0000000000000002 0000000000000001 cmd_2 {"x":100}` + "\n" +
		`3f3f884c 0000000000000003 0000000000000002 cmd_1 {"val":"bat","i":-5}` + "\n"
	actual, _ = ioutil.ReadFile(path)
	if string(actual) != expected {
		t.Fatalf("Unexpected buffer:\nexp:\n%s\ngot:\n%s", expected, string(actual))
	}
}

//--------------------------------------
// Append
//--------------------------------------

// Ensure that we can truncate uncommitted entries in the log.
func TestLogTruncate(t *testing.T) {
	log, path := setupLog("")
	if err := log.Open(path); err != nil {
		t.Fatalf("Unable to open log: %v", err)
	}
	defer log.Close()
	defer os.Remove(path)

	entry1 := NewLogEntry(log, 1, 1, &TestCommand1{"foo", 20})
	if err := log.AppendEntry(entry1); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}
	entry2 := NewLogEntry(log, 2, 1, &TestCommand2{100})
	if err := log.AppendEntry(entry2); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}
	entry3 := NewLogEntry(log, 3, 2, &TestCommand1{"bar", 0})
	if err := log.AppendEntry(entry3); err != nil {
		t.Fatalf("Unable to append: %v", err)
	}
	if err := log.SetCommitIndex(2); err != nil {
		t.Fatalf("Unable to partially commit: %v", err)
	}

	// Truncate committed entry.
	if err := log.Truncate(1, 1); err == nil || err.Error() != "raft.Log: Index is already committed (2): (IDX=1, TERM=1)" {
		t.Fatalf("Truncating committed entries shouldn't work: %v", err)
	}
	// Truncate past end of log.
	if err := log.Truncate(4, 2); err == nil || err.Error() != "raft.Log: Entry index does not exist (MAX=3): (IDX=4, TERM=2)" {
		t.Fatalf("Truncating past end-of-log shouldn't work: %v", err)
	}
	// Truncate entry with mismatched term.
	if err := log.Truncate(2, 2); err == nil || err.Error() != "raft.Log: Entry at index does not have matching term (1): (IDX=2, TERM=2)" {
		t.Fatalf("Truncating mismatched entries shouldn't work: %v", err)
	}
	// Truncate end of log.
	if err := log.Truncate(3, 2); !(err == nil && reflect.DeepEqual(log.entries, []*LogEntry{entry1, entry2, entry3})) {
		t.Fatalf("Truncating end of log should work: %v\n\nEntries:\nActual: %v\nExpected: %v", err, log.entries, []*LogEntry{entry1, entry2, entry3})
	}
	// Truncate at last commit.
	if err := log.Truncate(2, 1); !(err == nil && reflect.DeepEqual(log.entries, []*LogEntry{entry1, entry2})) {
		t.Fatalf("Truncating at last commit should work: %v\n\nEntries:\nActual: %v\nExpected: %v", err, log.entries, []*LogEntry{entry1, entry2})
	}

}
