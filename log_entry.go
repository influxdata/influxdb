package raft

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A log entry stores a single item in the log.
type LogEntry struct {
	log     *Log
	Index   uint64      `json:"index"`
	Term    uint64      `json:"term"`
	Command Command     `json:"command"`
	commit  chan bool   `json:"-"`
}

// A temporary interface used for unmarshaling log entries.
type logEntryRawMessage struct {
	Index   uint64          `json:"index"`
	Term    uint64          `json:"term"`
	Name    string          `json:"name"`
	Command json.RawMessage `json:"command"`
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new log entry associated with a log.
func newLogEntry(log *Log, index uint64, term uint64, command Command) *LogEntry {
	return &LogEntry{
		log:     log,
		Index:   index,
		Term:    term,
		Command: command,
		commit:  make(chan bool, 5),
	}
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Encoding
//--------------------------------------

// Encodes the log entry to a buffer.
func (e *LogEntry) encode(w io.Writer) error {
	if w == nil {
		return errors.New("raft.LogEntry: Writer required to encode")
	}

	encodedCommand, err := json.Marshal(e.Command)
	if err != nil {
		return err
	}

	// Write log line to temporary buffer.
	var b bytes.Buffer
	if _, err = fmt.Fprintf(&b, "%016x %016x %s %s\n", e.Index, e.Term, e.Command.CommandName(), encodedCommand); err != nil {
		return err
	}

	// Generate checksum.
	checksum := crc32.ChecksumIEEE(b.Bytes())

	// Write log entry with checksum.
	_, err = fmt.Fprintf(w, "%08x %s", checksum, b.String())
	return err
}

// Decodes the log entry from a buffer. Returns the number of bytes read.
func (e *LogEntry) decode(r io.Reader) (pos int, err error) {
	pos = 0

	if r == nil {
		err = errors.New("raft.LogEntry: Reader required to decode")
		return
	}

	// Read the expected checksum first.
	var checksum uint32
	if _, err = fmt.Fscanf(r, "%08x", &checksum); err != nil {
		err = fmt.Errorf("raft.LogEntry: Unable to read checksum: %v", err)
		return
	}
	pos += 8

	// Read the rest of the line.
	bufr := bufio.NewReader(r)
	if c, _ := bufr.ReadByte(); c != ' ' {
		err = fmt.Errorf("raft.LogEntry: Expected space, received %02x", c)
		return
	}
	pos += 1

	line, err := bufr.ReadString('\n')
	pos += len(line)
	if err == io.EOF {
		err = fmt.Errorf("raft.LogEntry: Unexpected EOF")
		return
	} else if err != nil {
		err = fmt.Errorf("raft.LogEntry: Unable to read line: %v", err)
		return
	}
	b := bytes.NewBufferString(line)

	// Verify checksum.
	bchecksum := crc32.ChecksumIEEE(b.Bytes())
	if checksum != bchecksum {
		err = fmt.Errorf("raft.LogEntry: Invalid checksum: Expected %08x, calculated %08x", checksum, bchecksum)
		return
	}

	// Read term, index and command name.
	var commandName string
	if _, err = fmt.Fscanf(b, "%016x %016x %s ", &e.Index, &e.Term, &commandName); err != nil {
		err = fmt.Errorf("raft.LogEntry: Unable to scan: %v", err)
		return
	}

	// Instantiate command by name.
	command, err := newCommand(commandName)
	if err != nil {
		err = fmt.Errorf("raft.LogEntry: Unable to instantiate command (%s): %v", commandName, err)
		return
	}

	// Deserialize command.
	if err = json.NewDecoder(b).Decode(&command); err != nil {
		err = fmt.Errorf("raft.LogEntry: Unable to decode: %v", err)
		return
	}
	e.Command = command

	// Make sure there's only an EOF remaining.
	c, err := b.ReadByte()
	if err != io.EOF {
		err = fmt.Errorf("raft.LogEntry: Expected EOL, received %02x", c)
		return
	}

	err = nil
	return
}

//--------------------------------------
// Encoding
//--------------------------------------

// Encodes a log entry into JSON.
func (e *LogEntry) MarshalJSON() ([]byte, error) {
	obj := map[string]interface{}{
		"index": e.Index,
		"term":  e.Term,
	}
	if e.Command != nil {
		obj["name"] = e.Command.CommandName()
		obj["command"] = e.Command
	}
	return json.Marshal(obj)
}

// Decodes a log entry from a JSON byte array.
func (e *LogEntry) UnmarshalJSON(data []byte) error {
	// Extract base log entry info.
	obj := &logEntryRawMessage{}
	json.Unmarshal(data, obj)
	e.Index, e.Term = obj.Index, obj.Term

	// Create a command based on the name.
	var err error
	if e.Command, err = newCommand(obj.Name); err != nil {
		return err
	}
	json.Unmarshal(obj.Command, e.Command)

	return nil
}
