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
	index   uint64
	term    uint64
	command Command
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new log entry associated with a log.
func NewLogEntry(log *Log, index uint64, term uint64, command Command) *LogEntry {
	return &LogEntry{
		log:     log,
		index:   index,
		term:    term,
		command: command,
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
func (e *LogEntry) Encode(w io.Writer) error {
	if w == nil {
		return errors.New("raft.LogEntry: Writer required to encode")
	}

	encodedCommand, err := json.Marshal(e.command)
	if err != nil {
		return err
	}

	// Write log line to temporary buffer.
	var b bytes.Buffer
	if _, err = fmt.Fprintf(&b, "%016x %016x %s %s\n", e.index, e.term, e.command.CommandName(), encodedCommand); err != nil {
		return err
	}

	// Generate checksum.
	checksum := crc32.ChecksumIEEE(b.Bytes())

	// Write log entry with checksum.
	_, err = fmt.Fprintf(w, "%08x %s", checksum, b.String())
	return err
}

// Decodes the log entry from a buffer. Returns the number of bytes read.
func (e *LogEntry) Decode(r io.Reader) (pos int, err error) {
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
	if _, err = fmt.Fscanf(b, "%016x %016x %s ", &e.index, &e.term, &commandName); err != nil {
		err = fmt.Errorf("raft.LogEntry: Unable to scan: %v", err)
		return
	}

	// Instantiate command by name.
	command, err := e.log.NewCommand(commandName)
	if err != nil {
		err = fmt.Errorf("raft.LogEntry: Unable to instantiate command (%s): %v", commandName, err)
		return
	}

	// Deserialize command.
	if err = json.NewDecoder(b).Decode(&command); err != nil {
		err = fmt.Errorf("raft.LogEntry: Unable to decode: %v", err)
		return
	}
	e.command = command

	// Make sure there's only an EOF remaining.
	c, err := b.ReadByte()
	if err != io.EOF {
		err = fmt.Errorf("raft.LogEntry: Expected EOL, received %02x", c)
		return
	}

	err = nil
	return
}
