package raft

import (
	"bufio"
	"bytes"
	"errors"
	"hash/crc32"
	"fmt"
	"io"
	"encoding/json"
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
		log: log,
		index: index,
		term: term,
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
	if _, err = fmt.Fprintf(&b, "%016x %016x %s %s\n", e.index, e.term, e.command.Name(), encodedCommand); err != nil {
		return err
	}

	// Generate checksum.
	checksum := crc32.ChecksumIEEE(b.Bytes())

	// Write log entry with checksum.
	_, err = fmt.Fprintf(w, "%08x %s", checksum, b.String())
	return err
}

// Decodes the log entry from a buffer.
func (e *LogEntry) Decode(r io.Reader) error {
	if r == nil {
		return errors.New("raft.LogEntry: Reader required to decode")
	}
	
	// Read the expected checksum first.
	var checksum uint32
	if _, err := fmt.Fscanf(r, "%08x", &checksum); err != nil {
		return fmt.Errorf("raft.LogEntry: Unable to read checksum: %v", err)
	}

	// Read the rest of the line.
	bufr := bufio.NewReader(r)
	if c, _ := bufr.ReadByte(); c != ' ' {
		return fmt.Errorf("raft.LogEntry: Expected space, received %02x", c)
	}

	line, err := bufr.ReadString('\n')
	if err == io.EOF {
		return fmt.Errorf("raft.LogEntry: Unexpected EOF")
	} else if err != nil {
		return fmt.Errorf("raft.LogEntry: Unable to read line: %v", err)
	}
	b := bytes.NewBufferString(line)

	// Verify checksum.
	bchecksum := crc32.ChecksumIEEE(b.Bytes())
	if checksum != bchecksum {
		return fmt.Errorf("raft.LogEntry: Invalid checksum: Expected %08x, calculated %08x", checksum, bchecksum)
	}

	// Read term, index and command name.
	var commandName string
	if _, err := fmt.Fscanf(b, "%016x %016x %s ", &e.index, &e.term, &commandName); err != nil {
		return fmt.Errorf("raft.LogEntry: Unable to scan: %v", err)
	}

	// Instantiate command by name.
	command, err := e.log.NewCommand(commandName)
	if err != nil {
		return fmt.Errorf("raft.LogEntry: Unable to instantiate command (%s): %v", commandName, err)
	}

	// Deserialize command.
	if err = json.NewDecoder(b).Decode(&command); err != nil {
		return fmt.Errorf("raft.LogEntry: Unable to decode: %v", err)
	}
	e.command = command
	
	// Make sure there's only an EOF remaining.
	if c, err := b.ReadByte(); err != io.EOF {
		return fmt.Errorf("raft.LogEntry: Expected EOL, received %02x", c)
	}

	return nil
}
