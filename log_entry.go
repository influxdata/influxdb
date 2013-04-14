package raft

import (
	"bufio"
	"bytes"
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
	term    uint64
	index   uint64
	command Command
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new log entry associated with a log.
func NewLogEntry(log *Log, term uint64, index uint64, command Command) *LogEntry {
	return &LogEntry{
		log: log,
		term: term,
		index: index,
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
	encodedCommand, err := json.Marshal(e.command)
	if err != nil {
		return err
	}

	// Write log line to temporary buffer.
	var b bytes.Buffer
	if _, err = fmt.Fprintf(&b, "%08x %08x %s %s\n", e.term, e.index, e.command.Name(), encodedCommand); err != nil {
		return err
	}

	// Generate checksum.
	checksum := crc32.ChecksumIEEE(b.Bytes())

	// Write log entry with checksum.
	_, err = fmt.Fprintf(w, "%04x %s", checksum, b.String())
	return err
}

// Decodes the log entry from a buffer.
func (e *LogEntry) Decode(r io.Reader) error {
	// Read the expected checksum first.
	var checksum uint32
	fmt.Fscanf(r, "%04x ", &checksum)

	// Read the rest of the line.
	line, err := bufio.NewReader(r).ReadString('\n')
	if err == io.EOF {
		return fmt.Errorf("raft.LogEntry: Unexpected EOF")
	} else if err != nil {
		return err
	}
	b := bytes.NewBufferString(line)

	// Verify checksum.
	bchecksum := crc32.ChecksumIEEE(b.Bytes())
	if checksum != bchecksum {
		return fmt.Errorf("Invalid checksum: Expected %04x, received %04x", checksum, bchecksum)
	}

	// Read term, index and command name.
	var commandName string
	if _, err := fmt.Fscanf(b, "%08x %08x %s ", &e.term, &e.index, commandName); err != nil {
		return err
	}

	// Instantiate command by name.
	command, err := e.log.NewCommand(commandName)
	if err != nil {
		return err
	}

	// Deserialize command.
	if err = json.NewDecoder(b).Decode(&command); err != nil {
		return err
	}

	// Make sure there's only a newline and EOF remaining.
	if c, _ := b.ReadByte(); c != '\n' {
		return fmt.Errorf("raft.LogEntry: Expected newline, received %02x", c)
	}
	if c, err := b.ReadByte(); err != io.EOF {
		return fmt.Errorf("raft.LogEntry: Expected EOL, received %02x", c)
	}

	return nil
}
