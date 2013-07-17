package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
)

const logEntryHeaderSize int = 4 + 4 + 8 + 8 + 4 + 4

var errInvalidChecksum = errors.New("Invalid checksum")

// A log entry stores a single item in the log.
type LogEntry struct {
	log     *Log
	Index   uint64
	Term    uint64
	CommandName string
	Command []byte
	commit  chan bool
}

// Creates a new log entry associated with a log.
func newLogEntry(log *Log, index uint64, term uint64, command Command) (*LogEntry, error) {
	var buf bytes.Buffer
	var commandName string
	if command != nil {
		commandName = command.CommandName()
		if encoder, ok := command.(CommandEncoder); ok {
			if err := encoder.Encode(&buf); err != nil {
				return nil, err
			}
		} else {
			json.NewEncoder(&buf).Encode(command)
		}
	}

	e := &LogEntry{
		log:     log,
		Index:   index,
		Term:    term,
		CommandName: commandName,
		Command: buf.Bytes(),
		commit:  make(chan bool, 5),
	}

	return e, nil
}

// Encodes the log entry to a buffer. Returns the number of bytes
// written and any error that may have occurred.
func (e *LogEntry) encode(w io.Writer) (int, error) {
	commandNameSize, commandSize := len([]byte(e.CommandName)), len(e.Command)
	b := make([]byte, logEntryHeaderSize + commandNameSize + commandSize)

	// Write log entry.
	binary.BigEndian.PutUint32(b[4:8], protocolVersion)
	binary.BigEndian.PutUint64(b[8:16], e.Term)
	binary.BigEndian.PutUint64(b[16:24], e.Index)
	binary.BigEndian.PutUint32(b[24:28], uint32(commandNameSize))
	binary.BigEndian.PutUint32(b[28:32], uint32(commandSize))
	copy(b[32:32+commandNameSize], []byte(e.CommandName))
	copy(b[32+commandNameSize:], e.Command)

	// Write checksum.
	binary.BigEndian.PutUint32(b[0:4], crc32.ChecksumIEEE(b[4:]))

	return w.Write(b)
}

// Decodes the log entry from a buffer. Returns the number of bytes read and
// any error that occurs.
func (e *LogEntry) decode(r io.Reader) (int, error) {
	// Read the header.
	header := make([]byte, logEntryHeaderSize)
	if n, err := r.Read(header); err != nil {
		return n, err
	}

	// Read command name.
	commandName := make([]byte, binary.BigEndian.Uint32(header[24:28]))
	if n, err := r.Read(commandName); err != nil {
		return logEntryHeaderSize+n, err
	}

	// Read command data.
	command := make([]byte, binary.BigEndian.Uint32(header[28:32]))
	if n, err := r.Read(command); err != nil {
		return logEntryHeaderSize+len(commandName)+n, err
	}
	totalBytes := logEntryHeaderSize + len(commandName) + len(command)

	// Verify checksum.
	checksum := binary.BigEndian.Uint32(header[0:4])
	crc := crc32.NewIEEE()
	crc.Write(header[4:])
	crc.Write(commandName)
	crc.Write(command)
	if checksum != crc.Sum32() {
		return totalBytes, errInvalidChecksum
	}

	// Verify that the encoding format can be read.
	if version := binary.BigEndian.Uint32(header[4:8]); version != protocolVersion {
		return totalBytes, errUnsupportedLogVersion
	}
	
	e.Term = binary.BigEndian.Uint64(header[8:16])
	e.Index = binary.BigEndian.Uint64(header[16:24])
	e.CommandName = string(commandName)
	e.Command = command

	return totalBytes, nil
}
