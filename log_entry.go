package raft

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"encoding/json"
	"fmt"
	"github.com/benbjohnson/go-raft/protobuf"
	"io"
)

// A log entry stores a single item in the log.
type LogEntry struct {
	log         *Log
	Index       uint64
	Term        uint64
	CommandName string
	Command     []byte
	Position    int64 // position in the log file
	commit      chan bool
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
		log:         log,
		Index:       index,
		Term:        term,
		CommandName: commandName,
		Command:     buf.Bytes(),
		commit:      make(chan bool, 5),
	}

	return e, nil
}

// Encodes the log entry to a buffer. Returns the number of bytes
// written and any error that may have occurred.
func (e *LogEntry) encode(w io.Writer) (int, error) {

	p := proto.NewBuffer(nil)

	pb := &protobuf.ProtoLogEntry{
		Index:       proto.Uint64(e.Index),
		Term:        proto.Uint64(e.Term),
		CommandName: proto.String(e.CommandName),
		Command:     e.Command,
	}

	err := p.Marshal(pb)

	if err != nil {
		return -1, err
	}

	_, err = fmt.Fprintf(w, "%8x\n", len(p.Bytes()))

	if err != nil {
		return -1, err
	}

	return w.Write(p.Bytes())
}

// Decodes the log entry from a buffer. Returns the number of bytes read and
// any error that occurs.
func (e *LogEntry) decode(r io.Reader) (int, error) {

	var length int
	_, err := fmt.Fscanf(r, "%8x\n", &length)
	if err != nil {
		return -1, err
	}

	data := make([]byte, length)
	_, err = r.Read(data)

	if err != nil {
		return -1, err
	}

	pb := &protobuf.ProtoLogEntry{}
	p := proto.NewBuffer(data)
	err = p.Unmarshal(pb)

	if err != nil {
		return -1, err
	}

	e.Term = pb.GetTerm()
	e.Index = pb.GetIndex()
	e.CommandName = pb.GetCommandName()
	e.Command = pb.Command

	return length, nil
}
