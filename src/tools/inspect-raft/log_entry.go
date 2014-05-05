package main

import (
	"fmt"
	"io"

	"github.com/goraft/raft/protobuf"
)

type LogEntry struct {
	protobuf.LogEntry
}

func (le *LogEntry) Encode(w io.Writer) error {
	b, err := le.Marshal()
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "%8x\n", len(b)); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func (le *LogEntry) Decode(r io.Reader) error {
	var length int
	if _, err := fmt.Fscanf(r, "%8x\n", &length); err != nil {
		return err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	return le.Unmarshal(buf)
}
