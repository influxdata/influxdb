package raft

import (
	"encoding/binary"
	"io"
)

// LogEntryEncoder encodes entries to a writer.
type LogEntryEncoder struct {
	w io.Writer
}

// NewLogEntryEncoder returns a new instance of the LogEntryEncoder that
// will encode to a writer.
func NewLogEntryEncoder(w io.Writer) *LogEntryEncoder {
	return &LogEntryEncoder{w: w}
}

// Encode writes a log entry to the encoder's writer.
func (enc *LogEntryEncoder) Encode(e *LogEntry) error {
	// Write header.
	if _, err := enc.w.Write(e.encodedHeader()); err != nil {
		return err
	}

	// Write data.
	if _, err := enc.w.Write(e.Data); err != nil {
		return err
	}
	return nil
}

// LogEntryDecoder decodes entries from a reader.
type LogEntryDecoder struct {
	r io.Reader
}

// NewLogEntryDecoder returns a new instance of the LogEntryDecoder that
// will decode from a reader.
func NewLogEntryDecoder(r io.Reader) *LogEntryDecoder {
	return &LogEntryDecoder{r: r}
}

// Decode reads a log entry from the decoder's reader.
func (dec *LogEntryDecoder) Decode(e *LogEntry) error {
	// Read first byte to determine the log entry type.
	var b [logEntryHeaderSize]byte
	if _, err := io.ReadFull(dec.r, b[:1]); err != nil {
		return err
	}
	e.Type = LogEntryType(b[0])

	// If it's a snapshot then return immediately.
	if e.Type == logEntrySnapshot {
		e.Index = 0
		e.Term = 0
		e.Data = nil
		return nil
	}

	// If it's not a snapshot then read the full header.
	if _, err := io.ReadFull(dec.r, b[1:]); err == io.EOF {
		return io.ErrUnexpectedEOF
	} else if err != nil {
		return err
	}
	sz := binary.BigEndian.Uint64(b[0:8]) & 0x00FFFFFFFFFFFFFF
	e.Index = binary.BigEndian.Uint64(b[8:16])
	e.Term = binary.BigEndian.Uint64(b[16:24])

	// Read data.
	data := make([]byte, sz)
	if _, err := io.ReadFull(dec.r, data); err != nil && err != io.EOF {
		return err
	}
	e.Data = data

	return nil
}
