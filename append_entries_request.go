package raft

import (
	"bytes"
	"encoding/binary"
	"io"
)

const appendEntriesRequestHeaderSize = 4 + 8 + 8 + 8 + 8 + 4 + 4

// The request sent to a server to append entries to the log.
type AppendEntriesRequest struct {
	Term         uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	CommitIndex  uint64
	LeaderName   string
	Entries      []*LogEntry
}

// Creates a new AppendEntries request.
func newAppendEntriesRequest(term uint64, prevLogIndex uint64, prevLogTerm uint64, commitIndex uint64, leaderName string, entries []*LogEntry) *AppendEntriesRequest {
	return &AppendEntriesRequest{
		Term:         term,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		CommitIndex:  commitIndex,
		LeaderName:   leaderName,
		Entries:      entries,
	}
}

func (req *AppendEntriesRequest) encode(w io.Writer) (int, error) {
	leaderNameSize := len(req.LeaderName)
	b := make([]byte, appendEntriesRequestHeaderSize + leaderNameSize)

	// Write request.
	binary.BigEndian.PutUint32(b[0:4], protocolVersion)
	binary.BigEndian.PutUint64(b[4:12], req.Term)
	binary.BigEndian.PutUint64(b[12:20], req.PrevLogIndex)
	binary.BigEndian.PutUint64(b[20:28], req.PrevLogTerm)
	binary.BigEndian.PutUint64(b[28:36], req.CommitIndex)
	binary.BigEndian.PutUint32(b[36:40], uint32(leaderNameSize))
	binary.BigEndian.PutUint32(b[40:44], uint32(len(req.Entries)))
	copy(b[44:44+leaderNameSize], []byte(req.LeaderName))

	// Append entries.
	buf := bytes.NewBuffer(b)
	for _, entry := range req.Entries {
		if _, err := entry.encode(buf); err != nil {
			return 0, err
		}
	}

	return w.Write(buf.Bytes())
}

func (req *AppendEntriesRequest) decode(r io.Reader) (int, error) {
	var eof error
	header := make([]byte, appendEntriesRequestHeaderSize)
	if n, err := r.Read(header); err == io.EOF {
		return n, io.ErrUnexpectedEOF
	} else if err != nil {
		return n, err
	}
	entryCount := int(binary.BigEndian.Uint32(header[40:44]))

	// Read leader name.
	leaderName := make([]byte, binary.BigEndian.Uint32(header[36:40]))
	if n, err := r.Read(leaderName); err == io.EOF {
		if err == io.EOF && n != len(leaderName) {
			return appendEntriesRequestHeaderSize+n, io.ErrUnexpectedEOF
		} else {
			eof = io.EOF
		}
	} else if err != nil {
		return appendEntriesRequestHeaderSize+n, err
	}
	totalBytes := appendEntriesRequestHeaderSize + len(leaderName)

	// Read entries.
	entries := []*LogEntry{}
	for i:=0; i<entryCount; i++ {
		entry := &LogEntry{}
		n, err := entry.decode(r)
		entries = append(entries, entry)
		totalBytes += n
		
		if err == io.EOF {
			if len(entries) == entryCount {
				err = io.EOF
			} else {
				return totalBytes, io.ErrUnexpectedEOF
			}
		} else if err != nil {
			return totalBytes, err
		}
	}

	// Verify that the encoding format can be read.
	if version := binary.BigEndian.Uint32(header[0:4]); version != protocolVersion {
		return totalBytes, errUnsupportedLogVersion
	}

	req.Term = binary.BigEndian.Uint64(header[4:12])
	req.PrevLogIndex = binary.BigEndian.Uint64(header[12:20])
	req.PrevLogTerm = binary.BigEndian.Uint64(header[20:28])
	req.CommitIndex = binary.BigEndian.Uint64(header[28:36])
	req.LeaderName = string(leaderName)
	req.Entries = entries

	return totalBytes, eof
}
