package raft

import (
	"encoding/binary"
	"io"
)

const appendEntriesResponseHeaderSize = 4 + 8 + 1 + 8 + 8

// The response returned from a server appending entries to the log.
type AppendEntriesResponse struct {
	Term uint64
	// the current index of the server
	Index       uint64
	Success     bool
	CommitIndex uint64
	peer        string
	append      bool
}

// Creates a new AppendEntries response.
func newAppendEntriesResponse(term uint64, success bool, index uint64, commitIndex uint64) *AppendEntriesResponse {
	return &AppendEntriesResponse{
		Term:        term,
		Success:     success,
		Index:       index,
		CommitIndex: commitIndex,
	}
}


func (resp *AppendEntriesResponse) encode(w io.Writer) (int, error) {
	b := make([]byte, appendEntriesResponseHeaderSize)

	binary.BigEndian.PutUint32(b[0:4], protocolVersion)
	binary.BigEndian.PutUint64(b[4:12], resp.Term)
	bigEndianPutBool(b[12:13], resp.Success)
	binary.BigEndian.PutUint64(b[13:21], resp.Index)
	binary.BigEndian.PutUint64(b[21:29], resp.CommitIndex)

	return w.Write(b)
}

func (resp *AppendEntriesResponse) decode(r io.Reader) (int, error) {
	var eof error
	header := make([]byte, appendEntriesResponseHeaderSize)
	if n, err := r.Read(header); err == io.EOF {
		if n == len(header) {
			eof = io.EOF
		} else {
			return n, io.ErrUnexpectedEOF
		}
	} else if err != nil {
		return n, err
	}

	// Verify that the encoding format can be read.
	if version := binary.BigEndian.Uint32(header[0:4]); version != protocolVersion {
		return appendEntriesResponseHeaderSize, errUnsupportedLogVersion
	}

	resp.Term = binary.BigEndian.Uint64(header[4:12])
	resp.Success = bigEndianBool(header[12:13])
	resp.Index = binary.BigEndian.Uint64(header[13:21])
	resp.CommitIndex = binary.BigEndian.Uint64(header[21:29])

	return appendEntriesResponseHeaderSize, eof
}
