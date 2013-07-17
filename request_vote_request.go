package raft

import (
	"encoding/binary"
	"io"
)

const requestVoteRequestHeaderSize = 4 + 8 + 8 + 8 + 4

// The request sent to a server to vote for a candidate to become a leader.
type RequestVoteRequest struct {
	peer          *Peer
	Term          uint64
	LastLogIndex  uint64
	LastLogTerm   uint64
	CandidateName string
}

// Creates a new RequestVote request.
func newRequestVoteRequest(term uint64, candidateName string, lastLogIndex uint64, lastLogTerm uint64) *RequestVoteRequest {
	return &RequestVoteRequest{
		Term:          term,
		LastLogIndex:  lastLogIndex,
		LastLogTerm:   lastLogTerm,
		CandidateName: candidateName,
	}
}

func (req *RequestVoteRequest) encode(w io.Writer) (int, error) {
	candidateNameSize := len(req.CandidateName)
	b := make([]byte, requestVoteRequestHeaderSize + candidateNameSize)

	// Write request.
	binary.BigEndian.PutUint32(b[0:4], protocolVersion)
	binary.BigEndian.PutUint64(b[4:12], req.Term)
	binary.BigEndian.PutUint64(b[12:20], req.LastLogIndex)
	binary.BigEndian.PutUint64(b[20:28], req.LastLogTerm)
	binary.BigEndian.PutUint32(b[28:32], uint32(candidateNameSize))
	copy(b[32:], []byte(req.CandidateName))

	return w.Write(b)
}

func (req *RequestVoteRequest) decode(r io.Reader) (int, error) {
	var eof error
	header := make([]byte, requestVoteRequestHeaderSize)
	if n, err := r.Read(header); err == io.EOF {
		return n, io.ErrUnexpectedEOF
	} else if err != nil {
		return n, err
	}

	// Read candidate name.
	candidateName := make([]byte, binary.BigEndian.Uint32(header[28:32]))
	if n, err := r.Read(candidateName); err == io.EOF {
		if err == io.EOF && n != len(candidateName) {
			return requestVoteRequestHeaderSize+n, io.ErrUnexpectedEOF
		} else {
			eof = io.EOF
		}
	} else if err != nil {
		return requestVoteRequestHeaderSize+n, err
	}
	totalBytes := requestVoteRequestHeaderSize + len(candidateName)

	// Verify that the encoding format can be read.
	if version := binary.BigEndian.Uint32(header[0:4]); version != protocolVersion {
		return totalBytes, errUnsupportedLogVersion
	}

	req.Term = binary.BigEndian.Uint64(header[4:12])
	req.LastLogIndex = binary.BigEndian.Uint64(header[12:20])
	req.LastLogTerm = binary.BigEndian.Uint64(header[20:28])
	req.CandidateName = string(candidateName)

	return totalBytes, eof
}
