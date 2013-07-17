package raft

import (
	"encoding/binary"
	"io"
)

const requestVoteResponseHeaderSize = 4 + 8 + 1

// The response returned from a server after a vote for a candidate to become a leader.
type RequestVoteResponse struct {
	peer        *Peer
	Term        uint64
	VoteGranted bool
}

// Creates a new RequestVote response.
func newRequestVoteResponse(term uint64, voteGranted bool) *RequestVoteResponse {
	return &RequestVoteResponse{
		Term:        term,
		VoteGranted: voteGranted,
	}
}

func (resp *RequestVoteResponse) encode(w io.Writer) (int, error) {
	b := make([]byte, requestVoteResponseHeaderSize)

	binary.BigEndian.PutUint32(b[0:4], protocolVersion)
	binary.BigEndian.PutUint64(b[4:12], resp.Term)
	bigEndianPutBool(b[12:13], resp.VoteGranted)

	return w.Write(b)
}

func (resp *RequestVoteResponse) decode(r io.Reader) (int, error) {
	var eof error
	header := make([]byte, requestVoteResponseHeaderSize)
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
		return requestVoteResponseHeaderSize, errUnsupportedLogVersion
	}

	resp.Term = binary.BigEndian.Uint64(header[4:12])
	resp.VoteGranted = bigEndianBool(header[12:13])

	return requestVoteResponseHeaderSize, eof
}
