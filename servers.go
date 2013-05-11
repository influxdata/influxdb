package raft

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A collection of servers.
type Servers []*Server

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Handlers
//--------------------------------------

// Sets the RequestVoteHandler for a set of servers.
func (s Servers) SetRequestVoteHandler(f func(*Server, *Peer, *RequestVoteRequest) (*RequestVoteResponse, error)) {
	for _, server := range s {
		server.RequestVoteHandler = f
	}
}
