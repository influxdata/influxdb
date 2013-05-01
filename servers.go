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

// Sets the ApplyFunc handler for a set of servers.
func (s Servers) SetApplyFunc(f func(*Server, Command)) {
	for _, server := range s {
		server.ApplyFunc = f
	}
}

// Sets the RequestVoteHandler for a set of servers.
func (s Servers) SetRequestVoteHandler(f func(*Server, *Peer, *RequestVoteRequest) (*RequestVoteResponse, error)) {
	for _, server := range s {
		server.RequestVoteHandler = f
	}
}
