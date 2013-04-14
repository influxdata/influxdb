package raft

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A command represents an action to be taken on the replicated state machine.
type Command interface {
	Name() string
}
