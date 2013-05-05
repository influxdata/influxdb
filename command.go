package raft

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A command represents an action to be taken on the replicated state machine.
type Command interface {
	CommandName() string
	Validate(server *Server) error
	Apply(server *Server)
}

// This is a marker interface to filter out commands that are processed
// internally by the protocol such as the "Join" command.
type InternalCommand interface {
	InternalCommand() bool
}
