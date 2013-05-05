package raft

import (
	"errors"
	"fmt"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// The join command allows a server to gain membership into a cluster.
type JoinCommand struct {
	Name string `join:"name"`
}

//------------------------------------------------------------------------------
//
// Accessors
//
//------------------------------------------------------------------------------

// This function marks the command as internal.
func (c *JoinCommand) InternalCommand() bool {
	return true
}

// The name of the command in the log.
func (c *JoinCommand) CommandName() string {
	return "raft:join"
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

// Validates that the command can be executed on the current state machine.
func (c *JoinCommand) Validate(server *Server) error {
	if c.Name == "" {
		return errors.New("raft.JoinCommand: Cannot add unnamed server")
	}
	if server.peers[c.Name] != nil {
		return fmt.Errorf("raft.JoinCommand: Server with name is already registered (%s)", c.Name)
	}
	return nil
}

// Updates the state machine to join the server to the cluster.
func (c *JoinCommand) Apply(server *Server) {
	if server.name != c.Name {
		peer := NewPeer(server, c.Name, server.heartbeatTimeout)
		server.peers[peer.name] = peer
	}
}
