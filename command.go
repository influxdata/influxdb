package raft

import (
	"fmt"
	"reflect"
)

//------------------------------------------------------------------------------
//
// Globals
//
//------------------------------------------------------------------------------

var commandTypes map[string]Command

func init() {
	commandTypes = map[string]Command{}
	RegisterCommand(&JoinCommand{})
}

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

//------------------------------------------------------------------------------
//
// Functions
//
//------------------------------------------------------------------------------

//--------------------------------------
// Instantiation
//--------------------------------------

// Creates a new instance of a command by name.
func newCommand(name string) (Command, error) {
	// Find the registered command.
	command := commandTypes[name]
	if command == nil {
		return nil, fmt.Errorf("raft.Command: Unregistered command type: %s", name)
	}

	// Make a copy of the command.
	v := reflect.New(reflect.Indirect(reflect.ValueOf(command)).Type()).Interface()
	copy, ok := v.(Command)
	if !ok {
		panic(fmt.Sprintf("raft.Command: Unable to copy command: %s (%v)", command.CommandName(), reflect.ValueOf(v).Kind().String()))
	}
	return copy, nil
}

//--------------------------------------
// Registration
//--------------------------------------

// Registers a command by storing a reference to an instance of it.
func RegisterCommand(command Command) {
	if command == nil {
		panic(fmt.Sprintf("raft.Command: Cannot register nil"))
	} else if commandTypes[command.CommandName()] != nil {
		panic(fmt.Sprintf("raft.Command: Duplicate registration: %s", command.CommandName()))
	}
	commandTypes[command.CommandName()] = command
}
