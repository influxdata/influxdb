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
}

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A command represents an action to be taken on the replicated state machine.
type Command interface {
	CommandName() string
	Apply(server *Server) (interface{}, error)
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
		panic(fmt.Sprintf("raft: Unable to copy command: %s (%v)", command.CommandName(), reflect.ValueOf(v).Kind().String()))
	}
	return copy, nil
}

//--------------------------------------
// Registration
//--------------------------------------

// Registers a command by storing a reference to an instance of it.
func RegisterCommand(command Command) {
	if command == nil {
		panic(fmt.Sprintf("raft: Cannot register nil"))
	} else if commandTypes[command.CommandName()] != nil {
		// we need to register NOP command at the beginning
		// for testing, it may register mutliple times
		// i am not quite familiar with reg prorcess
		// maybe you can fix it. sorry!

		//panic(fmt.Sprintf("raft: Duplicate registration: %s", command.CommandName()))
		return
	}
	commandTypes[command.CommandName()] = command
}

//--------------------------------------
// NOP command
//--------------------------------------

// NOP command
type NOPCommand struct {
}

// The name of the NOP command in the log
func (c NOPCommand) CommandName() string {
	return "nop"
}

// NOP
func (c NOPCommand) Apply(server *Server) (interface{}, error) {
	return nil, nil
}
