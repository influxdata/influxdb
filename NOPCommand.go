package raft

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
