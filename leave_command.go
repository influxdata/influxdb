package raft

// Leave command
type LeaveCommand struct {
	Name string `json:"name"`
}

// The name of the Leave command in the log
func (c *LeaveCommand) CommandName() string {
	return "raft:leave"
}

func (c *LeaveCommand) Apply(server *Server) (interface{}, error) {
	err := server.RemovePeer(c.Name)

	return []byte("leave"), err
}
