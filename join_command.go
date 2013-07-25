package raft

// Join command
type JoinCommand struct {
	Name string `json:"name"`
}

// The name of the Join command in the log
func (c *JoinCommand) CommandName() string {
	return "raft:join"
}

func (c *JoinCommand) Apply(server *Server) (interface{}, error) {
	err := server.AddPeer(c.Name)

	return []byte("join"), err
}
