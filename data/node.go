package data

func NewDataNode() *Node {
	return &Node{}
}

type Node struct {
	//ms meta.Store
}

func (n *Node) Open() error {
	// Open shards
	// Start AE for Node
	return nil
}

func (n *Node) Close() error { return nil }
func (n *Node) Init() error  { return nil }
