package influxdb

import "sync"

type Node struct {
	mu sync.RWMutex
	id uint64
}

func NewNode() *Node {
	// TODO: (@corylanou): make this load the id properly
	return &Node{}
}

func (n *Node) ID() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.id
}
