package influxdb

import (
	"math/rand"
	"time"
)

// Balancer represents a load-balancing algorithm for a set of DataNodes
type Balancer interface {
	// Next returns the next DataNode according to the balancing method
	// or nil if there are no nodes available
	Next() *DataNode
}

type dataNodeBalancer struct {
	dataNodes []*DataNode // data nodes to balance between
	p         int         // current node index
}

// NewDataNodeBalancer create a shuffled, round-robin balancer so that
// multiple instances will return nodes in randomized order and each
// each returned DataNode will be repeated in a cycle
func NewDataNodeBalancer(dataNodes []*DataNode) Balancer {
	// make a copy of the dataNode slice so we can randomize it
	// without affecting the original instance as well as ensure
	// that each Balancer returns nodes in a different order
	nodes := make([]*DataNode, len(dataNodes))
	copy(nodes, dataNodes)

	b := &dataNodeBalancer{
		dataNodes: nodes,
	}
	b.shuffle()
	return b
}

// shuffle randomizes the ordering the balancers available DataNodes
func (b *dataNodeBalancer) shuffle() {
	for i := range b.dataNodes {
		j := rand.Intn(i + 1)
		b.dataNodes[i], b.dataNodes[j] = b.dataNodes[j], b.dataNodes[i]
	}
}

// online returns a slice of the DataNodes that are online
func (b *dataNodeBalancer) online() []*DataNode {
	now := time.Now().UTC()
	up := []*DataNode{}
	for _, n := range b.dataNodes {
		if n.OfflineUntil.After(now) {
			continue
		}
		up = append(up, n)
	}
	return up
}

// Next returns the next available DataNode
func (b *dataNodeBalancer) Next() *DataNode {
	// only use online nodes
	up := b.online()

	// no nodes online
	if len(up) == 0 {
		return nil
	}

	// rollover back to the beginning
	if b.p >= len(up) {
		b.p = 0
	}

	d := up[b.p]
	b.p += 1

	return d
}
