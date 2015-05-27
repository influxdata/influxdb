package cluster

import (
	"net"
	"sync"

	"github.com/fatih/pool"
)

type clientPool struct {
	mu   sync.RWMutex
	pool map[uint64]pool.Pool
}

func newClientPool() *clientPool {
	return &clientPool{
		pool: make(map[uint64]pool.Pool),
	}
}

func (c *clientPool) setPool(nodeID uint64, p pool.Pool) {
	c.mu.Lock()
	c.pool[nodeID] = p
	c.mu.Unlock()
}

func (c *clientPool) getPool(nodeID uint64) (pool.Pool, bool) {
	c.mu.Lock()
	p, ok := c.pool[nodeID]
	c.mu.Unlock()
	return p, ok
}

func (c *clientPool) size() int {
	c.mu.RLock()
	var size int
	for _, p := range c.pool {
		size += p.Len()
	}
	c.mu.RUnlock()
	return size
}

func (c *clientPool) conn(nodeID uint64) (net.Conn, error) {
	c.mu.Lock()
	conn, err := c.pool[nodeID].Get()
	c.mu.Unlock()
	return conn, err
}

func (c *clientPool) close() {
	c.mu.Lock()
	for _, p := range c.pool {
		p.Close()
	}
	c.mu.Unlock()
}
