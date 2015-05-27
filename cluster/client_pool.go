package cluster

import (
	"net"
	"sync"

	"github.com/fatih/pool"
	"github.com/influxdb/influxdb/meta"
)

type clientPool struct {
	mu   sync.RWMutex
	pool map[*meta.NodeInfo]pool.Pool
}

func newClientPool() *clientPool {
	return &clientPool{
		pool: make(map[*meta.NodeInfo]pool.Pool),
	}
}

func (c *clientPool) setPool(n *meta.NodeInfo, p pool.Pool) {
	c.mu.Lock()
	c.pool[n] = p
	c.mu.Unlock()
}

func (c *clientPool) getPool(n *meta.NodeInfo) (pool.Pool, bool) {
	c.mu.Lock()
	p, ok := c.pool[n]
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

func (c *clientPool) conn(n *meta.NodeInfo) (net.Conn, error) {
	c.mu.Lock()
	conn, err := c.pool[n].Get()
	c.mu.Unlock()
	return conn, err
}

func (c *clientPool) close() error {
	c.mu.Lock()
	for _, p := range c.pool {
		p.Close()
	}
	c.mu.Unlock()
	return nil
}
