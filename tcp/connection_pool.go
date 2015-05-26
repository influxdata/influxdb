package tcp

import (
	"log"
	"net"
	"sync"
)

type connectionPool struct {
	mu   sync.RWMutex
	pool map[string]Pool
}

func newConnectionPool() *connectionPool {
	return &connectionPool{
		pool: make(map[string]Pool),
	}
}

func (c *connectionPool) setPool(addr string, p Pool) {
	log.Println("setting pool")
	c.mu.Lock()
	c.pool[addr] = p
	c.mu.Unlock()
	log.Println("setting pool complete")
}

func (c *connectionPool) getPool(addr string) (Pool, bool) {
	log.Println("getting pool")
	c.mu.Lock()
	p, ok := c.pool[addr]
	c.mu.Unlock()
	log.Println("getting pool complete")
	return p, ok
}

func (c *connectionPool) size() int {
	log.Println("getting pool size")
	c.mu.RLock()
	var size int
	for _, p := range c.pool {
		size += p.Len()
	}
	c.mu.RUnlock()
	log.Println("getting pool size complete")
	return size
}

func (c *connectionPool) conn(addr string) (net.Conn, error) {
	log.Println("getting connection")
	c.mu.Lock()
	conn, err := c.pool[addr].Get()
	c.mu.Unlock()
	log.Println("getting connection complete")
	return conn, err
}

func (c *connectionPool) close() error {
	log.Println("closing")
	c.mu.Lock()
	for _, p := range c.pool {
		p.Close()
	}
	c.mu.Unlock()
	return nil
}
