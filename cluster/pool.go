package cluster

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/fatih/pool.v2"
)

// boundedPool implements the Pool interface based on buffered channels.
type boundedPool struct {
	// storage for our net.Conn connections
	mu    sync.Mutex
	conns chan net.Conn

	timeout time.Duration
	total   int32
	// net.Conn generator
	factory Factory
}

// Factory is a function to create new connections.
type Factory func() (net.Conn, error)

// NewBoundedPool returns a new pool based on buffered channels with an initial
// capacity, maximum capacity and timeout to wait for a connection from the pool.
// Factory is used when initial capacity is
// greater than zero to fill the pool. A zero initialCap doesn't fill the Pool
// until a new Get() is called. During a Get(), If there is no new connection
// available in the pool and total connections is less than the max, a new connection
// will be created via the Factory() method.  Othewise, the call will block until
// a connection is available or the timeout is reached.
func NewBoundedPool(initialCap, maxCap int, timeout time.Duration, factory Factory) (pool.Pool, error) {
	if initialCap < 0 || maxCap <= 0 || initialCap > maxCap {
		return nil, errors.New("invalid capacity settings")
	}

	c := &boundedPool{
		conns:   make(chan net.Conn, maxCap),
		factory: factory,
		timeout: timeout,
	}

	// create initial connections, if something goes wrong,
	// just close the pool error out.
	for i := 0; i < initialCap; i++ {
		conn, err := factory()
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("factory is not able to fill the pool: %s", err)
		}
		c.conns <- conn
		atomic.AddInt32(&c.total, 1)
	}

	return c, nil
}

func (c *boundedPool) getConns() chan net.Conn {
	c.mu.Lock()
	conns := c.conns
	c.mu.Unlock()
	return conns
}

// Get implements the Pool interfaces Get() method. If there is no new
// connection available in the pool, a new connection will be created via the
// Factory() method.
func (c *boundedPool) Get() (net.Conn, error) {
	conns := c.getConns()
	if conns == nil {
		return nil, pool.ErrClosed
	}

	// Try and grab a connection from the pool
	select {
	case conn := <-conns:
		if conn == nil {
			return nil, pool.ErrClosed
		}
		return c.wrapConn(conn), nil
	default:
		// Could not get connection, can we create a new one?
		if atomic.LoadInt32(&c.total) < int32(cap(conns)) {
			conn, err := c.factory()
			if err != nil {
				return nil, err
			}
			atomic.AddInt32(&c.total, 1)

			return c.wrapConn(conn), nil
		}
	}

	// The pool was empty and we couldn't create a new one to
	// retry until one is free or we timeout
	select {
	case conn := <-conns:
		if conn == nil {
			return nil, pool.ErrClosed
		}
		return c.wrapConn(conn), nil
	case <-time.After(c.timeout):
		return nil, fmt.Errorf("timed out waiting for free connection")
	}

}

// put puts the connection back to the pool. If the pool is full or closed,
// conn is simply closed. A nil conn will be rejected.
func (c *boundedPool) put(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conns == nil {
		// pool is closed, close passed connection
		return conn.Close()
	}

	// put the resource back into the pool. If the pool is full, this will
	// block and the default case will be executed.
	select {
	case c.conns <- conn:
		return nil
	default:
		// pool is full, close passed connection
		return conn.Close()
	}
}

func (c *boundedPool) Close() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for conn := range conns {
		conn.Close()
	}
}

func (c *boundedPool) Len() int { return len(c.getConns()) }

// newConn wraps a standard net.Conn to a poolConn net.Conn.
func (c *boundedPool) wrapConn(conn net.Conn) net.Conn {
	p := &pooledConn{c: c}
	p.Conn = conn
	return p
}

// pooledConn is a wrapper around net.Conn to modify the the behavior of
// net.Conn's Close() method.
type pooledConn struct {
	net.Conn
	c        *boundedPool
	unusable bool
}

// Close() puts the given connects back to the pool instead of closing it.
func (p pooledConn) Close() error {
	if p.unusable {
		if p.Conn != nil {
			return p.Conn.Close()
		}
		return nil
	}
	return p.c.put(p.Conn)
}

// MarkUnusable() marks the connection not usable any more, to let the pool close it instead of returning it to pool.
func (p *pooledConn) MarkUnusable() {
	p.unusable = true
	atomic.AddInt32(&p.c.total, -1)
}
