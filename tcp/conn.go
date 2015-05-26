package tcp

import (
	"net"

	"github.com/davecgh/go-spew/spew"
)

// poolConn is a wrapper around net.Conn to modify the the behavior of
// net.Conn's Close() method.
type poolConn struct {
	net.Conn
	c *channelPool
}

// Close() puts the given connects back to the pool instead of closing it.
func (p poolConn) Close() error {
	spew.Dump("I'm back on the queue!")
	return p.c.put(p.Conn)
}

// newConn wraps a standard net.Conn to a poolConn net.Conn.
func (c *channelPool) wrapConn(conn net.Conn) net.Conn {
	p := poolConn{c: c}
	p.Conn = conn
	return p
}
