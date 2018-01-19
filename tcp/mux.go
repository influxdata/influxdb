// Package tcp provides a simple multiplexer over TCP.
package tcp // import "github.com/influxdata/influxdb/tcp"

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

const (
	// DefaultTimeout is the default length of time to wait for first byte.
	DefaultTimeout = 30 * time.Second
)

// Mux multiplexes a network connection.
type Mux struct {
	mu sync.RWMutex
	ln net.Listener
	m  map[byte]*listener

	defaultListener *listener

	wg sync.WaitGroup

	// The amount of time to wait for the first header byte.
	Timeout time.Duration

	// Out-of-band error logger
	Logger *log.Logger
}

type replayConn struct {
	net.Conn
	firstByte     byte
	readFirstbyte bool
}

func (rc *replayConn) Read(b []byte) (int, error) {
	if rc.readFirstbyte {
		return rc.Conn.Read(b)
	}

	if len(b) == 0 {
		return 0, nil
	}

	b[0] = rc.firstByte
	rc.readFirstbyte = true
	return 1, nil
}

// NewMux returns a new instance of Mux.
func NewMux() *Mux {
	return &Mux{
		m:       make(map[byte]*listener),
		Timeout: DefaultTimeout,
		Logger:  log.New(os.Stderr, "[tcp] ", log.LstdFlags),
	}
}

// Serve handles connections from ln and multiplexes then across registered listeners.
func (mux *Mux) Serve(ln net.Listener) error {
	mux.mu.Lock()
	mux.ln = ln
	mux.mu.Unlock()
	for {
		// Wait for the next connection.
		// If it returns a temporary error then simply retry.
		// If it returns any other error then exit immediately.
		conn, err := ln.Accept()
		if err, ok := err.(interface {
			Temporary() bool
		}); ok && err.Temporary() {
			continue
		}
		if err != nil {
			// Wait for all connections to be demux
			mux.wg.Wait()

			// Concurrently close all registered listeners.
			// Because mux.m is keyed by byte, in the worst case we would spawn 256 goroutines here.
			var wg sync.WaitGroup
			mux.mu.RLock()
			for _, ln := range mux.m {
				wg.Add(1)
				go func(ln *listener) {
					defer wg.Done()
					ln.Close()
				}(ln)
			}
			mux.mu.RUnlock()
			wg.Wait()

			mux.mu.RLock()
			dl := mux.defaultListener
			mux.mu.RUnlock()
			if dl != nil {
				dl.Close()
			}

			return err
		}

		// Demux in a goroutine to
		mux.wg.Add(1)
		go mux.handleConn(conn)
	}
}

func (mux *Mux) handleConn(conn net.Conn) {
	defer mux.wg.Done()
	// Set a read deadline so connections with no data don't timeout.
	if err := conn.SetReadDeadline(time.Now().Add(mux.Timeout)); err != nil {
		conn.Close()
		mux.Logger.Printf("tcp.Mux: cannot set read deadline: %s", err)
		return
	}

	// Read first byte from connection to determine handler.
	var typ [1]byte
	if _, err := io.ReadFull(conn, typ[:]); err != nil {
		conn.Close()
		mux.Logger.Printf("tcp.Mux: cannot read header byte: %s", err)
		return
	}

	// Reset read deadline and let the listener handle that.
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		conn.Close()
		mux.Logger.Printf("tcp.Mux: cannot reset set read deadline: %s", err)
		return
	}

	// Retrieve handler based on first byte.
	mux.mu.RLock()
	handler := mux.m[typ[0]]
	mux.mu.RUnlock()

	if handler == nil {
		if mux.defaultListener == nil {
			conn.Close()
			mux.Logger.Printf("tcp.Mux: handler not registered: %d. Connection from %s closed", typ[0], conn.RemoteAddr())
			return
		}

		conn = &replayConn{
			Conn:      conn,
			firstByte: typ[0],
		}
		handler = mux.defaultListener
	}

	handler.HandleConn(conn, typ[0])
}

// Listen returns a listener identified by header.
// Any connection accepted by mux is multiplexed based on the initial header byte.
func (mux *Mux) Listen(header byte) net.Listener {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	// Ensure two listeners are not created for the same header byte.
	if _, ok := mux.m[header]; ok {
		panic(fmt.Sprintf("listener already registered under header byte: %d", header))
	}

	// Create a new listener and assign it.
	ln := &listener{
		c:    make(chan net.Conn),
		done: make(chan struct{}),
		mux:  mux,
	}
	mux.m[header] = ln

	return ln
}

// release removes the listener from the mux.
func (mux *Mux) release(ln *listener) bool {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	for b, l := range mux.m {
		if l == ln {
			delete(mux.m, b)
			return true
		}
	}
	return false
}

// DefaultListener will return a net.Listener that will pass-through any
// connections with non-registered values for the first byte of the connection.
// The connections returned from this listener's Accept() method will replay the
// first byte of the connection as a short first Read().
//
// This can be used to pass to an HTTP server, so long as there are no conflicts
// with registered listener bytes and the first character of the HTTP request:
// 71 ('G') for GET, etc.
func (mux *Mux) DefaultListener() net.Listener {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	if mux.defaultListener == nil {
		mux.defaultListener = &listener{
			c:    make(chan net.Conn),
			done: make(chan struct{}),
			mux:  mux,
		}
	}

	return mux.defaultListener
}

// listener is a receiver for connections received by Mux.
type listener struct {
	mux *Mux

	// The done channel is closed before taking a lock on mu to close c.
	// That way, anyone holding an RLock can release the lock by receiving from done.
	done chan struct{}

	mu sync.RWMutex
	c  chan net.Conn
}

// Accept waits for and returns the next connection to the listener.
func (ln *listener) Accept() (net.Conn, error) {
	ln.mu.RLock()
	defer ln.mu.RUnlock()

	select {
	case <-ln.done:
		return nil, errors.New("network connection closed")
	case conn := <-ln.c:
		return conn, nil
	}
}

// Close removes this listener from the parent mux and closes the channel.
func (ln *listener) Close() error {
	if ok := ln.mux.release(ln); ok {
		// Close done to signal to any RLock holders to release their lock.
		close(ln.done)

		// Hold a lock while reassigning ln.c to nil
		// so that attempted sends or receives will block forever.
		ln.mu.Lock()
		ln.c = nil
		ln.mu.Unlock()
	}
	return nil
}

// HandleConn handles the connection, if the listener has not been closed.
func (ln *listener) HandleConn(conn net.Conn, handlerID byte) {
	ln.mu.RLock()
	defer ln.mu.RUnlock()

	// Send connection to handler.  The handler is responsible for closing the connection.
	timer := time.NewTimer(ln.mux.Timeout)
	defer timer.Stop()

	select {
	case <-ln.done:
		// Receive will return immediately if ln.Close has been called.
		conn.Close()
	case ln.c <- conn:
		// Send will block forever if ln.Close has been called.
	case <-timer.C:
		conn.Close()
		ln.mux.Logger.Printf("tcp.Mux: handler not ready: %d. Connection from %s closed", handlerID, conn.RemoteAddr())
		return
	}
}

// Addr returns the Addr of the listener
func (ln *listener) Addr() net.Addr {
	if ln.mux == nil {
		return nil
	}

	ln.mux.mu.RLock()
	defer ln.mux.mu.RUnlock()

	if ln.mux.ln == nil {
		return nil
	}

	return ln.mux.ln.Addr()
}

// Dial connects to a remote mux listener with a given header byte.
func Dial(network, address string, header byte) (net.Conn, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}

	if _, err := conn.Write([]byte{header}); err != nil {
		return nil, fmt.Errorf("write mux header: %s", err)
	}

	return conn, nil
}
