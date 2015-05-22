package tcp

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"sync"

	"github.com/influxdb/influxdb/data"
)

var (
	// ErrBindAddressRequired is returned when starting the Server
	// without a TCP or UDP listening address.
	ErrBindAddressRequired = errors.New("bind address required")

	// ErrServerClosed return when closing an already closed graphite server.
	ErrServerClosed = errors.New("server already closed")

	// ErrServerNotSpecified returned when Server is not specified.
	ErrServerNotSpecified = errors.New("server not present")
)

// Server processes data received over raw TCP connections.
type Server struct {
	writer   data.ShardWriter
	listener *net.Listener

	wg sync.WaitGroup

	Logger *log.Logger

	shutdown chan struct{}
}

// NewServer returns a new instance of a Server.
func NewServer(w data.ShardWriter) *Server {
	return &Server{
		writer:   w,
		Logger:   log.New(os.Stderr, "[tcp] ", log.LstdFlags),
		shutdown: make(chan struct{}),
	}
}

// ListenAndServe instructs the Server to start processing connections
// on the given interface. iface must be in the form host:port
// If successful, it returns the host as the first argument
func (s *Server) ListenAndServe(laddr string) (string, error) {
	if laddr == "" { // Make sure we have an laddr
		return "", ErrBindAddressRequired
	}

	ln, err := net.Listen("tcp", laddr)
	if err != nil {
		return "", err
	}
	s.listener = &ln

	s.Logger.Println("listening on TCP connection", ln.Addr().String())
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		for {
			// Are we shutting down? If so, exit
			select {
			case <-s.shutdown:
				return
			default:
			}

			conn, err := ln.Accept()
			if opErr, ok := err.(*net.OpError); ok && !opErr.Temporary() {
				s.Logger.Println("error temporarily accepting TCP connection", err.Error())
				continue
			}
			if err != nil {
				s.Logger.Println("TCP listener closed")
				return
			}

			s.wg.Add(1)
			go s.handleConnection(conn)
		}
	}()

	// Return the host we started up on. Mostly needed for testing
	return ln.Addr().String(), nil
}

// Close will close the listener
func (s *Server) Close() error {
	// Stop accepting client connections
	if s.listener != nil {
		err := (*s.listener).Close()
		if err != nil {
			return err
		}
	} else {
		return ErrServerClosed
	}
	// Shut down all handlers
	close(s.shutdown)
	s.wg.Wait()
	s.listener = nil

	return nil
}

// handleConnection services an individual TCP connection.
func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		s.wg.Done()
	}()

	messageChannel := make(chan byte)

	// Start our reader up in a go routine so we don't block checking our close channel
	go func() {
		var messageType byte
		err := binary.Read(conn, binary.LittleEndian, &messageType)
		if err != nil {
			s.Logger.Printf("unable to read message type %s", err)
			return
		}
		messageChannel <- messageType
	}()

	for {
		select {
		case <-s.shutdown:
			// Are we shutting down? If so, exit
			return
		case messageType := <-messageChannel:
			switch messageType {
			case writeShardRequestMessage:
				if err := s.WriteShardRequest(conn); err != nil {
					s.WriteShardResponse(conn, err)
					return
				}
				s.WriteShardResponse(conn, nil)
			}
		default:
		}
	}

}

func (s *Server) WriteShardRequest(conn net.Conn) error {
	messageChannel := make(chan data.WriteShardRequest)
	errChan := make(chan error)

	go func() {
		var size int64
		if err := binary.Read(conn, binary.LittleEndian, &size); err != nil {
			errChan <- err
			return
		}

		message := make([]byte, size)

		reader := io.LimitReader(conn, size)
		_, err := reader.Read(message)
		if err != nil {
			errChan <- err
			return
		}
		var wsr data.WriteShardRequest
		if err := wsr.UnmarshalBinary(message); err != nil {
			errChan <- err
			return
		}
		messageChannel <- wsr
	}()

	for {
		select {
		case <-s.shutdown:
			// Are we shutting down? If so, exit
			return nil
		case e := <-errChan:
			return e
		case wsr := <-messageChannel:
			if _, err := s.writer.WriteShard(wsr.ShardID(), wsr.Points()); err != nil {
				return err
			}
			return nil
		default:
		}
	}
}

func (s *Server) WriteShardResponse(conn net.Conn, e error) {
	s.Logger.Println("writing shard response")
	var mt byte = writeShardResponseMessage
	if err := binary.Write(conn, binary.LittleEndian, &mt); err != nil {
		s.Logger.Printf("error writing shard response message type: %s", err)
		return
	}

	var wsr data.WriteShardResponse
	if e != nil {
		wsr.SetCode(1)
		wsr.SetMessage(e.Error())
	} else {
		wsr.SetCode(0)
	}

	b, err := wsr.MarshalBinary()
	if err != nil {
		s.Logger.Printf("error marshalling shard response: %s", err)
		return
	}

	size := int64(len(b))

	if err := binary.Write(conn, binary.LittleEndian, &size); err != nil {
		s.Logger.Printf("error writing shard response length: %s", err)
		return
	}

	if _, err := conn.Write(b); err != nil {
		s.Logger.Printf("error writing shard response: %s", err)
		return
	}
}
