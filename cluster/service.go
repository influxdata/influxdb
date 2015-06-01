package cluster

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/influxdb/influxdb/tsdb"
)

// Service processes data received over raw TCP connections.
type Service struct {
	mu   sync.RWMutex
	addr string
	ln   net.Listener

	wg      sync.WaitGroup
	closing chan struct{}

	ShardWriter interface {
		WriteShard(shardID, ownerID uint64, points []tsdb.Point) error
	}

	Logger *log.Logger
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	return &Service{
		addr:    c.BindAddress,
		closing: make(chan struct{}),
		Logger:  log.New(os.Stderr, "[tcp] ", log.LstdFlags),
	}
}

// Open opens the network listener and begins serving requests.
func (s *Service) Open() error {
	// Open TCP listener.
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln

	s.Logger.Println("listening on TCP:", ln.Addr().String())

	// Begin serving conections.
	s.wg.Add(1)
	go s.serve()

	return nil
}

// serve accepts connections from the listener and handles them.
func (s *Service) serve() {
	defer s.wg.Done()

	for {
		// Check if the service is shutting down.
		select {
		case <-s.closing:
			return
		default:
		}

		// Accept the next connection.
		conn, err := s.ln.Accept()
		if opErr, ok := err.(*net.OpError); ok && opErr.Temporary() {
			s.Logger.Println("error temporarily accepting TCP connection", err.Error())
			continue
		} else if err != nil {
			return
		}

		// Delegate connection handling to a separate goroutine.
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
}

// Close shuts down the listener and waits for all connections to finish.
func (s *Service) Close() error {
	if s.ln != nil {
		s.ln.Close()
	}

	// Shut down all handlers.
	close(s.closing)
	s.wg.Wait()

	return nil
}

// Addr returns the network address of the service.
func (s *Service) Addr() net.Addr {
	if s.ln != nil {
		return s.ln.Addr()
	}
	return nil
}

// handleConn services an individual TCP connection.
func (s *Service) handleConn(conn net.Conn) {
	// Ensure connection is closed when service is closed.
	closing := make(chan struct{})
	defer close(closing)
	go func() {
		select {
		case <-closing:
		case <-s.closing:
		}
		conn.Close()
	}()

	for {
		// Read type-length-value.
		typ, buf, err := ReadTLV(conn)
		if err != nil && strings.Contains(err.Error(), "closed network connection") {
			return
		} else if err != nil {
			s.Logger.Printf("unable to read type-length-value %s", err)
			return
		}

		// Delegate message processing by type.
		switch typ {
		case writeShardRequestMessage:
			err := s.processWriteShardRequest(buf)
			s.writeShardResponse(conn, err)
		}
	}
}

func (s *Service) processWriteShardRequest(buf []byte) error {
	// Build request
	var req WriteShardRequest
	if err := req.UnmarshalBinary(buf); err != nil {
		return err
	}

	if err := s.ShardWriter.WriteShard(req.ShardID(), req.OwnerID(), req.Points()); err != nil {
		return fmt.Errorf("write shard: %s", err)
	}

	return nil
}

func (s *Service) writeShardResponse(w io.Writer, e error) {
	// Build response.
	var resp WriteShardResponse
	if e != nil {
		resp.SetCode(1)
		resp.SetMessage(e.Error())
	} else {
		resp.SetCode(0)
	}

	// Marshal response to binary.
	buf, err := resp.MarshalBinary()
	if err != nil {
		s.Logger.Printf("error marshalling shard response: %s", err)
		return
	}

	// Write to connection.
	if err := WriteTLV(w, writeShardResponseMessage, buf); err != nil {
		s.Logger.Printf("write shard response error: %s", err)
	}
}

// ReadTLV reads a type-length-value record from r.
func ReadTLV(r io.Reader) (byte, []byte, error) {
	var typ [1]byte
	if _, err := io.ReadFull(r, typ[:]); err != nil {
		return 0, nil, fmt.Errorf("read message type: %s", err)
	}

	// Read the size of the message.
	var sz int64
	if err := binary.Read(r, binary.BigEndian, &sz); err != nil {
		return 0, nil, fmt.Errorf("read message size: %s", err)
	}

	// Read the value.
	buf := make([]byte, sz)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, fmt.Errorf("read message value: %s", err)
	}

	return typ[0], buf, nil
}

// WriteTLV writes a type-length-value record to w.
func WriteTLV(w io.Writer, typ byte, buf []byte) error {
	if _, err := w.Write([]byte{typ}); err != nil {
		return fmt.Errorf("write message type: %s", err)
	}

	// Write the size of the message.
	if err := binary.Write(w, binary.BigEndian, int64(len(buf))); err != nil {
		return fmt.Errorf("write message size: %s", err)
	}

	// Write the value.
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("write message value: %s", err)
	}

	return nil
}
