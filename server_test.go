package influxdb_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/messaging"
)

// Ensure the server can be successfully opened and closed.
func TestServer_Open(t *testing.T) {
	c := NewMessagingClient()
	s := NewServer(c)

	defer s.Close()

	// TODO
}

// Server is a wrapping test struct for influxdb.Server.
type Server struct {
	*influxdb.Server
}

// NewServer returns a new test server instance.
func NewServer(client influxdb.MessagingClient) *Server {
	return &Server{influxdb.NewServer(client)}
}

// OpenServer returns a new, open test server instance.
func OpenServer(client influxdb.MessagingClient) *Server {
	s := NewServer(client)
	if err := s.Open(tempfile()); err != nil {
		panic(err.Error())
	}
	return s
}

// Close shuts down the server and removes all temporary files.
func (s *Server) Close() {
	defer os.RemoveAll(s.Path())
	s.Server.Close()
}

// MessagingClient represents a test client for the messaging broker.
type MessagingClient struct {
	c chan *messaging.Message
}

// NewMessagingClient returns a new instance of MessagingClient.
func NewMessagingClient() *MessagingClient {
	return &MessagingClient{c: make(chan *messaging.Message, 1)}
}

// Returns a channel for streaming message.
func (c *MessagingClient) C() <-chan *messaging.Message { return c.c }

// tempfile returns a temporary path.
func tempfile() string {
	f, _ := ioutil.TempFile("", "influxdb-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }
