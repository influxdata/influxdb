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
	if err := s.Server.Open(tempfile()); err != nil {
		t.Fatal(err)
	}
	if err := s.Server.Close(); err != nil {
		t.Fatal(err)
	}
}

// Ensure the server can create a database.
func TestServer_CreateDatabase(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create the "foo" database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}

	// Verify that the database exists.
	if d := s.Database("foo"); d == nil {
		t.Fatalf("database not found")
	} else if d.Name() != "foo" {
		t.Fatalf("name mismatch: %s", d.Name())
	}
}

// Ensure the server returns an error when creating a duplicate database.
func TestServer_CreateDatabase_ErrDatabaseExists(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create the "foo" database twice.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateDatabase("foo"); err != influxdb.ErrDatabaseExists {
		t.Fatal(err)
	}
}

// Ensure the server can drop a database.
func TestServer_DropDatabase(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create the "foo" database and verify it exists.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	} else if d := s.Database("foo"); d == nil {
		t.Fatalf("database not actually created")
	}

	// Drop the "foo" database and verify that it's gone.
	if err := s.DeleteDatabase("foo"); err != nil {
		t.Fatal(err)
	} else if d := s.Database("foo"); d != nil {
		t.Fatalf("database not actually dropped")
	}
}

// Ensure the server returns an error when dropping a database that doesn't exist.
func TestServer_DropDatabase_ErrDatabaseNotFound(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Drop a database that doesn't exist.
	if err := s.DeleteDatabase("no_such_db"); err != influxdb.ErrDatabaseNotFound {
		t.Fatal(err)
	}
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
	index uint64
	c     chan *messaging.Message

	PublishFunc func(*messaging.Message) (uint64, error)
}

// NewMessagingClient returns a new instance of MessagingClient.
func NewMessagingClient() *MessagingClient {
	c := &MessagingClient{c: make(chan *messaging.Message, 1)}
	c.PublishFunc = c.send
	return c
}

// Publish attaches an autoincrementing index to the message.
// This function also execute's the client's PublishFunc mock function.
func (c *MessagingClient) Publish(m *messaging.Message) (uint64, error) {
	c.index++
	m.Index = c.index
	return c.PublishFunc(m)
}

// send sends the message through to the channel.
// This is the default value of PublishFunc.
func (c *MessagingClient) send(m *messaging.Message) (uint64, error) {
	c.c <- m
	return m.Index, nil
}

// C returns a channel for streaming message.
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
