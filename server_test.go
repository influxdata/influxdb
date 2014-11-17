package influxdb_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"code.google.com/p/go.crypto/bcrypt"
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

// Ensure an error is returned when opening an already open server.
func TestServer_Open_ErrServerOpen(t *testing.T) { t.Skip("pending") }

// Ensure an error is returned when opening a server without a path.
func TestServer_Open_ErrPathRequired(t *testing.T) { t.Skip("pending") }

// Ensure the server can create a database.
func TestServer_CreateDatabase(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create the "foo" database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}
	s.Restart()

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
	s.Restart()

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

// Ensure the server can return a list of all databases.
func TestServer_Databases(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create some databases.
	s.CreateDatabase("foo")
	s.CreateDatabase("bar")
	s.Restart()

	// Return the databases.
	if a := s.Databases(); len(a) != 2 {
		t.Fatalf("unexpected db count: %d", len(a))
	} else if a[0].Name() != "bar" {
		t.Fatalf("unexpected db(0): %s", a[0].Name())
	} else if a[1].Name() != "foo" {
		t.Fatalf("unexpected db(1): %s", a[1].Name())
	}
}

// Ensure the server can create a new cluster admin.
func TestServer_CreateClusterAdmin(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a cluster admin.
	if err := s.CreateClusterAdmin("susy", "pass"); err != nil {
		t.Fatal(err)
	}
	s.Restart()

	// Verify that the admin exists.
	if u := s.ClusterAdmin("susy"); u == nil {
		t.Fatalf("admin not found")
	} else if u.Name != "susy" {
		t.Fatalf("username mismatch: %v", u.Name)
	} else if bcrypt.CompareHashAndPassword([]byte(u.Hash), []byte("pass")) != nil {
		t.Fatal("invalid password")
	}
}

// Ensure the server returns an error when creating an admin without a name.
func TestServer_CreateClusterAdmin_ErrUsernameRequired(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	if err := s.CreateClusterAdmin("", "pass"); err != influxdb.ErrUsernameRequired {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when creating a duplicate admin.
func TestServer_CreateClusterAdmin_ErrClusterAdminExists(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	if err := s.CreateClusterAdmin("susy", "pass"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateClusterAdmin("susy", "pass"); err != influxdb.ErrClusterAdminExists {
		t.Fatal(err)
	}
}

// Ensure the server can delete an existing cluster admin.
func TestServer_DeleteClusterAdmin(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a cluster admin.
	if err := s.CreateClusterAdmin("susy", "pass"); err != nil {
		t.Fatal(err)
	} else if s.ClusterAdmin("susy") == nil {
		t.Fatalf("admin not created")
	}

	// Delete the cluster admin.
	if err := s.DeleteClusterAdmin("susy"); err != nil {
		t.Fatal(err)
	} else if s.ClusterAdmin("susy") != nil {
		t.Fatalf("admin not actually deleted")
	}
	s.Restart()

	if s.ClusterAdmin("susy") != nil {
		t.Fatalf("admin not actually deleted after restart")
	}
}

// Ensure the server can return a list of all admins.
func TestServer_ClusterAdmins(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create some databases.
	s.CreateClusterAdmin("susy", "pass")
	s.CreateClusterAdmin("john", "pass")
	s.Restart()

	// Return the databases.
	if a := s.ClusterAdmins(); len(a) != 2 {
		t.Fatalf("unexpected admin count: %d", len(a))
	} else if a[0].Name != "john" {
		t.Fatalf("unexpected admin(0): %s", a[0].Name)
	} else if a[1].Name != "susy" {
		t.Fatalf("unexpected admin(1): %s", a[1].Name)
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

// Restart stops and restarts the server.
func (s *Server) Restart() {
	path := s.Path()
	if err := s.Server.Close(); err != nil {
		panic("close: " + err.Error())
	}
	if err := s.Server.Open(path); err != nil {
		panic("open: " + err.Error())
	}
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

// mustParseTime parses an IS0-8601 string. Panic on error.
func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err.Error())
	}
	return t
}

// mustParseMicroTime parses an IS0-8601 string into microseconds since epoch.
// Panic on error.
func mustParseMicroTime(s string) int64 {
	return mustParseTime(s).UnixNano() / int64(time.Microsecond)
}

// errstr is an ease-of-use function to convert an error to a string.
func errstr(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }
