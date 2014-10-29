package influxdb_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

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

// Ensure the server can return a list of all databases.
func TestServer_Databases(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create some databases.
	s.CreateDatabase("foo")
	s.CreateDatabase("bar")

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
}

// Ensure the server can return a list of all admins.
func TestServer_ClusterAdmins(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create some databases.
	s.CreateClusterAdmin("susy", "pass")
	s.CreateClusterAdmin("john", "pass")

	// Return the databases.
	if a := s.ClusterAdmins(); len(a) != 2 {
		t.Fatalf("unexpected admin count: %d", len(a))
	} else if a[0].Name != "john" {
		t.Fatalf("unexpected admin(0): %s", a[0].Name)
	} else if a[1].Name != "susy" {
		t.Fatalf("unexpected admin(1): %s", a[1].Name)
	}
}

// Ensure the server can create a new user.
func TestDatabase_CreateUser(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}
	db := s.Database("foo")

	// Create a user on the database.
	if err := db.CreateUser("susy", "pass", nil); err != nil {
		t.Fatal(err)
	}

	// Verify that the user exists.
	if u := db.User("susy"); u == nil {
		t.Fatalf("user not found")
	} else if u.Name != "susy" {
		t.Fatalf("username mismatch: %v", u.Name)
	} else if bcrypt.CompareHashAndPassword([]byte(u.Hash), []byte("pass")) != nil {
		t.Fatal("invalid password")
	}
}

// Ensure the server returns an error when creating a user without a name.
func TestDatabase_CreateUser_ErrUsernameRequired(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}
	if err := s.Database("foo").CreateUser("", "pass", nil); err != influxdb.ErrUsernameRequired {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when creating a user with an invalid name.
func TestDatabase_CreateUser_ErrInvalidUsername(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}
	if err := s.Database("foo").CreateUser("my%user", "pass", nil); err != influxdb.ErrInvalidUsername {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when creating a user after the db is dropped.
func TestDatabase_CreateUser_ErrDatabaseNotFound(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}
	db := s.Database("foo")

	// Drop the database.
	if err := s.DeleteDatabase("foo"); err != nil {
		t.Fatal(err)
	}

	// Create a user using the old database reference.
	if err := db.CreateUser("susy", "pass", nil); err != influxdb.ErrDatabaseNotFound {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when creating a duplicate user.
func TestDatabase_CreateUser_ErrUserExists(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}
	db := s.Database("foo")

	// Create a user a user. Then create the user again.
	if err := db.CreateUser("susy", "pass", nil); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateUser("susy", "pass", nil); err != influxdb.ErrUserExists {
		t.Fatal(err)
	}
}

// Ensure the server can delete an existing user.
func TestDatabase_DeleteUser(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a database and user.
	s.CreateDatabase("foo")
	db := s.Database("foo")
	if err := db.CreateUser("susy", "pass", nil); err != nil {
		t.Fatal(err)
	} else if db.User("susy") == nil {
		t.Fatal("user not created")
	}

	// Remove user from database.
	if err := db.DeleteUser("susy"); err != nil {
		t.Fatal(err)
	} else if db.User("susy") != nil {
		t.Fatal("user not deleted")
	}
}

// Ensure the server returns an error when delete a user without a name.
func TestDatabase_DeleteUser_ErrUsernameRequired(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	if err := s.Database("foo").DeleteUser(""); err != influxdb.ErrUsernameRequired {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when deleting a user after the db is dropped.
func TestDatabase_DeleteUser_ErrDatabaseNotFound(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create & delete the database.
	s.CreateDatabase("foo")
	db := s.Database("foo")
	s.DeleteDatabase("foo")

	// Delete a user using the old database reference.
	if err := db.DeleteUser("susy"); err != influxdb.ErrDatabaseNotFound {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when deleting a non-existent user.
func TestDatabase_DeleteUser_ErrUserNotFound(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	if err := s.Database("foo").DeleteUser("no_such_user"); err != influxdb.ErrUserNotFound {
		t.Fatal(err)
	}
}

// Ensure the server can change the password of a user.
func TestDatabase_ChangePassword(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a database and user.
	s.CreateDatabase("foo")
	db := s.Database("foo")
	if err := db.CreateUser("susy", "pass", nil); err != nil {
		t.Fatal(err)
	} else if bcrypt.CompareHashAndPassword([]byte(db.User("susy").Hash), []byte("pass")) != nil {
		t.Fatal("invalid initial password")
	}

	// Update user password.
	if err := db.ChangePassword("susy", "newpass"); err != nil {
		t.Fatal(err)
	} else if bcrypt.CompareHashAndPassword([]byte(db.User("susy").Hash), []byte("newpass")) != nil {
		t.Fatal("invalid new password")
	}
}

// Ensure the database can return a list of all users.
func TestDatabase_Users(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create two databases with users.
	s.CreateDatabase("foo")
	s.Database("foo").CreateUser("susy", "pass", nil)
	s.Database("foo").CreateUser("john", "pass", nil)
	s.CreateDatabase("bar")
	s.Database("bar").CreateUser("jimmy", "pass", nil)

	// Retrieve a list of all users for "foo" (sorted by name).
	if a := s.Database("foo").Users(); len(a) != 2 {
		t.Fatalf("unexpected user count: %d", len(a))
	} else if a[0].Name != "john" {
		t.Fatalf("unexpected user(0): %s", a[0].Name)
	} else if a[1].Name != "susy" {
		t.Fatalf("unexpected user(1): %s", a[1].Name)
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
