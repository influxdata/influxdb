package influxdb_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
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
	if !s.DatabaseExists("foo") {
		t.Fatalf("database not found")
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
	} else if !s.DatabaseExists("foo") {
		t.Fatalf("database not actually created")
	}
	s.Restart()

	// Drop the "foo" database and verify that it's gone.
	if err := s.DeleteDatabase("foo"); err != nil {
		t.Fatal(err)
	} else if s.DatabaseExists("foo") {
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
	} else if a[0] != "bar" {
		t.Fatalf("unexpected db(0): %s", a[0])
	} else if a[1] != "foo" {
		t.Fatalf("unexpected db(1): %s", a[1])
	}
}

// Ensure the server can create a new user.
func TestServer_CreateUser(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a user.
	if err := s.CreateUser("susy", "pass", true); err != nil {
		t.Fatal(err)
	}
	s.Restart()

	// Verify that the user exists.
	if u := s.User("susy"); u == nil {
		t.Fatalf("user not found")
	} else if u.Name != "susy" {
		t.Fatalf("username mismatch: %v", u.Name)
	} else if !u.Admin {
		t.Fatalf("admin mismatch: %v", u.Admin)
	} else if bcrypt.CompareHashAndPassword([]byte(u.Hash), []byte("pass")) != nil {
		t.Fatal("invalid password")
	}
}

// Ensure the server returns an error when creating an user without a name.
func TestServer_CreateUser_ErrUsernameRequired(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	if err := s.CreateUser("", "pass", false); err != influxdb.ErrUsernameRequired {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when creating a duplicate user.
func TestServer_CreateUser_ErrUserExists(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	if err := s.CreateUser("susy", "pass", false); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateUser("susy", "pass", false); err != influxdb.ErrUserExists {
		t.Fatal(err)
	}
}

// Ensure the server can delete an existing user.
func TestServer_DeleteUser(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a user.
	if err := s.CreateUser("susy", "pass", false); err != nil {
		t.Fatal(err)
	} else if s.User("susy") == nil {
		t.Fatalf("user not created")
	}

	// Delete the user.
	if err := s.DeleteUser("susy"); err != nil {
		t.Fatal(err)
	} else if s.User("susy") != nil {
		t.Fatalf("user not actually deleted")
	}
	s.Restart()

	if s.User("susy") != nil {
		t.Fatalf("user not actually deleted after restart")
	}
}

// Ensure the server can return a list of all users.
func TestServer_Users(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create some databases.
	s.CreateUser("susy", "pass", false)
	s.CreateUser("john", "pass", false)
	s.Restart()

	// Return the databases.
	if a := s.Users(); len(a) != 2 {
		t.Fatalf("unexpected user count: %d", len(a))
	} else if a[0].Name != "john" {
		t.Fatalf("unexpected user(0): %s", a[0].Name)
	} else if a[1].Name != "susy" {
		t.Fatalf("unexpected user(1): %s", a[1].Name)
	}
}

// Ensure the database can create a new retention policy.
func TestServer_CreateRetentionPolicy(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}

	// Create a retention policy on the database.
	rp := &influxdb.RetentionPolicy{
		Name:     "bar",
		Duration: time.Hour,
		ReplicaN: 2,
		SplitN:   3,
	}
	if err := s.CreateRetentionPolicy("foo", rp); err != nil {
		t.Fatal(err)
	}
	s.Restart()

	// Verify that the policy exists.
	if o, err := s.RetentionPolicy("foo", "bar"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if o == nil {
		t.Fatalf("retention policy not found")
	} else if !reflect.DeepEqual(rp, o) {
		t.Fatalf("retention policy mismatch: %#v", o)
	}
}

// Ensure the server returns an error when creating a retention policy with an invalid db.
func TestServer_CreateRetentionPolicy_ErrDatabaseNotFound(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	if err := s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "bar"}); err != influxdb.ErrDatabaseNotFound {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when creating a retention policy without a name.
func TestServer_CreateRetentionPolicy_ErrRetentionPolicyNameRequired(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	if err := s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: ""}); err != influxdb.ErrRetentionPolicyNameRequired {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when creating a duplicate retention policy.
func TestServer_CreateRetentionPolicy_ErrRetentionPolicyExists(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "bar"})
	if err := s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "bar"}); err != influxdb.ErrRetentionPolicyExists {
		t.Fatal(err)
	}
}

// Ensure the server can delete an existing retention policy.
func TestServer_DeleteRetentionPolicy(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a database and retention policy.
	s.CreateDatabase("foo")
	if err := s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "bar"}); err != nil {
		t.Fatal(err)
	} else if rp, _ := s.RetentionPolicy("foo", "bar"); rp == nil {
		t.Fatal("retention policy not created")
	}

	// Remove retention policy from database.
	if err := s.DeleteRetentionPolicy("foo", "bar"); err != nil {
		t.Fatal(err)
	} else if rp, _ := s.RetentionPolicy("foo", "bar"); rp != nil {
		t.Fatal("retention policy not deleted")
	}
	s.Restart()

	if rp, _ := s.RetentionPolicy("foo", "bar"); rp != nil {
		t.Fatal("retention policy not deleted after restart")
	}
}

// Ensure the server returns an error when deleting a retention policy on invalid db.
func TestServer_DeleteRetentionPolicy_ErrDatabaseNotFound(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	if err := s.DeleteRetentionPolicy("foo", "bar"); err != influxdb.ErrDatabaseNotFound {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when deleting a retention policy without a name.
func TestServer_DeleteRetentionPolicy_ErrRetentionPolicyNameRequired(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	if err := s.DeleteRetentionPolicy("foo", ""); err != influxdb.ErrRetentionPolicyNameRequired {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when deleting a non-existent retention policy.
func TestServer_DeleteRetentionPolicy_ErrRetentionPolicyNotFound(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	if err := s.DeleteRetentionPolicy("foo", "no_such_policy"); err != influxdb.ErrRetentionPolicyNotFound {
		t.Fatal(err)
	}
}

// Ensure the server can set the default retention policy
func TestServer_SetDefaultRetentionPolicy(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")

	rp := &influxdb.RetentionPolicy{Name: "bar"}
	if err := s.CreateRetentionPolicy("foo", rp); err != nil {
		t.Fatal(err)
	} else if rp, _ := s.RetentionPolicy("foo", "bar"); rp == nil {
		t.Fatal("retention policy not created")
	}

	// Set bar as default
	if err := s.SetDefaultRetentionPolicy("foo", "bar"); err != nil {
		t.Fatal(err)
	}

	if o, _ := s.DefaultRetentionPolicy("foo"); o == nil {
		t.Fatal("default policy not set")
	} else if !reflect.DeepEqual(rp, o) {
		t.Fatalf("retention policy mismatch: %#v", o)
	}

	s.Restart()

	if o, _ := s.DefaultRetentionPolicy("foo"); o == nil {
		t.Fatal("default policy not kept after restart")
	} else if !reflect.DeepEqual(rp, o) {
		t.Fatalf("retention policy mismatch after restart: %#v", o)
	}
}

// Ensure the server returns an error when setting the deafult retention policy to a non-existant one.
func TestServer_SetDefaultRetentionPolicy_ErrRetentionPolicyNotFound(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	if err := s.SetDefaultRetentionPolicy("foo", "no_such_policy"); err != influxdb.ErrRetentionPolicyNotFound {
		t.Fatal(err)
	}
}

// Ensure the database can write data to the database.
func TestServer_WriteSeries(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "myspace", Duration: 1 * time.Hour})
	s.CreateUser("susy", "pass", false)

	// Write series with one point to the database.
	timestamp := mustParseTime("2000-01-01T00:00:00Z")
	name := "cpu_load"
	tags := map[string]string{"host": "servera.influx.com", "region": "uswest"}
	values := map[string]interface{}{"value": 23.2}

	if err := s.WriteSeries("foo", "myspace", name, tags, timestamp, values); err != nil {
		t.Fatal(err)
	}

	t.Skip("pending")

	// TODO: Execute a query and record all series found.
	// q := mustParseQuery(`select myval from cpu_load`)
	// if err := db.ExecuteQuery(q); err != nil {
	// 	t.Fatal(err)
	// }
}

func TestServer_CreateShardIfNotExist(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")

	if err := s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "bar"}); err != nil {
		t.Fatal(err)
	}

	if err := s.CreateShardsIfNotExists("foo", "bar", time.Time{}); err != nil {
		t.Fatal(err)
	}

	if ss, err := s.Shards("foo"); err != nil {
		t.Fatal(err)
	} else if len(ss) != 1 {
		t.Fatalf("expected 1 shard but found %d", len(ss))
	}
}

func TestServer_Measurements(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "myspace", Duration: 1 * time.Hour})
	s.CreateUser("susy", "pass", false)

	// Write series with one point to the database.
	timestamp := mustParseTime("2000-01-01T00:00:00Z")

	tags := map[string]string{"host": "servera.influx.com", "region": "uswest"}
	values := map[string]interface{}{"value": 23.2}

	if err := s.WriteSeries("foo", "myspace", "cpu_load", tags, timestamp, values); err != nil {
		t.Fatal(err)
	}

	r := s.Measurements("foo")
	m := []*influxdb.Measurement{
		&influxdb.Measurement{
			Name: "cpu_load",
			Series: []*influxdb.Series{
				&influxdb.Series{
					ID:   uint32(1),
					Tags: map[string]string{"host": "servera.influx.com", "region": "uswest"}}}}}
	if !measurementsEqual(r, m) {
		t.Fatalf("Mesurements not the same:\n%s\n%s", r, m)
	}
}

func measurementsEqual(l influxdb.Measurements, r influxdb.Measurements) bool {
	if len(l) != len(r) {
		return false
	}
	for i, ll := range l {
		if !reflect.DeepEqual(ll, r[i]) {
			return false
		}
	}
	return true
}

func TestServer_SeriesByTagNames(t *testing.T)  { t.Skip("pending") }
func TestServer_SeriesByTagValues(t *testing.T) { t.Skip("pending") }
func TestDatabase_TagNames(t *testing.T)        { t.Skip("pending") }
func TestServer_TagNamesBySeries(t *testing.T)  { t.Skip("pending") }
func TestServer_TagValues(t *testing.T)         { t.Skip("pending") }
func TestServer_TagValuesBySeries(t *testing.T) { t.Skip("pending") }

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

// errstr is an ease-of-use function to convert an error to a string.
func errstr(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }
