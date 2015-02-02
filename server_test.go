package influxdb_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"code.google.com/p/go.crypto/bcrypt"
	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/messaging"
)

// Ensure the server can be successfully opened and closed.
func TestServer_Open(t *testing.T) {
	s := NewServer()
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

// Ensure the server can create a new data node.
func TestServer_CreateDataNode(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a new node.
	u, _ := url.Parse("http://localhost:80000")
	if err := s.CreateDataNode(u); err != nil {
		t.Fatal(err)
	}
	s.Restart()

	// Verify that the node exists.
	if n := s.DataNodeByURL(u); n == nil {
		t.Fatalf("data node not found")
	} else if n.URL.String() != "http://localhost:80000" {
		t.Fatalf("unexpected url: %s", n.URL)
	} else if n.ID == 0 {
		t.Fatalf("unexpected id: %d", n.ID)
	}
}

// Ensure the server returns an error when creating a duplicate node.
func TestServer_CreateDatabase_ErrDataNodeExists(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a node with the same URL twice.
	u, _ := url.Parse("http://localhost:80000")
	if err := s.CreateDataNode(u); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateDataNode(u); err != influxdb.ErrDataNodeExists {
		t.Fatal(err)
	}
}

// Ensure the server can delete a node.
func TestServer_DeleteDataNode(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a data node and verify it exists.
	u, _ := url.Parse("http://localhost:80000")
	if err := s.CreateDataNode(u); err != nil {
		t.Fatal(err)
	} else if s.DataNodeByURL(u) == nil {
		t.Fatalf("data node not actually created")
	}
	s.Restart()

	// Drop the node and verify that it's gone.
	n := s.DataNodeByURL(u)
	if err := s.DeleteDataNode(n.ID); err != nil {
		t.Fatal(err)
	} else if s.DataNode(n.ID) != nil {
		t.Fatalf("data node not actually dropped")
	}
}

// Test user privilege authorization.
func TestServer_UserPrivilegeAuthorization(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create cluster admin.
	s.CreateUser("admin", "admin", true)
	admin := s.User("admin")

	// Create normal database user.
	s.CreateUser("user1", "user1", false)
	user1 := s.User("user1")
	user1.Privileges["foo"] = influxql.ReadPrivilege

	s.Restart()

	// admin user should be authorized for all privileges.
	if !admin.Authorize(influxql.AllPrivileges, "") {
		t.Fatalf("cluster admin doesn't have influxql.AllPrivileges")
	} else if !admin.Authorize(influxql.WritePrivilege, "") {
		t.Fatalf("cluster admin doesn't have influxql.WritePrivilege")
	}

	// Normal user with only read privilege on database foo.
	if !user1.Authorize(influxql.ReadPrivilege, "foo") {
		t.Fatalf("user1 doesn't have influxql.ReadPrivilege on foo")
	} else if user1.Authorize(influxql.WritePrivilege, "foo") {
		t.Fatalf("user1 has influxql.WritePrivilege on foo")
	} else if user1.Authorize(influxql.ReadPrivilege, "bar") {
		t.Fatalf("user1 has influxql.ReadPrivilege on bar")
	} else if user1.Authorize(influxql.AllPrivileges, "") {
		t.Fatalf("user1 is cluster admin")
	}
}

// Test single statement query authorization.
func TestServer_SingleStatementQueryAuthorization(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create cluster admin.
	s.CreateUser("admin", "admin", true)
	admin := s.User("admin")

	// Create normal database user.
	s.CreateUser("user", "user", false)
	user := s.User("user")
	user.Privileges["foo"] = influxql.ReadPrivilege

	s.Restart()

	// Create a query that only cluster admins can run.
	adminOnlyQuery := &influxql.Query{
		Statements: []influxql.Statement{
			&influxql.DropDatabaseStatement{Name: "foo"},
		},
	}

	// Create a query that requires read on one db and write on another.
	readWriteQuery := &influxql.Query{
		Statements: []influxql.Statement{
			&influxql.CreateContinuousQueryStatement{
				Name:     "myquery",
				Database: "foo",
				Source: &influxql.SelectStatement{
					Fields: []*influxql.Field{{Expr: &influxql.Call{Name: "count"}}},
					Target: &influxql.Target{Measurement: "measure1", Database: "bar"},
					Source: &influxql.Measurement{Name: "myseries"},
				},
			},
		},
	}

	// admin user should be authorized to execute any query.
	if err := influxdb.Authorize(admin, adminOnlyQuery, ""); err != nil {
		t.Fatal(err)
	}

	if err := influxdb.Authorize(admin, readWriteQuery, "foo"); err != nil {
		t.Fatal(err)
	}

	// Normal user should not be authorized to execute admin only query.
	if err := influxdb.Authorize(user, adminOnlyQuery, ""); err == nil {
		t.Fatalf("normal user should not be authorized to execute cluster admin level queries")
	}

	// Normal user should not be authorized to execute query that selects into another
	// database which (s)he doesn't have privileges on.
	if err := influxdb.Authorize(user, readWriteQuery, ""); err == nil {
		t.Fatalf("normal user should not be authorized to write to database bar")
	}

	// Grant normal user write privileges on database "bar".
	user.Privileges["bar"] = influxql.WritePrivilege

	//Authorization on the previous query should now succeed.
	if err := influxdb.Authorize(user, readWriteQuery, ""); err != nil {
		t.Fatal(err)
	}
}

// Test multiple statement query authorization.
func TestServer_MultiStatementQueryAuthorization(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create cluster admin.
	s.CreateUser("admin", "admin", true)
	admin := s.User("admin")

	// Create normal database user.
	s.CreateUser("user", "user", false)
	user := s.User("user")
	user.Privileges["foo"] = influxql.ReadPrivilege

	s.Restart()

	// Create a query that requires read for one statement and write for the second.
	readWriteQuery := &influxql.Query{
		Statements: []influxql.Statement{
			// Statement that requires read.
			&influxql.SelectStatement{
				Fields: []*influxql.Field{{Expr: &influxql.Call{Name: "count"}}},
				Source: &influxql.Measurement{Name: "cpu"},
			},

			// Statement that requires write.
			&influxql.SelectStatement{
				Fields: []*influxql.Field{{Expr: &influxql.Call{Name: "count"}}},
				Source: &influxql.Measurement{Name: "cpu"},
				Target: &influxql.Target{Measurement: "tmp"},
			},
		},
	}

	// Admin should be authorized to execute both statements in the query.
	if err := influxdb.Authorize(admin, readWriteQuery, "foo"); err != nil {
		t.Fatal(err)
	}

	// Normal user with only read privileges should not be authorized to execute both statements.
	if err := influxdb.Authorize(user, readWriteQuery, "foo"); err == nil {
		t.Fatalf("user should not be authorized to execute both statements")
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

	// Verify that the authenticated user exists.
	u, err := s.Authenticate("susy", "pass")
	if err != nil {
		t.Fatalf("error fetching authenticated user")
	} else if u.Name != "susy" {
		t.Fatalf("username mismatch: %v", u.Name)
	} else if !u.Admin {
		t.Fatalf("admin mismatch: %v", u.Admin)
	} else if bcrypt.CompareHashAndPassword([]byte(u.Hash), []byte("pass")) != nil {
		t.Fatal("invalid password")
	}

}

// Ensure the server correctly detects when there is an admin user.
func TestServer_AdminUserExists(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// A server should start up without any admin user.
	if s.AdminUserExists() {
		t.Fatalf("admin user unexpectedly exists at start-up")
	}

	// Create a non-admin user and verify Server agrees there is no admin user.
	if err := s.CreateUser("bert", "pass", false); err != nil {
		t.Fatal(err)
	}
	s.Restart()
	if s.AdminUserExists() {
		t.Fatalf("admin user unexpectedly exists")
	}

	// Next, create an admin user, and ensure the Server agrees there is an admin user.
	if err := s.CreateUser("ernie", "pass", true); err != nil {
		t.Fatal(err)
	}
	s.Restart()
	if !s.AdminUserExists() {
		t.Fatalf("admin user does not exist")
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

	// Create some users.
	s.CreateUser("susy", "pass", false)
	s.CreateUser("john", "pass", false)
	s.Restart()

	// Return the users.
	if a := s.Users(); len(a) != 2 {
		t.Fatalf("unexpected user count: %d", len(a))
	} else if a[0].Name != "john" {
		t.Fatalf("unexpected user(0): %s", a[0].Name)
	} else if a[1].Name != "susy" {
		t.Fatalf("unexpected user(1): %s", a[1].Name)
	}
}

// Ensure the server does not return non-existent users
func TestServer_NonExistingUsers(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create some users.
	s.CreateUser("susy", "pass", false)
	s.CreateUser("john", "pass2", false)
	s.Restart()

	// Ask for users that should not be returned.
	u := s.User("bob")
	if u != nil {
		t.Fatalf("unexpected user found")
	}
	u, err := s.Authenticate("susy", "wrong_password")
	if err == nil {
		t.Fatalf("unexpected authenticated user found")
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
	c := NewMessagingClient()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "mypolicy", Duration: 1 * time.Hour})
	s.CreateUser("susy", "pass", false)

	// Check if a topic is being subscribed to.
	var subscribed bool
	c.SubscribeFunc = func(replicaID, topicID uint64) error {
		subscribed = true
		return nil
	}

	// Write series with one point to the database.
	tags := map[string]string{"host": "servera.influx.com", "region": "uswest"}
	index, err := s.WriteSeries("foo", "mypolicy", []influxdb.Point{{Name: "cpu_load", Tags: tags, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Values: map[string]interface{}{"value": float64(23.2)}}})
	if err != nil {
		t.Fatal(err)
	} else if err = s.Sync(index); err != nil {
		t.Fatalf("sync error: %s", err)
	}

	// Write another point 10 seconds later so it goes through "raw series".
	index, err = s.WriteSeries("foo", "mypolicy", []influxdb.Point{{Name: "cpu_load", Tags: tags, Timestamp: mustParseTime("2000-01-01T00:00:10Z"), Values: map[string]interface{}{"value": float64(100)}}})
	if err != nil {
		t.Fatal(err)
	} else if err = s.Sync(index); err != nil {
		t.Fatalf("sync error: %s", err)
	}

	// Verify a subscription was made.
	if !subscribed {
		t.Fatal("expected subscription")
	}

	// Retrieve first series data point.
	if v, err := s.ReadSeries("foo", "mypolicy", "cpu_load", tags, mustParseTime("2000-01-01T00:00:00Z")); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(v, map[string]interface{}{"value": float64(23.2)}) {
		t.Fatalf("values mismatch: %#v", v)
	}

	// Retrieve second series data point.
	if v, err := s.ReadSeries("foo", "mypolicy", "cpu_load", tags, mustParseTime("2000-01-01T00:00:10Z")); err != nil {
		t.Fatal(err)
	} else if mustMarshalJSON(v) != mustMarshalJSON(map[string]interface{}{"value": float64(100)}) {
		t.Fatalf("values mismatch: %#v", v)
	}

	// Retrieve non-existent series data point.
	if v, err := s.ReadSeries("foo", "mypolicy", "cpu_load", tags, mustParseTime("2000-01-01T00:01:00Z")); err != nil {
		t.Fatal(err)
	} else if v != nil {
		t.Fatalf("expected nil values: %#v", v)
	}
}

// Ensure the server can execute a query and return the data correctly.
func TestServer_ExecuteQuery(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")
	s.CreateUser("susy", "pass", false)

	// Write series with one point to the database.
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east"}, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Values: map[string]interface{}{"value": float64(20)}}})
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east"}, Timestamp: mustParseTime("2000-01-01T00:00:10Z"), Values: map[string]interface{}{"value": float64(30)}}})
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-west"}, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Values: map[string]interface{}{"value": float64(100)}}})

	// Select data from the server.
	results := s.ExecuteQuery(MustParseQuery(`SELECT sum(value) FROM cpu GROUP BY time(10s), region`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Rows) != 2 {
		t.Fatalf("unexpected row count: %d", len(res.Rows))
	} else if s := mustMarshalJSON(res); s != `{"rows":[{"name":"cpu","tags":{"region":"us-east"},"columns":["time","sum"],"values":[[946684800000000000,20],[946684810000000000,30]]},{"name":"cpu","tags":{"region":"us-west"},"columns":["time","sum"],"values":[[946684800000000000,100]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}
}

func TestServer_CreateShardGroupIfNotExist(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")

	if err := s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "bar"}); err != nil {
		t.Fatal(err)
	}

	if err := s.CreateShardGroupIfNotExists("foo", "bar", time.Time{}); err != nil {
		t.Fatal(err)
	}

	if a, err := s.ShardGroups("foo"); err != nil {
		t.Fatal(err)
	} else if len(a) != 1 {
		t.Fatalf("expected 1 shard group but found %d", len(a))
	}
}

/* TODO(benbjohnson): Change test to not expose underlying series ids directly.
func TestServer_Measurements(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "mypolicy", Duration: 1 * time.Hour})
	s.CreateUser("susy", "pass", false)

	// Write series with one point to the database.
	timestamp := mustParseTime("2000-01-01T00:00:00Z")

	tags := map[string]string{"host": "servera.influx.com", "region": "uswest"}
	values := map[string]interface{}{"value": 23.2}

	index, err := s.WriteSeries("foo", "mypolicy", []influxdb.Point{influxdb.Point{Name: "cpu_load", Tags: tags, Timestamp: timestamp, Values: values}})
	if err != nil {
		t.Fatal(err)
	} else if err = s.Sync(index); err != nil {
		t.Fatalf("sync error: %s", err)
	}

	expectedMeasurementNames := []string{"cpu_load"}
	expectedSeriesIDs := influxdb.SeriesIDs([]uint32{uint32(1)})
	names := s.MeasurementNames("foo")
	if !reflect.DeepEqual(names, expectedMeasurementNames) {
		t.Fatalf("Mesurements not the same:\n  exp: %s\n  got: %s", expectedMeasurementNames, names)
	}
	ids := s.MeasurementSeriesIDs("foo", "foo")
	if !reflect.DeepEqual(ids, expectedSeriesIDs) {
		t.Fatalf("Series IDs not the same:\n  exp: %v\n  got: %v", expectedSeriesIDs, ids)
	}

	s.Restart()

	names = s.MeasurementNames("foo")
	if !reflect.DeepEqual(names, expectedMeasurementNames) {
		t.Fatalf("Mesurements not the same:\n  exp: %s\n  got: %s", expectedMeasurementNames, names)
	}
	ids = s.MeasurementSeriesIDs("foo", "foo")
	if !reflect.DeepEqual(ids, expectedSeriesIDs) {
		t.Fatalf("Series IDs not the same:\n  exp: %v\n  got: %v", expectedSeriesIDs, ids)
	}
}
*/

// Ensure the server can convert a measurement into its normalized form.
func TestServer_NormalizeMeasurement(t *testing.T) {
	var tests = []struct {
		in  string // input string
		db  string // current database
		out string // normalized string
		err string // error, if any
	}{
		{in: `cpu`, db: `db0`, out: `"db0"."rp0"."cpu"`},
		{in: `"".cpu`, db: `db0`, out: `"db0"."rp0"."cpu"`},
		{in: `"rp0".cpu`, db: `db0`, out: `"db0"."rp0"."cpu"`},
		{in: `""."".cpu`, db: `db0`, out: `"db0"."rp0"."cpu"`},
		{in: `""..cpu`, db: `db0`, out: `"db0"."rp0"."cpu"`},

		{in: `"db1"..cpu`, db: `db0`, out: `"db1"."rp1"."cpu"`},
		{in: `"db1"."rp1".cpu`, db: `db0`, out: `"db1"."rp1"."cpu"`},
		{in: `"db1"."rp2".cpu`, db: `db0`, out: `"db1"."rp2"."cpu"`},

		{in: ``, err: `invalid measurement: `},
		{in: `"foo"."bar"."baz"."bat"`, err: `invalid measurement: "foo"."bar"."baz"."bat"`},
		{in: `"no_db"..cpu`, db: ``, err: `database not found: no_db`},
		{in: `"db2"..cpu`, db: ``, err: `default retention policy not set for: db2`},
		{in: `"db2"."no_policy".cpu`, db: ``, err: `retention policy does not exist: db2.no_policy`},
	}

	// Create server with a variety of databases, retention policies, and measurements
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Default database with one policy.
	s.CreateDatabase("db0")
	s.CreateRetentionPolicy("db0", &influxdb.RetentionPolicy{Name: "rp0"})
	s.SetDefaultRetentionPolicy("db0", "rp0")

	// Another database with two policies.
	s.CreateDatabase("db1")
	s.CreateRetentionPolicy("db1", &influxdb.RetentionPolicy{Name: "rp1"})
	s.CreateRetentionPolicy("db1", &influxdb.RetentionPolicy{Name: "rp2"})
	s.SetDefaultRetentionPolicy("db1", "rp1")

	// Another database with no policies.
	s.CreateDatabase("db2")

	// Execute the tests
	for i, tt := range tests {
		out, err := s.NormalizeMeasurement(tt.in, tt.db)
		if tt.err != errstr(err) {
			t.Errorf("%d. %s/%s: error: exp: %s, got: %s", i, tt.db, tt.in, tt.err, errstr(err))
		} else if tt.out != out {
			t.Errorf("%d. %s/%s: out: exp: %s, got: %s", i, tt.db, tt.in, tt.out, out)
		}
	}
}

// Ensure the server can normalize all statements in query.
func TestServer_NormalizeQuery(t *testing.T) {
	var tests = []struct {
		in  string // input query
		db  string // default database
		out string // output query
		err string // error, if any
	}{
		{
			in: `SELECT cpu.value FROM cpu`, db: `db0`,
			out: `SELECT "db0"."rp0"."cpu"."value" FROM "db0"."rp0"."cpu"`,
		},

		{
			in: `SELECT value FROM cpu`, db: `no_db`, err: `database not found: no_db`,
		},
	}

	// Start server with database & retention policy.
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("db0")
	s.CreateRetentionPolicy("db0", &influxdb.RetentionPolicy{Name: "rp0"})
	s.SetDefaultRetentionPolicy("db0", "rp0")

	// Execute the tests
	for i, tt := range tests {
		out := MustParseQuery(tt.in)
		err := s.NormalizeStatement(out.Statements[0], tt.db)
		if tt.err != errstr(err) {
			t.Errorf("%d. %s/%s: error: exp: %s, got: %s", i, tt.db, tt.in, tt.err, errstr(err))
		} else if err == nil && tt.out != out.String() {
			t.Errorf("%d. %s/%s:\n\nexp: %s\n\ngot: %s\n\n", i, tt.db, tt.in, tt.out, out.String())
		}
	}
}

func mustMarshalJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic("marshal: " + err.Error())
	}
	return string(b)
}

func measurementsEqual(l influxdb.Measurements, r influxdb.Measurements) bool {
	if mustMarshalJSON(l) == mustMarshalJSON(r) {
		return true
	}
	return false
}

func TestServer_SeriesByTagNames(t *testing.T)  { t.Skip("pending") }
func TestServer_SeriesByTagValues(t *testing.T) { t.Skip("pending") }
func TestDatabase_TagNames(t *testing.T)        { t.Skip("pending") }
func TestServer_TagNamesBySeries(t *testing.T)  { t.Skip("pending") }
func TestServer_TagValues(t *testing.T)         { t.Skip("pending") }
func TestServer_TagValuesBySeries(t *testing.T) { t.Skip("pending") }

// Point JSON Unmarshal tests

func TestbatchWrite_UnmarshalEpoch(t *testing.T) {
	var (
		now     = time.Now()
		nanos   = now.UnixNano()
		micros  = nanos / int64(time.Microsecond)
		millis  = nanos / int64(time.Millisecond)
		seconds = nanos / int64(time.Second)
		minutes = nanos / int64(time.Minute)
		hours   = nanos / int64(time.Hour)
	)

	tests := []struct {
		name  string
		epoch int64
	}{
		{name: "nanos", epoch: nanos},
		{name: "micros", epoch: micros},
		{name: "millis", epoch: millis},
		{name: "seconds", epoch: seconds},
		{name: "minutes", epoch: minutes},
		{name: "hours", epoch: hours},
	}

	for _, test := range tests {
		json := fmt.Sprintf(`"points": [{timestamp: "%d"}`, test.epoch)
		log.Println(json)
		t.Fatal("foo")
	}

}

// Server is a wrapping test struct for influxdb.Server.
type Server struct {
	*influxdb.Server
}

// NewServer returns a new test server instance.
func NewServer() *Server {
	return &Server{influxdb.NewServer()}
}

// OpenServer returns a new, open test server instance.
func OpenServer(client influxdb.MessagingClient) *Server {
	s := OpenUninitializedServer(client)
	if err := s.Initialize(&url.URL{Host: "127.0.0.1:8080"}); err != nil {
		panic(err.Error())
	}
	return s
}

// OpenUninitializedServer returns a new, uninitialized, open test server instance.
func OpenUninitializedServer(client influxdb.MessagingClient) *Server {
	s := NewServer()
	if err := s.Open(tempfile()); err != nil {
		panic(err.Error())
	}
	if err := s.SetClient(client); err != nil {
		panic(err.Error())
	}
	return s
}

// OpenDefaultServer opens a server and creates a default db & retention policy.
func OpenDefaultServer(client influxdb.MessagingClient) *Server {
	s := OpenServer(client)
	if err := s.CreateDatabase("db"); err != nil {
		panic(err.Error())
	} else if err = s.CreateRetentionPolicy("db", &influxdb.RetentionPolicy{Name: "raw", Duration: 1 * time.Hour}); err != nil {
		panic(err.Error())
	} else if err = s.SetDefaultRetentionPolicy("db", "raw"); err != nil {
		panic(err.Error())
	}
	return s
}

// Restart stops and restarts the server.
func (s *Server) Restart() {
	path, client := s.Path(), s.Client()

	// Stop the server.
	if err := s.Server.Close(); err != nil {
		panic("close: " + err.Error())
	}

	// Open and reset the client.
	if err := s.Server.Open(path); err != nil {
		panic("open: " + err.Error())
	}
	if err := s.Server.SetClient(client); err != nil {
		panic("client: " + err.Error())
	}
}

// Close shuts down the server and removes all temporary files.
func (s *Server) Close() {
	defer os.RemoveAll(s.Path())
	s.Server.Close()
}

// MustWriteSeries writes series data and waits for the data to be applied.
// Returns the messaging index for the write.
func (s *Server) MustWriteSeries(database, retentionPolicy string, points []influxdb.Point) uint64 {
	index, err := s.WriteSeries(database, retentionPolicy, points)
	if err != nil {
		panic(err.Error())
	} else if err = s.Sync(index); err != nil {
		panic("sync error: " + err.Error())
	}
	return index
}

// MessagingClient represents a test client for the messaging broker.
type MessagingClient struct {
	index uint64
	c     chan *messaging.Message

	PublishFunc       func(*messaging.Message) (uint64, error)
	CreateReplicaFunc func(replicaID uint64) error
	DeleteReplicaFunc func(replicaID uint64) error
	SubscribeFunc     func(replicaID, topicID uint64) error
	UnsubscribeFunc   func(replicaID, topicID uint64) error
}

// NewMessagingClient returns a new instance of MessagingClient.
func NewMessagingClient() *MessagingClient {
	c := &MessagingClient{c: make(chan *messaging.Message, 1)}
	c.PublishFunc = c.send
	c.CreateReplicaFunc = func(replicaID uint64) error { return nil }
	c.DeleteReplicaFunc = func(replicaID uint64) error { return nil }
	c.SubscribeFunc = func(replicaID, topicID uint64) error { return nil }
	c.UnsubscribeFunc = func(replicaID, topicID uint64) error { return nil }
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

// Creates a new replica with a given ID on the broker.
func (c *MessagingClient) CreateReplica(replicaID uint64) error {
	return c.CreateReplicaFunc(replicaID)
}

// Deletes an existing replica with a given ID from the broker.
func (c *MessagingClient) DeleteReplica(replicaID uint64) error {
	return c.DeleteReplicaFunc(replicaID)
}

// Subscribe adds a subscription to a replica for a topic on the broker.
func (c *MessagingClient) Subscribe(replicaID, topicID uint64) error {
	return c.SubscribeFunc(replicaID, topicID)
}

// Unsubscribe removes a subscrition from a replica for a topic on the broker.
func (c *MessagingClient) Unsubscribe(replicaID, topicID uint64) error {
	return c.UnsubscribeFunc(replicaID, topicID)
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

// MustParseQuery parses an InfluxQL query. Panic on error.
func MustParseQuery(s string) *influxql.Query {
	q, err := influxql.NewParser(strings.NewReader(s)).ParseQuery()
	if err != nil {
		panic(err.Error())
	}
	return q
}

// MustParseSelectStatement parses an InfluxQL select statement. Panic on error.
func MustParseSelectStatement(s string) *influxql.SelectStatement {
	stmt, err := influxql.NewParser(strings.NewReader(s)).ParseStatement()
	if err != nil {
		panic(err.Error())
	}
	return stmt.(*influxql.SelectStatement)
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
