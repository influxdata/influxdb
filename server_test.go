package influxdb_test

import (
	"bytes"
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

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/test"
	"golang.org/x/crypto/bcrypt"
)

// Ensure the server can be successfully opened and closed.
func TestServer_Open(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := NewServer()
	defer s.Close()
	if err := s.Server.Open(tempfile(), c); err != nil {
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
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
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
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
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
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
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

// Test unuathorized requests logging
func TestServer_UnauthorizedRequests(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()

	s.SetAuthenticationEnabled(true)

	var b bytes.Buffer
	s.SetLogOutput(&b)

	adminOnlyQuery := &influxql.Query{
		Statements: []influxql.Statement{
			&influxql.DropDatabaseStatement{Name: "foo"},
		},
	}

	e := s.Authorize(nil, adminOnlyQuery, "foo")
	if _, ok := e.(influxdb.ErrAuthorize); !ok {
		t.Fatalf("unexpected error.  expected %v, actual: %v", influxdb.ErrAuthorize{}, e)
	}
	if !strings.Contains(b.String(), "unauthorized request") {
		t.Log(b.String())
		t.Fatalf(`log should contain "unuathorized request"`)
	}

	b.Reset()

	// Create normal database user.
	s.CreateUser("user1", "user1", false)
	user1 := s.User("user1")

	e = s.Authorize(user1, adminOnlyQuery, "foo")
	if _, ok := e.(influxdb.ErrAuthorize); !ok {
		t.Fatalf("unexpected error.  expected %v, actual: %v", influxdb.ErrAuthorize{}, e)
	}
	if !strings.Contains(b.String(), "unauthorized request") {
		t.Log(b.String())
		t.Fatalf(`log should contain "unuathorized request"`)
	}
}

// Test user privilege authorization.
func TestServer_UserPrivilegeAuthorization(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
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
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
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
	if err := s.Authorize(admin, adminOnlyQuery, ""); err != nil {
		t.Fatal(err)
	}

	if err := s.Authorize(admin, readWriteQuery, "foo"); err != nil {
		t.Fatal(err)
	}

	// Normal user should not be authorized to execute admin only query.
	if err := s.Authorize(user, adminOnlyQuery, ""); err == nil {
		t.Fatalf("normal user should not be authorized to execute cluster admin level queries")
	}

	// Normal user should not be authorized to execute query that selects into another
	// database which (s)he doesn't have privileges on.
	if err := s.Authorize(user, readWriteQuery, ""); err == nil {
		t.Fatalf("normal user should not be authorized to write to database bar")
	}

	// Grant normal user write privileges on database "bar".
	user.Privileges["bar"] = influxql.WritePrivilege

	//Authorization on the previous query should now succeed.
	if err := s.Authorize(user, readWriteQuery, ""); err != nil {
		t.Fatal(err)
	}
}

// Test multiple statement query authorization.
func TestServer_MultiStatementQueryAuthorization(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
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
	if err := s.Authorize(admin, readWriteQuery, "foo"); err != nil {
		t.Fatal(err)
	}

	// Normal user with only read privileges should not be authorized to execute both statements.
	if err := s.Authorize(user, readWriteQuery, "foo"); err == nil {
		t.Fatalf("user should not be authorized to execute both statements")
	}
}

// Ensure the server can create a database.
func TestServer_CreateDatabase(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()

	// Attempt creating database without a name
	if err := s.CreateDatabase(""); err != influxdb.ErrDatabaseNameRequired {
		t.Fatal("expected error on empty database name")
	}

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
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
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
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()

	// Attempt dropping a database without a name.
	if err := s.DropDatabase(""); err != influxdb.ErrDatabaseNameRequired {
		t.Fatal("expected error on empty database name")
	}

	// Create the "foo" database and verify it exists.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	} else if !s.DatabaseExists("foo") {
		t.Fatalf("database not actually created")
	}
	s.Restart()

	// Drop the "foo" database and verify that it's gone.
	if err := s.DropDatabase("foo"); err != nil {
		t.Fatal(err)
	} else if s.DatabaseExists("foo") {
		t.Fatalf("database not actually dropped")
	}
}

// Ensure the server returns an error when dropping a database that doesn't exist.
func TestServer_DropDatabase_ErrDatabaseNotFound(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()

	// Drop a database that doesn't exist.
	if err := s.DropDatabase("no_such_db"); err != influxdb.ErrDatabaseNotFound {
		t.Fatal(err)
	}
}

// Ensure the server can return a list of all databases.
func TestServer_Databases(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
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
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
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
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
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
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	if err := s.CreateUser("", "pass", false); err != influxdb.ErrUsernameRequired {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when creating a duplicate user.
func TestServer_CreateUser_ErrUserExists(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
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
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
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
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
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
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
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
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()

	// Create a database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}

	// Create a retention policy on the database.
	rp := &influxdb.RetentionPolicy{
		Name:               "bar",
		Duration:           time.Hour,
		ShardGroupDuration: time.Hour,
		ReplicaN:           2,
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

// Ensure the database can create a new retention policy with infinite duration.
func TestServer_CreateRetentionPolicyInfinite(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()

	// Create a database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}

	// Create a retention policy on the database.
	rp := &influxdb.RetentionPolicy{
		Name:               "bar",
		Duration:           0,
		ShardGroupDuration: time.Hour * 24 * 7,
		ReplicaN:           2,
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
		t.Logf("expected: %#v\n", rp)
		t.Fatalf("retention policy mismatch: %#v", o)
	}
}

// Ensure the database can creates a default retention policy.
func TestServer_CreateRetentionPolicyDefault(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()

	s.RetentionAutoCreate = true

	// Create a database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}

	s.Restart()

	rp := &influxdb.RetentionPolicy{
		Name:               "default",
		Duration:           0,
		ShardGroupDuration: time.Hour * 24 * 7,
		ReplicaN:           1,
	}

	// Verify that the policy exists.
	if o, err := s.RetentionPolicy("foo", "default"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if o == nil {
		t.Fatalf("retention policy not found")
	} else if !reflect.DeepEqual(rp, o) {
		t.Logf("expected: %#v\n", rp)
		t.Fatalf("retention policy mismatch: %#v", o)
	}
}

// Ensure the server returns an error when creating a retention policy with an invalid db.
func TestServer_CreateRetentionPolicy_ErrDatabaseNotFound(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	if err := s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "bar", Duration: time.Hour}); err != influxdb.ErrDatabaseNotFound {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when creating a retention policy without a name.
func TestServer_CreateRetentionPolicy_ErrRetentionPolicyNameRequired(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	if err := s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "", Duration: time.Hour}); err != influxdb.ErrRetentionPolicyNameRequired {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when creating a duplicate retention policy.
func TestServer_CreateRetentionPolicy_ErrRetentionPolicyExists(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "bar", Duration: time.Hour})
	if err := s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "bar", Duration: time.Hour}); err != influxdb.ErrRetentionPolicyExists {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when creating a retention policy with a duration less than one hour.
func TestServer_CreateRetentionPolicy_ErrRetentionPolicyMinDuration(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	if err := s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "bar", Duration: time.Minute}); err != influxdb.ErrRetentionPolicyMinDuration {
		t.Fatal(err)
	}
}

// Ensure the database can alter an existing retention policy.
func TestServer_AlterRetentionPolicy(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
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

	// Alter the retention policy.
	duration := 2 * time.Hour
	replicaN := uint32(3)
	rp2 := &influxdb.RetentionPolicyUpdate{
		Duration: &duration,
		ReplicaN: &replicaN,
	}
	if err := s.UpdateRetentionPolicy("foo", "bar", rp2); err != nil {
		t.Fatal(err)
	}

	// Restart the server to make sure the changes persist afterwards.
	s.Restart()

	// Verify that the policy exists.
	if o, err := s.RetentionPolicy("foo", "bar"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if o == nil {
		t.Fatalf("retention policy not found")
	} else if o.Duration != *rp2.Duration {
		t.Fatalf("retention policy mismatch:\n\texp Duration = %s\n\tgot Duration = %s\n", rp2.Duration, o.Duration)
	} else if o.ReplicaN != *rp2.ReplicaN {
		t.Fatalf("retention policy mismatch:\n\texp ReplicaN = %d\n\tgot ReplicaN = %d\n", rp2.ReplicaN, o.ReplicaN)
	}

	// Test update duration only.
	duration = time.Hour
	results := s.ExecuteQuery(MustParseQuery(`ALTER RETENTION POLICY bar ON foo DURATION 1h`), "foo", nil)
	if results.Error() != nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}

	// Verify results
	if o, err := s.RetentionPolicy("foo", "bar"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if o == nil {
		t.Fatalf("retention policy not found")
	} else if o.Duration != duration {
		t.Fatalf("retention policy mismatch:\n\texp Duration = %s\n\tgot Duration = %s\n", duration, o.Duration)
	} else if o.ReplicaN != *rp2.ReplicaN {
		t.Fatalf("retention policy mismatch:\n\texp ReplicaN = %d\n\tgot ReplicaN = %d\n", rp2.ReplicaN, o.ReplicaN)
	}

	// set duration to infinite to catch edge case.
	duration = 0
	results = s.ExecuteQuery(MustParseQuery(`ALTER RETENTION POLICY bar ON foo DURATION INF`), "foo", nil)
	if results.Error() != nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}

	// Verify results
	if o, err := s.RetentionPolicy("foo", "bar"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if o == nil {
		t.Fatalf("retention policy not found")
	} else if o.Duration != duration {
		t.Fatalf("retention policy mismatch:\n\texp Duration = %s\n\tgot Duration = %s\n", duration, o.Duration)
	} else if o.ReplicaN != *rp2.ReplicaN {
		t.Fatalf("retention policy mismatch:\n\texp ReplicaN = %d\n\tgot ReplicaN = %d\n", rp2.ReplicaN, o.ReplicaN)
	}

}

// Ensure the server an error is returned if trying to alter a retention policy with a duration too small.
func TestServer_AlterRetentionPolicy_Minduration(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
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

	// Alter the retention policy.
	duration := 2 * time.Hour
	replicaN := uint32(3)
	rp2 := &influxdb.RetentionPolicyUpdate{
		Duration: &duration,
		ReplicaN: &replicaN,
	}
	if err := s.UpdateRetentionPolicy("foo", "bar", rp2); err != nil {
		t.Fatal(err)
	}

	// Restart the server to make sure the changes persist afterwards.
	s.Restart()

	// Verify that the policy exists.
	if o, err := s.RetentionPolicy("foo", "bar"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if o == nil {
		t.Fatalf("retention policy not found")
	} else if o.Duration != *rp2.Duration {
		t.Fatalf("retention policy mismatch:\n\texp Duration = %s\n\tgot Duration = %s\n", rp2.Duration, o.Duration)
	} else if o.ReplicaN != *rp2.ReplicaN {
		t.Fatalf("retention policy mismatch:\n\texp ReplicaN = %d\n\tgot ReplicaN = %d\n", rp2.ReplicaN, o.ReplicaN)
	}

	// Test update duration only.
	duration = time.Hour
	results := s.ExecuteQuery(MustParseQuery(`ALTER RETENTION POLICY bar ON foo DURATION 1m`), "foo", nil)
	if results.Error() == nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}
}

// Ensure the server can delete an existing retention policy.
func TestServer_DeleteRetentionPolicy(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()

	// Create a database and retention policy.
	s.CreateDatabase("foo")
	if err := s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "bar", Duration: time.Hour}); err != nil {
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
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	if err := s.DeleteRetentionPolicy("foo", "bar"); err != influxdb.ErrDatabaseNotFound {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when deleting a retention policy without a name.
func TestServer_DeleteRetentionPolicy_ErrRetentionPolicyNameRequired(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	if err := s.DeleteRetentionPolicy("foo", ""); err != influxdb.ErrRetentionPolicyNameRequired {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when deleting a non-existent retention policy.
func TestServer_DeleteRetentionPolicy_ErrRetentionPolicyNotFound(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	if err := s.DeleteRetentionPolicy("foo", "no_such_policy"); err != influxdb.ErrRetentionPolicyNotFound {
		t.Fatal(err)
	}
}

// Ensure the server can set the default retention policy
func TestServer_SetDefaultRetentionPolicy(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")

	rp := &influxdb.RetentionPolicy{Name: "bar", ShardGroupDuration: time.Hour, Duration: time.Hour}
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

// Ensure the server returns an error when setting the default retention policy to a non-existant one.
func TestServer_SetDefaultRetentionPolicy_ErrRetentionPolicyNotFound(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	if err := s.SetDefaultRetentionPolicy("foo", "no_such_policy"); err != influxdb.ErrRetentionPolicyNotFound {
		t.Fatal(err)
	}
}

// Ensure the server pre-creates shard groups as needed.
func TestServer_PreCreateRetentionPolices(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "mypolicy", Duration: 60 * time.Minute})

	// Create two shard groups for the the new retention policy -- 1 which will age out immediately
	// the other in more than an hour.
	s.CreateShardGroupIfNotExists("foo", "mypolicy", time.Now().Add(-2*time.Hour))

	// Check the two shard groups exist.
	var g []*influxdb.ShardGroup
	g, err := s.ShardGroups("foo")
	if err != nil {
		t.Fatal(err)
	} else if len(g) != 1 {
		t.Fatalf("expected 1 shard group but found %d", len(g))
	}

	// Run shard group pre-create.
	s.ShardGroupPreCreate(time.Hour)

	// Ensure enforcement is in effect across restarts.
	s.Restart()

	// Second shard group should now be created.
	g, err = s.ShardGroups("foo")
	if err != nil {
		t.Fatal(err)
	} else if len(g) != 2 {
		t.Fatalf("expected 2 shard group but found %d", len(g))
	}
}

// Ensure the server prohibits a zero check interval for retention policy enforcement.
func TestServer_StartRetentionPolicyEnforcement_ErrZeroInterval(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	if err := s.StartRetentionPolicyEnforcement(time.Duration(0)); err == nil {
		t.Fatal("failed to prohibit retention policies zero check interval")
	}
}

func TestServer_EnforceRetentionPolices(t *testing.T) {
	c := test.NewMessagingClient()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "mypolicy", Duration: 60 * time.Minute})

	// Create two shard groups for the the new retention policy -- 1 which will age out immediately
	// the other in more than an hour.
	s.CreateShardGroupIfNotExists("foo", "mypolicy", time.Now().Add(-2*time.Hour))
	s.CreateShardGroupIfNotExists("foo", "mypolicy", time.Now().Add(time.Hour))

	// Check the two shard groups exist.
	var g []*influxdb.ShardGroup
	g, err := s.ShardGroups("foo")
	if err != nil {
		t.Fatal(err)
	} else if len(g) != 2 {
		t.Fatalf("expected 2 shard group but found %d", len(g))
	}

	// Run retention enforcement.
	s.EnforceRetentionPolicies()

	// Ensure enforcement is in effect across restarts.
	s.Restart()

	// First shard group should have been removed.
	g, err = s.ShardGroups("foo")
	if err != nil {
		t.Fatal(err)
	} else if len(g) != 1 {
		t.Fatalf("expected 1 shard group but found %d", len(g))
	}
}

// Ensure the database can write data to the database.
func TestServer_WriteSeries(t *testing.T) {
	c := test.NewMessagingClient()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "mypolicy", Duration: 1 * time.Hour})
	s.CreateUser("susy", "pass", false)

	// Write series with one point to the database.
	tags := map[string]string{"host": "servera.influx.com", "region": "uswest"}
	index, err := s.WriteSeries("foo", "mypolicy", []influxdb.Point{{Name: "cpu_load", Tags: tags, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Fields: map[string]interface{}{"value": float64(23.2)}}})
	if err != nil {
		t.Fatal(err)
	}
	c.Sync(index)

	// Write another point 10 seconds later so it goes through "raw series".
	index, err = s.WriteSeries("foo", "mypolicy", []influxdb.Point{{Name: "cpu_load", Tags: tags, Timestamp: mustParseTime("2000-01-01T00:00:10Z"), Fields: map[string]interface{}{"value": float64(100)}}})
	if err != nil {
		t.Fatal(err)
	}
	c.Sync(index)

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

// Ensure the server can drop a measurement.
func TestServer_DropMeasurement(t *testing.T) {
	c := test.NewMessagingClient()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")
	s.CreateUser("susy", "pass", false)

	// Write series with one point to the database.
	tags := map[string]string{"host": "serverA", "region": "uswest"}
	index, err := s.WriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: tags, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Fields: map[string]interface{}{"value": float64(23.2)}}})
	if err != nil {
		t.Fatal(err)
	}
	c.Sync(index)

	// Ensure measurement exists
	results := s.ExecuteQuery(MustParseQuery(`SHOW MEASUREMENTS`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"measurements","columns":["name"],"values":[["cpu"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	// Ensure series exists
	results = s.ExecuteQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["id","host","region"],"values":[[1,"serverA","uswest"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	// Drop measurement
	results = s.ExecuteQuery(MustParseQuery(`DROP MEASUREMENT cpu`), "foo", nil)
	if results.Error() != nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}

	results = s.ExecuteQuery(MustParseQuery(`SHOW MEASUREMENTS`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 0 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	results = s.ExecuteQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 0 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{}` {
		t.Fatalf("unexpected row(0): %s", s)
	}
}

// Ensure the server can handles drop measurement if none exists.
func TestServer_DropMeasurementNoneExists(t *testing.T) {
	c := test.NewMessagingClient()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")
	s.CreateUser("susy", "pass", false)

	// Drop measurement
	results := s.ExecuteQuery(MustParseQuery(`DROP MEASUREMENT bar`), "foo", nil)
	if results.Error().Error() != `measurement not found` {
		t.Fatalf("unexpected error: %s", results.Error())
	}

	// Write series with one point to the database.
	tags := map[string]string{"host": "serverA", "region": "uswest"}
	index, err := s.WriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: tags, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Fields: map[string]interface{}{"value": float64(23.2)}}})
	if err != nil {
		t.Fatal(err)
	}
	c.Sync(index)

	// Drop measurement after writing data to ensure we still get the same error
	results = s.ExecuteQuery(MustParseQuery(`DROP MEASUREMENT bar`), "foo", nil)
	if results.Error().Error() != `measurement not found` {
		t.Fatalf("unexpected error: %s", results.Error())
	}
}

// Ensure Drop measurement can:
// write to measurement cpu with tags region=uswest host=serverA
// write to measurement memory with tags region=uswest host=serverB
// drop one of those measurements
// ensure that the dropped measurement is gone
// ensure that we can still query: show measurements
// ensure that we can still make various queries:
//    select * from memory where region=uswest and host=serverb
//    select * from memory where host=serverb
//    select * from memory where region=uswest
func TestServer_DropMeasurementSeriesTagsPreserved(t *testing.T) {
	c := test.NewMessagingClient()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")
	s.CreateUser("susy", "pass", false)

	// Write series with one point to the database.
	tags := map[string]string{"host": "serverA", "region": "uswest"}
	index, err := s.WriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: tags, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Fields: map[string]interface{}{"value": float64(23.2)}}})
	if err != nil {
		t.Fatal(err)
	}
	c.Sync(index)

	tags = map[string]string{"host": "serverB", "region": "uswest"}
	index, err = s.WriteSeries("foo", "raw", []influxdb.Point{{Name: "memory", Tags: tags, Timestamp: mustParseTime("2000-01-01T00:00:01Z"), Fields: map[string]interface{}{"value": float64(33.2)}}})
	if err != nil {
		t.Fatal(err)
	}
	c.Sync(index)

	// Ensure measurement exists
	results := s.ExecuteQuery(MustParseQuery(`SHOW MEASUREMENTS`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"measurements","columns":["name"],"values":[["cpu"],["memory"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	results = s.ExecuteQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 2 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["id","host","region"],"values":[[1,"serverA","uswest"]]},{"name":"memory","columns":["id","host","region"],"values":[[2,"serverB","uswest"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	// Ensure we can query for memory with both tags
	results = s.ExecuteQuery(MustParseQuery(`SELECT * FROM memory where region='uswest' and host='serverB'`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"memory","columns":["time","value"],"values":[["2000-01-01T00:00:01Z",33.2]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	// Drop measurement
	results = s.ExecuteQuery(MustParseQuery(`DROP MEASUREMENT cpu`), "foo", nil)
	if results.Error() != nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}

	// Ensure measurement exists
	results = s.ExecuteQuery(MustParseQuery(`SHOW MEASUREMENTS`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"measurements","columns":["name"],"values":[["memory"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	results = s.ExecuteQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"memory","columns":["id","host","region"],"values":[[2,"serverB","uswest"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	results = s.ExecuteQuery(MustParseQuery(`SELECT * FROM cpu`), "foo", nil)
	if res := results.Results[0]; res.Err.Error() != `measurement not found: "foo"."raw"."cpu"` {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 0 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	}

	results = s.ExecuteQuery(MustParseQuery(`SELECT * FROM memory where host='serverB'`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"memory","columns":["time","value"],"values":[["2000-01-01T00:00:01Z",33.2]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	results = s.ExecuteQuery(MustParseQuery(`SELECT * FROM memory where region='uswest'`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"memory","columns":["time","value"],"values":[["2000-01-01T00:00:01Z",33.2]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	results = s.ExecuteQuery(MustParseQuery(`SELECT * FROM memory where region='uswest' and host='serverB'`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"memory","columns":["time","value"],"values":[["2000-01-01T00:00:01Z",33.2]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}
}

// Ensure the server can drop a series.
func TestServer_DropSeries(t *testing.T) {
	c := test.NewMessagingClient()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")
	s.CreateUser("susy", "pass", false)

	// Write series with one point to the database.
	tags := map[string]string{"host": "serverA", "region": "uswest"}
	index, err := s.WriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: tags, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Fields: map[string]interface{}{"value": float64(23.2)}}})
	if err != nil {
		t.Fatal(err)
	}
	c.Sync(index)

	// Ensure series exists
	results := s.ExecuteQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["id","host","region"],"values":[[1,"serverA","uswest"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	// Drop series
	results = s.ExecuteQuery(MustParseQuery(`DROP SERIES FROM cpu`), "foo", nil)
	if results.Error() != nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}

	results = s.ExecuteQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 0 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{}` {
		t.Fatalf("unexpected row(0): %s", s)
	}
}

// Ensure the server can drop a series from measurement when more than one shard exists.
func TestServer_DropSeriesFromMeasurement(t *testing.T) {
	c := test.NewMessagingClient()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")
	s.CreateUser("susy", "pass", false)

	// Write series with one point to the database.
	tags := map[string]string{"host": "serverA", "region": "uswest"}
	index, err := s.WriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: tags, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Fields: map[string]interface{}{"value": float64(23.2)}}})
	if err != nil {
		t.Fatal(err)
	}
	c.Sync(index)

	tags = map[string]string{"host": "serverb", "region": "useast"}
	index, err = s.WriteSeries("foo", "raw", []influxdb.Point{{Name: "memory", Tags: tags, Timestamp: mustParseTime("2000-01-02T00:00:00Z"), Fields: map[string]interface{}{"value": float64(23465432423)}}})
	if err != nil {
		t.Fatal(err)
	}
	c.Sync(index)

	// Drop series
	results := s.ExecuteQuery(MustParseQuery(`DROP SERIES FROM memory`), "foo", nil)
	if results.Error() != nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}

	results = s.ExecuteQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["id","host","region"],"values":[[1,"serverA","uswest"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}
}

// Ensure that when merging many series together and some of them have a different number of points than others
// in a group by interval the results are correct
func TestServer_MergeManySeries(t *testing.T) {
	c := test.NewMessagingClient()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")

	for i := 1; i < 11; i++ {
		for j := 1; j < 5+i%3; j++ {
			tags := map[string]string{"host": fmt.Sprintf("server_%d", i)}
			index, err := s.WriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: tags, Timestamp: time.Unix(int64(j), int64(0)), Fields: map[string]interface{}{"value": float64(22)}}})
			if err != nil {
				t.Fatalf("unexpected error: %s", err.Error())
			}
			c.Sync(index)
		}
	}

	results := s.ExecuteQuery(MustParseQuery(`select count(value) from cpu where time >= '1970-01-01T00:00:01Z' AND time <= '1970-01-01T00:00:06Z' group by time(1s)`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:01Z",10],["1970-01-01T00:00:02Z",10],["1970-01-01T00:00:03Z",10],["1970-01-01T00:00:04Z",10],["1970-01-01T00:00:05Z",7],["1970-01-01T00:00:06Z",3]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}
}

// Ensure Drop Series can:
// write to measurement cpu with tags region=uswest host=serverA
// write to measurement cpu with tags region=uswest host=serverB
// drop one of those series
// ensure that the dropped series is gone
// ensure that we can still query: select value from cpu where region=uswest
func TestServer_DropSeriesTagsPreserved(t *testing.T) {
	c := test.NewMessagingClient()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")
	s.CreateUser("susy", "pass", false)

	// Write series with one point to the database.
	tags := map[string]string{"host": "serverA", "region": "uswest"}
	index, err := s.WriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: tags, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Fields: map[string]interface{}{"value": float64(23.2)}}})
	if err != nil {
		t.Fatal(err)
	}
	c.Sync(index)

	tags = map[string]string{"host": "serverB", "region": "uswest"}
	index, err = s.WriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: tags, Timestamp: mustParseTime("2000-01-01T00:00:01Z"), Fields: map[string]interface{}{"value": float64(33.2)}}})
	if err != nil {
		t.Fatal(err)
	}
	c.Sync(index)

	results := s.ExecuteQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["id","host","region"],"values":[[1,"serverA","uswest"],[2,"serverB","uswest"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	results = s.ExecuteQuery(MustParseQuery(`DROP SERIES FROM cpu where host='serverA'`), "foo", nil)
	if results.Error() != nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}

	results = s.ExecuteQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["id","host","region"],"values":[[2,"serverB","uswest"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	results = s.ExecuteQuery(MustParseQuery(`SELECT * FROM cpu where host='serverA'`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 0 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	}

	results = s.ExecuteQuery(MustParseQuery(`SELECT * FROM cpu where host='serverB'`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:01Z",33.2]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	results = s.ExecuteQuery(MustParseQuery(`SELECT * FROM cpu where region='uswest'`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:01Z",33.2]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}
}

// Ensure the server can execute a query and return the data correctly.
func TestServer_ExecuteQuery(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")
	s.CreateUser("susy", "pass", false)

	// Write series with one point to the database.
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east"}, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Fields: map[string]interface{}{"value": float64(20)}}})
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east"}, Timestamp: mustParseTime("2000-01-01T00:00:10Z"), Fields: map[string]interface{}{"value": float64(30)}}})
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-west"}, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Fields: map[string]interface{}{"value": float64(100)}}})

	// Select data from the server.
	expected := `{"series":[{"name":"cpu","tags":{"region":"us-east"},"columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",20],["2000-01-01T00:00:10Z",30]]},{"name":"cpu","tags":{"region":"us-west"},"columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",100],["2000-01-01T00:00:10Z",null]]}]}`
	results := s.ExecuteQuery(MustParseQuery(`SELECT sum(value) FROM cpu where time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:20Z' GROUP BY time(10s), region`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 2 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != expected {
		t.Fatalf("unexpected row(0):\nexp: %s\ngot: %s", expected, s)
	}

	// Simple non-aggregation.
	expected = `{"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:10Z",30]]}]}`
	results = s.ExecuteQuery(MustParseQuery(`SELECT value FROM cpu WHERE time >= '2000-01-01 00:00:05'`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error during simple SELECT: %s", res.Err)
	} else if s := mustMarshalJSON(res); s != expected {
		t.Fatalf("unexpected row(0):\nexp: %s\ngot: %s", expected, s)
	}

	// Sum aggregation.
	expected = `{"series":[{"name":"cpu","tags":{"region":"us-east"},"columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",30]]}]}`
	results = s.ExecuteQuery(MustParseQuery(`SELECT sum(value) FROM cpu WHERE time >= '2000-01-01 00:00:05' AND time <= '2000-01-01T00:00:10Z' GROUP BY time(10s), region`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error during SUM: %s", res.Err)
	} else if s := mustMarshalJSON(res); s != expected {
		t.Fatalf("unexpected row(0):\nexp: %s\ngot: %s", expected, s)
	}

	// Aggregation with a null field value
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east"}, Timestamp: mustParseTime("2000-01-01T00:00:03Z"), Fields: map[string]interface{}{"otherVal": float64(20)}}})
	// Sum aggregation.
	results = s.ExecuteQuery(MustParseQuery(`SELECT sum(value) FROM cpu GROUP BY region`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error during SUM: %s", res.Err)
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","tags":{"region":"us-east"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",50]]},{"name":"cpu","tags":{"region":"us-west"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",100]]}]}` {
		t.Fatalf("unexpected row(0) during SUM: %s", s)
	}

	// Multiple aggregations
	expected = `{"series":[{"name":"cpu","tags":{"region":"us-east"},"columns":["time","sum","mean"],"values":[["1970-01-01T00:00:00Z",50,25]]},{"name":"cpu","tags":{"region":"us-west"},"columns":["time","sum","mean"],"values":[["1970-01-01T00:00:00Z",100,100]]}]}`
	results = s.ExecuteQuery(MustParseQuery(`SELECT sum(value), mean(value) FROM cpu GROUP BY region`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error during multiple aggregation: %s", res.Err)
	} else if s := mustMarshalJSON(res); s != expected {
		t.Fatalf("unexpected row(0) during multiple aggregation:\n  exp: %s\n  got: %s", expected, s)
	}

	expected = `{"series":[{"name":"cpu","tags":{"region":"us-east"},"columns":["time","div"],"values":[["1970-01-01T00:00:00Z",2]]},{"name":"cpu","tags":{"region":"us-west"},"columns":["time","div"],"values":[["1970-01-01T00:00:00Z",1]]}]}`
	results = s.ExecuteQuery(MustParseQuery(`SELECT sum(value) / mean(value) as div FROM cpu GROUP BY region`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error during multiple aggregation: %s", res.Err)
	} else if s := mustMarshalJSON(res); s != expected {
		t.Fatalf("unexpected row(0) during multiple aggregation:\n  exp: %s\n  got: %s", expected, s)
	}

	// Group by multiple dimensions
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "load", Tags: map[string]string{"region": "us-east", "host": "serverA"}, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Fields: map[string]interface{}{"value": float64(20)}}})
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "load", Tags: map[string]string{"region": "us-east", "host": "serverB"}, Timestamp: mustParseTime("2000-01-01T00:00:10Z"), Fields: map[string]interface{}{"value": float64(30)}}})
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "load", Tags: map[string]string{"region": "us-west", "host": "serverC"}, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Fields: map[string]interface{}{"value": float64(100)}}})

	// Multiple group by dimensions
	expected = `{"series":[{"name":"load","tags":{"host":"serverA","region":"us-east"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",20]]},{"name":"load","tags":{"host":"serverB","region":"us-east"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",30]]},{"name":"load","tags":{"host":"serverC","region":"us-west"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",100]]}]}`
	results = s.ExecuteQuery(MustParseQuery(`SELECT sum(value) FROM load GROUP BY time(10s), region, host`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 3 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != expected {
		t.Fatalf("unexpected row(0) during multiple aggregation:\n  exp: %s\n  got: %s", expected, s)
	}

	// WHERE with AND
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "uk", "host": "serverZ", "service": "redis"}, Timestamp: mustParseTime("2000-01-01T00:00:03Z"), Fields: map[string]interface{}{"value": float64(20)}}})
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "uk", "host": "serverZ", "service": "mysql"}, Timestamp: mustParseTime("2000-01-01T00:00:03Z"), Fields: map[string]interface{}{"value": float64(30)}}})
	// Sum aggregation.
	results = s.ExecuteQuery(MustParseQuery(`SELECT sum(value) FROM cpu WHERE region='uk' AND host='serverZ'`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error during SUM: %s", res.Err)
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",50]]}]}` {
		t.Fatalf("unexpected row(0) during SUM AND: %s", s)
	}

	// TODO re-enable. The following code is racy
	//results = s.ExecuteQuery(MustParseQuery(`SELECT * FROM cpu WHERE region='uk' AND host='serverZ'`), "foo", nil)
	//if res := results.Results[0]; res.Err != nil {
	//t.Fatalf("unexpected error during SUM: %s", res.Err)
	//} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["time","value","otherVal"],"values":[["2000-01-01T00:00:03Z",30,0],["2000-01-01T00:00:03Z",20,0]]}]}` {
	//t.Fatalf("unexpected row(0) during SUM AND: %s", s)
	//}

	// Select that should return an empty result.
	results = s.ExecuteQuery(MustParseQuery(`SELECT value FROM cpu WHERE time >= '3000-01-01 00:00:05'`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error during simple SELECT: %s", res.Err)
	} else if s := mustMarshalJSON(res); s != `{}` {
		t.Fatalf("unexpected row(0) during simple SELECT: %s", s)
	}
}

// Ensure the server respects limit and offset in show series queries
func TestServer_ShowSeriesLimitOffset(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")

	// Write series with one point to the database.
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east", "host": "serverA"}, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Fields: map[string]interface{}{"value": float64(20)}}})
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east", "host": "serverB"}, Timestamp: mustParseTime("2000-01-01T00:00:10Z"), Fields: map[string]interface{}{"value": float64(30)}}})
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-west", "host": "serverC"}, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Fields: map[string]interface{}{"value": float64(100)}}})
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "memory", Tags: map[string]string{"region": "us-west", "host": "serverB"}, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Fields: map[string]interface{}{"value": float64(100)}}})
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "memory", Tags: map[string]string{"region": "us-east", "host": "serverA"}, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Fields: map[string]interface{}{"value": float64(100)}}})

	// Select data from the server.
	results := s.ExecuteQuery(MustParseQuery(`SHOW SERIES LIMIT 3 OFFSET 1`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 2 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["id","host","region"],"values":[[2,"serverB","us-east"],[3,"serverC","us-west"]]},{"name":"memory","columns":["id","host","region"],"values":[[4,"serverB","us-west"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	// Select data from the server.
	results = s.ExecuteQuery(MustParseQuery(`SHOW SERIES LIMIT 2 OFFSET 4`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"memory","columns":["id","host","region"],"values":[[5,"serverA","us-east"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	// Select data from the server.
	results = s.ExecuteQuery(MustParseQuery(`SHOW SERIES LIMIT 2 OFFSET 20`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 0 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	}

	// Select data from the server.
	results = s.ExecuteQuery(MustParseQuery(`SHOW SERIES LIMIT 4 OFFSET 0`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 2 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	}

	// Select data from the server.
	results = s.ExecuteQuery(MustParseQuery(`SHOW SERIES LIMIT 20`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 2 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["id","host","region"],"values":[[1,"serverA","us-east"],[2,"serverB","us-east"],[3,"serverC","us-west"]]},{"name":"memory","columns":["id","host","region"],"values":[[4,"serverB","us-west"],[5,"serverA","us-east"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}
}

// Ensure that when querying for raw data values that they return in time order
func TestServer_RawDataReturnsInOrder(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")
	s.CreateUser("susy", "pass", false)

	for i := 1; i < 500; i++ {
		host := fmt.Sprintf("server-%d", i%10)
		s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east", "host": host}, Timestamp: time.Unix(int64(i), 0), Fields: map[string]interface{}{"value": float64(i)}}})
	}

	results := s.ExecuteQuery(MustParseQuery(`SELECT count(value) FROM cpu`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error during COUNT: %s", res.Err)
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",499]]}]}` {
		t.Fatalf("unexpected row(0) during COUNT: %s", s)
	}

	results = s.ExecuteQuery(MustParseQuery(`SELECT value FROM cpu`), "foo", nil)

	lastTime := int64(0)
	for _, v := range results.Results[0].Series[0].Values {
		tt := v[0].(time.Time)
		if lastTime > tt.UnixNano() {
			t.Fatal("values out of order")
		}
		lastTime = tt.UnixNano()
	}
	if len(results.Results[0].Series[0].Values) != 499 {
		t.Fatal("expected 499 values")
	}

	results = s.ExecuteQuery(MustParseQuery(`SELECT value FROM cpu GROUP BY *`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error during GROUP BY *: %s", res.Err)
	} else if len(res.Series) != 10 {
		t.Fatalf("expected 10 series back but got %d", len(res.Series))
	} else if len(res.Series[1].Values) != 50 {
		t.Fatalf("expected 50 values per series but got %d", len(res.Series[0].Values))
	}
}

// Ensure that limit and offset work
func TestServer_LimitAndOffset(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")

	for i := 1; i < 10; i++ {
		host := fmt.Sprintf("server-%d", i)
		s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east", "host": host}, Timestamp: time.Unix(int64(i), 0), Fields: map[string]interface{}{"value": float64(i)}}})
	}

	results := s.ExecuteQuery(MustParseQuery(`SELECT count(value) FROM cpu GROUP BY * SLIMIT 20`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error during COUNT: %s", res.Err)
	} else if len(res.Series) != 9 {
		t.Fatalf("unexpected 9 series back but got %d", len(res.Series))
	}

	results = s.ExecuteQuery(MustParseQuery(`SELECT count(value) FROM cpu GROUP BY * SLIMIT 2 SOFFSET 1`), "foo", nil)
	expected := `{"series":[{"name":"cpu","tags":{"host":"server-2","region":"us-east"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]},{"name":"cpu","tags":{"host":"server-3","region":"us-east"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}`
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error during COUNT: %s", res.Err)
	} else if s := mustMarshalJSON(res); s != expected {
		t.Fatalf("unexpected row(0) during COUNT:\n  exp: %s\n  got: %s", expected, s)
	}

	results = s.ExecuteQuery(MustParseQuery(`SELECT count(value) FROM cpu GROUP BY * SLIMIT 2 SOFFSET 3`), "foo", nil)
	expected = `{"series":[{"name":"cpu","tags":{"host":"server-4","region":"us-east"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]},{"name":"cpu","tags":{"host":"server-5","region":"us-east"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}`
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error during COUNT: %s", res.Err)
	} else if s := mustMarshalJSON(res); s != expected {
		t.Fatalf("unexpected row(0) during COUNT:\n  exp: %s\n  got: %s", expected, s)
	}

	results = s.ExecuteQuery(MustParseQuery(`SELECT count(value) FROM cpu GROUP BY * SLIMIT 3 SOFFSET 8`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error during COUNT: %s", res.Err)
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","tags":{"host":"server-9","region":"us-east"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}` {
		t.Fatalf("unexpected row(0) during COUNT: %s", s)
	}

	results = s.ExecuteQuery(MustParseQuery(`SELECT count(value) FROM cpu GROUP BY * SLIMIT 3 SOFFSET 20`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error during COUNT: %s", res.Err)
	}
}

// Ensure the server can execute a wildcard query and return the data correctly.
func TestServer_ExecuteWildcardQuery(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")
	s.CreateUser("susy", "pass", false)

	// Write series with one point to the database.
	// We deliberatly write one value per insert as we need to create each field in a predicatable order for testing.
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east"}, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Fields: map[string]interface{}{"value": float64(10)}}})
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east"}, Timestamp: mustParseTime("2000-01-01T00:00:10Z"), Fields: map[string]interface{}{"val-x": 20}}})
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east"}, Timestamp: mustParseTime("2000-01-01T00:00:20Z"), Fields: map[string]interface{}{"value": 30, "val-x": 40}}})

	// Select * (wildcard).
	results := s.ExecuteQuery(MustParseQuery(`SELECT * FROM cpu`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error during SELECT *: %s", res.Err)
	} else if s, e := mustMarshalJSON(res), `{"series":[{"name":"cpu","columns":["time","val-x","value"],"values":[["2000-01-01T00:00:00Z",null,10],["2000-01-01T00:00:10Z",20,null],["2000-01-01T00:00:20Z",40,30]]}]}`; s != e {
		t.Logf("expected                            %s\n", e)
		t.Fatalf("unexpected results during SELECT *: %s", s)
	}
}

// Ensure the server can execute a wildcard GROUP BY
func TestServer_ExecuteWildcardGroupBy(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "raw", Duration: 1 * time.Hour})
	s.SetDefaultRetentionPolicy("foo", "raw")
	s.CreateUser("susy", "pass", false)

	// Write series with one point to the database.
	// We deliberatly write one value per insert as we need to create each field in a predicatable order for testing.
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east"}, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Fields: map[string]interface{}{"value": float64(10)}}})
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east"}, Timestamp: mustParseTime("2000-01-01T00:00:10Z"), Fields: map[string]interface{}{"value": 20}}})
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-west"}, Timestamp: mustParseTime("2000-01-01T00:00:20Z"), Fields: map[string]interface{}{"value": 30}}})

	// GROUP BY * (wildcard).
	results := s.ExecuteQuery(MustParseQuery(`SELECT mean(value) FROM cpu GROUP BY *`), "foo", nil)
	expected := `{"series":[{"name":"cpu","tags":{"region":"us-east"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",15]]},{"name":"cpu","tags":{"region":"us-west"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",30]]}]}`
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error during GROUP BY *: %s", res.Err)
	} else if s := mustMarshalJSON(res); s != expected {
		t.Fatalf("unexpected results during SELECT *:\n  exp: %s\n  got: %s", expected, s)
	}

	// GROUP BY * (wildcard) with time.
	results = s.ExecuteQuery(MustParseQuery(`SELECT mean(value) FROM cpu GROUP BY *,time(1m)`), "foo", nil)
	expected = `{"series":[{"name":"cpu","tags":{"region":"us-east"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",15]]},{"name":"cpu","tags":{"region":"us-west"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",30]]}]}`
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error during GROUP BY *: %s", res.Err)
	} else if s := mustMarshalJSON(res); s != expected {
		t.Fatalf("unexpected results during SELECT *:\n  exp: %s\n  got: %s", expected, s)
	}
}

func TestServer_CreateShardGroupIfNotExist(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")

	if err := s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "bar", Duration: time.Hour}); err != nil {
		t.Fatal(err)
	}

	if err := s.CreateShardGroupIfNotExists("foo", "bar", time.Time{}); err != nil {
		t.Fatal(err)
	}

	// Restart the server to ensure the shard group is not lost.
	s.Restart()

	if a, err := s.ShardGroups("foo"); err != nil {
		t.Fatal(err)
	} else if len(a) != 1 {
		t.Fatalf("expected 1 shard group but found %d", len(a))
	}
}

func TestServer_DeleteShardGroup(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")

	if err := s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "bar", Duration: time.Hour}); err != nil {
		t.Fatal(err)
	}

	if err := s.CreateShardGroupIfNotExists("foo", "bar", time.Time{}); err != nil {
		t.Fatal(err)
	}

	// Get the new shard's ID.
	var g []*influxdb.ShardGroup
	g, err := s.ShardGroups("foo")
	if err != nil {
		t.Fatal(err)
	} else if len(g) != 1 {
		t.Fatalf("expected 1 shard group but found %d", len(g))
	}
	id := g[0].ID

	// Delete the shard group and verify it's gone.
	if err := s.DeleteShardGroup("foo", "bar", id); err != nil {
		t.Fatal(err)
	}
	g, err = s.ShardGroups("foo")
	if err != nil {
		t.Fatal(err)
	} else if len(g) != 0 {
		t.Fatalf("expected 0 shard group but found %d", len(g))
	}
}

/* TODO(benbjohnson): Change test to not expose underlying series ids directly.
func TestServer_Measurements(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("foo")
	s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "mypolicy", Duration: 1 * time.Hour})
	s.CreateUser("susy", "pass", false)

	// Write series with one point to the database.
	timestamp := mustParseTime("2000-01-01T00:00:00Z")

	tags := map[string]string{"host": "servera.influx.com", "region": "uswest"}
	values := map[string]interface{}{"value": 23.2}

	index, err := s.WriteSeries("foo", "mypolicy", []influxdb.Point{influxdb.Point{Name: "cpu_load", Tags: tags, Timestamp: timestamp, Fields: values}})
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
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()

	// Default database with one policy.
	s.CreateDatabase("db0")
	s.CreateRetentionPolicy("db0", &influxdb.RetentionPolicy{Name: "rp0", Duration: time.Hour})
	s.SetDefaultRetentionPolicy("db0", "rp0")

	// Another database with two policies.
	s.CreateDatabase("db1")
	s.CreateRetentionPolicy("db1", &influxdb.RetentionPolicy{Name: "rp1", Duration: time.Hour})
	s.CreateRetentionPolicy("db1", &influxdb.RetentionPolicy{Name: "rp2", Duration: time.Hour})
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
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("db0")
	s.CreateRetentionPolicy("db0", &influxdb.RetentionPolicy{Name: "rp0", Duration: time.Hour})
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

// Ensure the server can create a continuous query
func TestServer_CreateContinuousQuery(t *testing.T) {
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()

	// Create the "foo" database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "bar", Duration: time.Hour}); err != nil {
		t.Fatal(err)
	}
	s.SetDefaultRetentionPolicy("foo", "bar")

	// create and check
	q := "CREATE CONTINUOUS QUERY myquery ON foo BEGIN SELECT count() INTO measure1 FROM myseries GROUP BY time(10m) END"
	stmt, err := influxql.NewParser(strings.NewReader(q)).ParseStatement()
	if err != nil {
		t.Fatalf("error parsing query %s", err.Error())
	}
	cq := stmt.(*influxql.CreateContinuousQueryStatement)
	if err := s.CreateContinuousQuery(cq); err != nil {
		t.Fatalf("error creating continuous query %s", err.Error())
	}

	queries := s.ContinuousQueries("foo")
	cqObj, _ := influxdb.NewContinuousQuery(q)
	expected := []*influxdb.ContinuousQuery{cqObj}
	if mustMarshalJSON(expected) != mustMarshalJSON(queries) {
		t.Fatalf("query not saved:\n\texp: %s\n\tgot: %s", mustMarshalJSON(expected), mustMarshalJSON(queries))
	}
	s.Restart()

	// check again
	queries = s.ContinuousQueries("foo")
	if !reflect.DeepEqual(queries, expected) {
		t.Fatalf("query not saved:\n\texp: %s\ngot: %s", mustMarshalJSON(expected), mustMarshalJSON(queries))
	}
}

// Ensure the server prevents a duplicate named continuous query from being created
func TestServer_CreateContinuousQuery_ErrContinuousQueryExists(t *testing.T) {
	t.Skip("pending")
}

// Ensure the server returns an error when creating a continuous query on a database that doesn't exist
func TestServer_CreateContinuousQuery_ErrDatabaseNotFound(t *testing.T) {
	t.Skip("pending")
}

// Ensure the server returns an error when creating a continuous query on a retention policy that doesn't exist
func TestServer_CreateContinuousQuery_ErrRetentionPolicyNotFound(t *testing.T) {
	t.Skip("pending")
}

func TestServer_CreateContinuousQuery_ErrInfinteLoop(t *testing.T) {
	t.Skip("pending")
}

// Ensure
func TestServer_RunContinuousQueries(t *testing.T) {
	t.Skip()
	c := test.NewMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()

	// Create the "foo" database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRetentionPolicy("foo", &influxdb.RetentionPolicy{Name: "raw"}); err != nil {
		t.Fatal(err)
	}
	s.SetDefaultRetentionPolicy("foo", "raw")

	s.RecomputePreviousN = 50
	s.RecomputeNoOlderThan = time.Second
	s.ComputeRunsPerInterval = 5
	s.ComputeNoMoreThan = 2 * time.Millisecond

	// create cq and check
	q := `CREATE CONTINUOUS QUERY myquery ON foo BEGIN SELECT mean(value) INTO cpu_region FROM cpu GROUP BY time(5ms), region END`
	stmt, err := influxql.NewParser(strings.NewReader(q)).ParseStatement()
	if err != nil {
		t.Fatalf("error parsing query %s", err.Error())
	}
	cq := stmt.(*influxql.CreateContinuousQueryStatement)
	if err := s.CreateContinuousQuery(cq); err != nil {
		t.Fatalf("error creating continuous query %s", err.Error())
	}
	if err := s.RunContinuousQueries(); err != nil {
		t.Fatalf("error running cqs when no data exists: %s", err.Error())
	}

	// set a test time in the middle of a 5 second interval that we can work with
	testTime := time.Now().UTC().Round(5 * time.Millisecond)
	if testTime.UnixNano() > time.Now().UnixNano() {
		testTime = testTime.Add(-5 * time.Millisecond)
	}
	testTime.Add(time.Millisecond * 2)

	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east"}, Timestamp: testTime, Fields: map[string]interface{}{"value": float64(30)}}})
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east"}, Timestamp: testTime.Add(-time.Millisecond * 5), Fields: map[string]interface{}{"value": float64(20)}}})
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-west"}, Timestamp: testTime, Fields: map[string]interface{}{"value": float64(100)}}})

	// Run CQs after a period of time
	time.Sleep(time.Millisecond * 50)
	s.RunContinuousQueries()
	// give the CQs time to run
	time.Sleep(time.Millisecond * 100)

	verify := func(num int, exp string) {
		results := s.ExecuteQuery(MustParseQuery(`SELECT mean(mean) FROM cpu_region GROUP BY region`), "foo", nil)
		if res := results.Results[0]; res.Err != nil {
			t.Fatalf("unexpected error verify %d: %s", num, res.Err)
		} else if len(res.Series) != 2 {
			t.Fatalf("unexpected row count on verify %d: %d", num, len(res.Series))
		} else if s := mustMarshalJSON(res); s != exp {
			t.Fatalf("unexpected row(0) on verify %d: %s", num, s)
		}
	}

	// ensure CQ results were saved
	verify(1, `{"series":[{"name":"cpu_region","tags":{"region":"us-east"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",25]]},{"name":"cpu_region","tags":{"region":"us-west"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",100]]}]}`)

	// ensure that repeated runs don't cause duplicate data
	s.RunContinuousQueries()
	verify(2, `{"series":[{"name":"cpu_region","tags":{"region":"us-east"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",25]]},{"name":"cpu_region","tags":{"region":"us-west"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",100]]}]}`)

	// ensure that data written into a previous window is picked up and the result recomputed.
	time.Sleep(time.Millisecond * 2)
	s.MustWriteSeries("foo", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-west"}, Timestamp: testTime.Add(-time.Millisecond), Fields: map[string]interface{}{"value": float64(50)}}})
	s.RunContinuousQueries()
	// give CQs time to run
	time.Sleep(time.Millisecond * 50)

	verify(3, `{"series":[{"name":"cpu_region","tags":{"region":"us-east"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",25]]},{"name":"cpu_region","tags":{"region":"us-west"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",75]]}]}`)
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
	s := influxdb.NewServer()
	s.SetAuthenticationEnabled(false)
	return &Server{s}
}

// OpenServer returns a new, open test server instance.
func OpenServer(client influxdb.MessagingClient) *Server {
	s := OpenUninitializedServer(client)
	if err := s.Initialize(url.URL{Host: "127.0.0.1:8080"}); err != nil {
		panic(err.Error())
	}
	return s
}

// OpenUninitializedServer returns a new, uninitialized, open test server instance.
func OpenUninitializedServer(client influxdb.MessagingClient) *Server {
	s := NewServer()
	if err := s.Open(tempfile(), client); err != nil {
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
	if err := s.Server.Open(path, client); err != nil {
		panic("open: " + err.Error())
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
	}
	s.Client().(*test.MessagingClient).Sync(index)
	return index
}

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
