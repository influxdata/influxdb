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
	"regexp"
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()

	s.SetAuthenticationEnabled(true)

	adminOnlyQuery := &influxql.Query{
		Statements: []influxql.Statement{
			&influxql.DropDatabaseStatement{Name: "foo"},
		},
	}

	e := s.Authorize(nil, adminOnlyQuery, "foo")
	if _, ok := e.(influxdb.ErrAuthorize); !ok {
		t.Fatalf("unexpected error.  expected %v, actual: %v", influxdb.ErrAuthorize{}, e)
	}

	// Create normal database user.
	s.CreateUser("user1", "user1", false)
	user1 := s.User("user1")

	e = s.Authorize(user1, adminOnlyQuery, "foo")
	if _, ok := e.(influxdb.ErrAuthorize); !ok {
		t.Fatalf("unexpected error.  expected %v, actual: %v", influxdb.ErrAuthorize{}, e)
	}
}

// Test user privilege authorization.
func TestServer_UserPrivilegeAuthorization(t *testing.T) {
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
					Target: &influxql.Target{Measurement: &influxql.Measurement{
						Database: "bar",
						Name:     "measure1",
					},
					},
					Sources: []influxql.Source{&influxql.Measurement{Name: "myseries"}},
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
	c := test.NewDefaultMessagingClient()
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
				Fields:  []*influxql.Field{{Expr: &influxql.Call{Name: "count"}}},
				Sources: []influxql.Source{&influxql.Measurement{Name: "cpu"}},
			},

			// Statement that requires write.
			&influxql.SelectStatement{
				Fields:  []*influxql.Field{{Expr: &influxql.Call{Name: "count"}}},
				Sources: []influxql.Source{&influxql.Measurement{Name: "cpu"}},
				Target:  &influxql.Target{Measurement: &influxql.Measurement{Name: "tmp"}},
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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

// Ensure the server can return a list of all databases.
func TestServer_Databases(t *testing.T) {
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	if err := s.CreateUser("", "pass", false); err != influxdb.ErrUsernameRequired {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when creating a duplicate user.
func TestServer_CreateUser_ErrUserExists(t *testing.T) {
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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

// Ensure the server returns an error when creating a retention policy without a name.
func TestServer_CreateRetentionPolicy_ErrRetentionPolicyNameRequired(t *testing.T) {
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
	results := s.executeQuery(MustParseQuery(`ALTER RETENTION POLICY bar ON foo DURATION 1h`), "foo", nil)
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
	results = s.executeQuery(MustParseQuery(`ALTER RETENTION POLICY bar ON foo DURATION INF`), "foo", nil)
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
	c := test.NewDefaultMessagingClient()
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
	results := s.executeQuery(MustParseQuery(`ALTER RETENTION POLICY bar ON foo DURATION 1m`), "foo", nil)
	if results.Error() == nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}
}

// Ensure the server can delete an existing retention policy.
func TestServer_DeleteRetentionPolicy(t *testing.T) {
	c := test.NewDefaultMessagingClient()
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

// Ensure the server returns an error when deleting a retention policy without a name.
func TestServer_DeleteRetentionPolicy_ErrRetentionPolicyNameRequired(t *testing.T) {
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	if err := s.StartRetentionPolicyEnforcement(time.Duration(0)); err == nil {
		t.Fatal("failed to prohibit retention policies zero check interval")
	}
}

func TestServer_EnforceRetentionPolices(t *testing.T) {
	c := test.NewDefaultMessagingClient()
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

// Ensure the server can drop a measurement.
func TestServer_DropMeasurement(t *testing.T) {
	c := test.NewDefaultMessagingClient()
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
	results := s.executeQuery(MustParseQuery(`SHOW MEASUREMENTS`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"measurements","columns":["name"],"values":[["cpu"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	// Ensure series exists
	results = s.executeQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["_id","host","region"],"values":[[1,"serverA","uswest"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	// Drop measurement
	results = s.executeQuery(MustParseQuery(`DROP MEASUREMENT cpu`), "foo", nil)
	if results.Error() != nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}

	results = s.executeQuery(MustParseQuery(`SHOW MEASUREMENTS`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 0 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	results = s.executeQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 0 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{}` {
		t.Fatalf("unexpected row(0): %s", s)
	}
}

// Ensure the server can drop a series.
func TestServer_DropSeries(t *testing.T) {
	c := test.NewDefaultMessagingClient()
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
	results := s.executeQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["_id","host","region"],"values":[[1,"serverA","uswest"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	// Drop series
	results = s.executeQuery(MustParseQuery(`DROP SERIES FROM cpu`), "foo", nil)
	if results.Error() != nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}

	results = s.executeQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
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
	c := test.NewDefaultMessagingClient()
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
	results := s.executeQuery(MustParseQuery(`DROP SERIES FROM memory`), "foo", nil)
	if results.Error() != nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}

	results = s.executeQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["_id","host","region"],"values":[[1,"serverA","uswest"]]}]}` {
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
	c := test.NewDefaultMessagingClient()
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

	results := s.executeQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["_id","host","region"],"values":[[1,"serverA","uswest"],[2,"serverB","uswest"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	results = s.executeQuery(MustParseQuery(`DROP SERIES FROM cpu where host='serverA'`), "foo", nil)
	if results.Error() != nil {
		t.Fatalf("unexpected error: %s", results.Error())
	}

	results = s.executeQuery(MustParseQuery(`SHOW SERIES`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["_id","host","region"],"values":[[2,"serverB","uswest"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	results = s.executeQuery(MustParseQuery(`SELECT * FROM cpu where host='serverA'`), "foo", nil)
	if len(results.Results) == 0 {
		t.Fatal("expected results to be non-empty")
	} else if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 0 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	}

	results = s.executeQuery(MustParseQuery(`SELECT * FROM cpu where host='serverB'`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:01Z",33.2]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	results = s.executeQuery(MustParseQuery(`SELECT * FROM cpu where region='uswest'`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:01Z",33.2]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}
}

// Ensure the server respects limit and offset in show series queries
func TestServer_ShowSeriesLimitOffset(t *testing.T) {
	c := test.NewDefaultMessagingClient()
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
	results := s.executeQuery(MustParseQuery(`SHOW SERIES LIMIT 3 OFFSET 1`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 2 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["_id","host","region"],"values":[[2,"serverB","us-east"],[3,"serverC","us-west"]]},{"name":"memory","columns":["_id","host","region"],"values":[[4,"serverB","us-west"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	// Select data from the server.
	results = s.executeQuery(MustParseQuery(`SHOW SERIES LIMIT 2 OFFSET 4`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 1 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"memory","columns":["_id","host","region"],"values":[[5,"serverA","us-east"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}

	// Select data from the server.
	results = s.executeQuery(MustParseQuery(`SHOW SERIES LIMIT 2 OFFSET 20`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 0 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	}

	// Select data from the server.
	results = s.executeQuery(MustParseQuery(`SHOW SERIES LIMIT 4 OFFSET 0`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 2 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	}

	// Select data from the server.
	results = s.executeQuery(MustParseQuery(`SHOW SERIES LIMIT 20`), "foo", nil)
	if res := results.Results[0]; res.Err != nil {
		t.Fatalf("unexpected error: %s", res.Err)
	} else if len(res.Series) != 2 {
		t.Fatalf("unexpected row count: %d", len(res.Series))
	} else if s := mustMarshalJSON(res); s != `{"series":[{"name":"cpu","columns":["_id","host","region"],"values":[[1,"serverA","us-east"],[2,"serverB","us-east"],[3,"serverC","us-west"]]},{"name":"memory","columns":["_id","host","region"],"values":[[4,"serverB","us-west"],[5,"serverA","us-east"]]}]}` {
		t.Fatalf("unexpected row(0): %s", s)
	}
}

func TestServer_CreateShardGroupIfNotExist(t *testing.T) {
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
	c := test.NewDefaultMessagingClient()
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
		in         *influxql.Measurement
		db         string // current database
		out        string // normalized string
		err        string // error, if any
		errPattern string // regex pattern
	}{
		{in: &influxql.Measurement{Name: `cpu`}, db: `db0`, out: `"db0"."rp0".cpu`},
		{in: &influxql.Measurement{RetentionPolicy: `rp0`, Name: `cpu`}, db: `db0`, out: `"db0"."rp0".cpu`},
		{in: &influxql.Measurement{Database: `db0`, RetentionPolicy: `rp0`, Name: `cpu`}, db: `db0`, out: `"db0"."rp0".cpu`},
		{in: &influxql.Measurement{Database: `db0`, Name: `cpu`}, db: `db0`, out: `"db0"."rp0".cpu`},

		{in: &influxql.Measurement{}, db: `db0`, err: `invalid measurement`},
		{in: &influxql.Measurement{Database: `no_db`, Name: `cpu`}, db: `db0`, errPattern: `database not found: no_db.*`},
		{in: &influxql.Measurement{Database: `db2`, Name: `cpu`}, db: `db0`, err: `default retention policy not set for: db2`},
		{in: &influxql.Measurement{Database: `db2`, RetentionPolicy: `no_policy`, Name: `cpu`}, db: `db0`, err: `retention policy does not exist: db2.no_policy`},
	}

	// Create server with a variety of databases, retention policies, and measurements
	c := test.NewDefaultMessagingClient()
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
		var re *regexp.Regexp
		if tt.errPattern != "" {
			re = regexp.MustCompile(tt.errPattern)
		}

		err := s.NormalizeMeasurement(tt.in, tt.db)
		if tt.err != "" && tt.err != errstr(err) {
			t.Errorf("%d. error:\n  exp: %s\n  got: %s\n", i, tt.err, errstr(err))
		} else if re != nil && !re.MatchString(err.Error()) {
			t.Errorf("%d. error:\n  exp: %s\n  got: %s\n", i, tt.errPattern, tt.in.String())
		} else if tt.err == "" && re == nil && tt.in.String() != tt.out {
			t.Errorf("%d. error:\n  exp: %s\n  got: %s\n", i, tt.out, tt.in.String())
		}
	}
}

// Ensure the server can normalize all statements in query.
func TestServer_NormalizeQuery(t *testing.T) {
	var tests = []struct {
		in         string // input query
		db         string // default database
		out        string // output query
		err        string // error, if any
		errPattern string // regex pattern
	}{
		{
			in: `SELECT value FROM cpu`, db: `db0`,
			out: `SELECT value FROM "db0"."rp0".cpu`,
		},

		{
			in: `SELECT value FROM cpu`, db: `no_db`, errPattern: `database not found: no_db.*`,
		},
	}

	// Start server with database & retention policy.
	c := test.NewDefaultMessagingClient()
	defer c.Close()
	s := OpenServer(c)
	defer s.Close()
	s.CreateDatabase("db0")
	s.CreateRetentionPolicy("db0", &influxdb.RetentionPolicy{Name: "rp0", Duration: time.Hour})
	s.SetDefaultRetentionPolicy("db0", "rp0")

	// Execute the tests
	for i, tt := range tests {
		var re *regexp.Regexp
		if tt.errPattern != "" {
			re = regexp.MustCompile(tt.errPattern)
		}

		out := MustParseQuery(tt.in)
		err := s.NormalizeStatement(out.Statements[0], tt.db)
		if tt.err != "" && tt.err != errstr(err) {
			t.Errorf("%d. error:\n  exp: %s\n  got: %s\n", i, tt.err, errstr(err))
		} else if re != nil && !re.MatchString(err.Error()) {
			t.Errorf("%d. error:\n  exp: %s\n  got: %s\n", i, tt.errPattern, tt.in)
		} else if err == nil && re == nil && tt.out != out.String() {
			t.Errorf("%d. error:\n  exp: %s\n  got: %s\n", i, tt.out, out.String())
		}
	}
}

// Ensure the server can create a continuous query
func TestServer_CreateContinuousQuery(t *testing.T) {
	c := test.NewDefaultMessagingClient()
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

func TestServer_DropContinuousQuery(t *testing.T) {
	c := test.NewDefaultMessagingClient()
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
	ccq := stmt.(*influxql.CreateContinuousQueryStatement)
	if err := s.CreateContinuousQuery(ccq); err != nil {
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

	// drop and check
	q = "DROP CONTINUOUS QUERY myquery ON foo"
	stmt, err = influxql.NewParser(strings.NewReader(q)).ParseStatement()
	if err != nil {
		t.Fatalf("error parsing query %s", err.Error())
	}
	dcq := stmt.(*influxql.DropContinuousQueryStatement)
	if err := s.DropContinuousQuery(dcq); err != nil {
		t.Fatalf("error dropping continuous query %s", err.Error())
	}

	queries = s.ContinuousQueries("foo")
	if len(queries) != 0 {
		t.Fatalf("continuous query didn't get dropped")
	}
}

// Ensure continuous queries run
func TestServer_RunContinuousQueries(t *testing.T) {
	t.Skip("skipped pending fix for issue #2218")
	c := test.NewDefaultMessagingClient()
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
		results := s.executeQuery(MustParseQuery(`SELECT mean(mean) FROM cpu_region GROUP BY region`), "foo", nil)
		if res := results.Results[0]; res.Err != nil {
			t.Fatalf("unexpected error verify %d: %s", num, res.Err)
		} else if len(res.Series) != 2 {
			t.Fatalf("unexpected row count on verify %d: %d", num, len(res.Series))
		} else if s := mustMarshalJSON(res); s != exp {
			t.Log("exp: ", exp)
			t.Log("got: ", s)
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
	time.Sleep(time.Millisecond * 100)

	verify(3, `{"series":[{"name":"cpu_region","tags":{"region":"us-east"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",25]]},{"name":"cpu_region","tags":{"region":"us-west"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",75]]}]}`)
}

// Ensure the server can create a snapshot writer.
func TestServer_CreateSnapshotWriter(t *testing.T) {
	c := test.NewDefaultMessagingClient()
	s := OpenServer(c)
	defer s.Close()

	// Write metadata.
	s.CreateDatabase("db")
	s.CreateRetentionPolicy("db", &influxdb.RetentionPolicy{Name: "raw", Duration: 1 * time.Hour})
	s.CreateUser("susy", "pass", false)

	// Write one point.
	index, err := s.WriteSeries("db", "raw", []influxdb.Point{{Name: "cpu", Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Fields: map[string]interface{}{"value": float64(100)}}})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second) // FIX: Sync on shard.

	// Create snapshot writer.
	sw, err := s.CreateSnapshotWriter()
	if err != nil {
		t.Fatal(err)
	}
	defer sw.Close()

	// Verify snapshot is correct.
	//
	// NOTE: Sizes and indices here are subject to change.
	// They are tracked here so that we can see when they change over time.
	if len(sw.Snapshot.Files) != 2 {
		t.Fatalf("unexpected file count: %d", len(sw.Snapshot.Files))
	} else if !reflect.DeepEqual(sw.Snapshot.Files[0], influxdb.SnapshotFile{Name: "meta", Size: 45056, Index: 6}) {
		t.Fatalf("unexpected file(0): %#v", sw.Snapshot.Files[0])
	} else if !reflect.DeepEqual(sw.Snapshot.Files[1], influxdb.SnapshotFile{Name: "shards/1", Size: 24576, Index: index}) {
		t.Fatalf("unexpected file(1): %#v", sw.Snapshot.Files[1])
	}

	// Write to buffer to verify that it does not error or panic.
	var buf bytes.Buffer
	if _, err := sw.WriteTo(&buf); err != nil {
		t.Fatal(err)
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

func (s *Server) executeQuery(q *influxql.Query, db string, user *influxdb.User) influxdb.Response {
	results, err := s.ExecuteQuery(q, db, user, 10000)
	if err != nil {
		return influxdb.Response{Err: err}
	}
	res := influxdb.Response{}
	for r := range results {
		l := len(res.Results)
		if l == 0 {
			res.Results = append(res.Results, r)
		} else if res.Results[l-1].StatementID == r.StatementID {
			res.Results[l-1].Series = append(res.Results[l-1].Series, r.Series...)
		} else {
			res.Results = append(res.Results, r)
		}
	}
	return res
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
