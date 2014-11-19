package influxdb_test

import (
	"reflect"
	"testing"
	"time"

	"code.google.com/p/go.crypto/bcrypt"
	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/influxql"
)

// Ensure the server can create a new user.
func TestDatabase_CreateUser(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}

	// Create a user on the database.
	if err := s.Database("foo").CreateUser("susy", "pass", nil); err != nil {
		t.Fatal(err)
	}
	s.Restart()

	// Verify that the user exists.
	if u := s.Database("foo").User("susy"); u == nil {
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
	s.Restart()

	if s.Database("foo").User("susy") != nil {
		t.Fatal("user not deleted after restart")
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
	s.Restart()

	if bcrypt.CompareHashAndPassword([]byte(s.Database("foo").User("susy").Hash), []byte("newpass")) != nil {
		t.Fatal("invalid new password after restart")
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
	s.Restart()

	// Retrieve a list of all users for "foo" (sorted by name).
	if a := s.Database("foo").Users(); len(a) != 2 {
		t.Fatalf("unexpected user count: %d", len(a))
	} else if a[0].Name != "john" {
		t.Fatalf("unexpected user(0): %s", a[0].Name)
	} else if a[1].Name != "susy" {
		t.Fatalf("unexpected user(1): %s", a[1].Name)
	}
}

// Ensure the database can create a new retention policy.
func TestDatabase_CreateRetentionPolicy(t *testing.T) {
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
	if err := s.Database("foo").CreateRetentionPolicy(rp); err != nil {
		t.Fatal(err)
	}
	s.Restart()

	// Verify that the policy exists.
	if o := s.Database("foo").RetentionPolicy("bar"); o == nil {
		t.Fatalf("retention policy not found")
	} else if !reflect.DeepEqual(rp, o) {
		t.Fatalf("retention policy mismatch: %#v", o)
	}
}

// Ensure the server returns an error when creating a retention policy after db is dropped.
func TestDatabase_CreateRetentionPolicy_ErrDatabaseNotFound(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a database & drop it.
	s.CreateDatabase("foo")
	db := s.Database("foo")
	s.DeleteDatabase("foo")

	// Create a retention policy on the database.
	if err := db.CreateRetentionPolicy(&influxdb.RetentionPolicy{Name: "bar"}); err != influxdb.ErrDatabaseNotFound {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when creating a retention policy without a name.
func TestDatabase_CreateRetentionPolicy_ErrRetentionPolicyNameRequired(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	if err := s.Database("foo").CreateRetentionPolicy(&influxdb.RetentionPolicy{Name: ""}); err != influxdb.ErrRetentionPolicyNameRequired {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when creating a duplicate retention policy.
func TestDatabase_CreateRetentionPolicy_ErrRetentionPolicyExists(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	s.Database("foo").CreateRetentionPolicy(&influxdb.RetentionPolicy{Name: "bar"})
	if err := s.Database("foo").CreateRetentionPolicy(&influxdb.RetentionPolicy{Name: "bar"}); err != influxdb.ErrRetentionPolicyExists {
		t.Fatal(err)
	}
}

// Ensure the server can delete an existing retention policy.
func TestDatabase_DeleteRetentionPolicy(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a database and retention policy.
	s.CreateDatabase("foo")
	db := s.Database("foo")
	if err := db.CreateRetentionPolicy(&influxdb.RetentionPolicy{Name: "bar"}); err != nil {
		t.Fatal(err)
	} else if db.RetentionPolicy("bar") == nil {
		t.Fatal("retention policy not created")
	}

	// Remove retention policy from database.
	if err := db.DeleteRetentionPolicy("bar"); err != nil {
		t.Fatal(err)
	} else if db.RetentionPolicy("bar") != nil {
		t.Fatal("retention policy not deleted")
	}
	s.Restart()

	if s.Database("foo").RetentionPolicy("bar") != nil {
		t.Fatal("retention policy not deleted after restart")
	}
}

// Ensure the server returns an error when deleting a retention policy after db is dropped.
func TestDatabase_DeleteRetentionPolicy_ErrDatabaseNotFound(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a database & drop it.
	s.CreateDatabase("foo")
	db := s.Database("foo")
	s.DeleteDatabase("foo")

	// Delete retention policy on the database.
	if err := db.DeleteRetentionPolicy("bar"); err != influxdb.ErrDatabaseNotFound {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when deleting a retention policy without a name.
func TestDatabase_DeleteRetentionPolicy_ErrRetentionPolicyNameRequired(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	if err := s.Database("foo").DeleteRetentionPolicy(""); err != influxdb.ErrRetentionPolicyNameRequired {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when deleting a non-existent retention policy.
func TestDatabase_DeleteRetentionPolicy_ErrRetentionPolicyNotFound(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	if err := s.Database("foo").DeleteRetentionPolicy("no_such_policy"); err != influxdb.ErrRetentionPolicyNotFound {
		t.Fatal(err)
	}
}

// Ensure the server can set the default retention policy
func TestDatabase_SetDefaultRetentionPolicy(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	db := s.Database("foo")

	rp := &influxdb.RetentionPolicy{Name: "bar"}
	if err := db.CreateRetentionPolicy(rp); err != nil {
		t.Fatal(err)
	} else if db.RetentionPolicy("bar") == nil {
		t.Fatal("retention policy not created")
	}

	// Set bar as default
	if err := db.SetDefaultRetentionPolicy("bar"); err != nil {
		t.Fatal(err)
	}

	o := db.DefaultRetentionPolicy()
	if o == nil {
		t.Fatal("default policy not set")
	} else if !reflect.DeepEqual(rp, o) {
		t.Fatalf("retention policy mismatch: %#v", o)
	}

	s.Restart()

	o = s.Database("foo").DefaultRetentionPolicy()
	if o == nil {
		t.Fatal("default policy not kept after restart")
	} else if !reflect.DeepEqual(rp, o) {
		t.Fatalf("retention policy mismatch after restart: %#v", o)
	}
}

// Ensure the server returns an error when setting the deafult retention policy to a non-existant one.
func TestDatabase_SetDefaultRetentionPolicy_ErrRetentionPolicyNotFound(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	if err := s.Database("foo").SetDefaultRetentionPolicy("no_such_policy"); err != influxdb.ErrRetentionPolicyNotFound {
		t.Fatal(err)
	}
}

// Ensure the database can write data to the database.
func TestDatabase_WriteSeries(t *testing.T) {
	t.Skip("pending")

	/* TEMPORARILY REMOVED FOR PROTOBUFS.
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	db := s.Database("foo")
	db.CreateRetentionPolicys(&influxdb.RetentionPolicy{Name: "myspace", Duration: 1 * time.Hour})
	db.CreateUser("susy", "pass", nil)

	// Write series with one point to the database.
	timestamp := mustParseMicroTime("2000-01-01T00:00:00Z")
	series := &protocol.Series{
		Name:   proto.String("cpu_load"),
		Fields: []string{"myval"},
		Points: []*protocol.Point{
			{
				Values:    []*protocol.FieldValue{{Int64Value: proto.Int64(100)}},
				Timestamp: proto.Int64(timestamp),
			},
		},
	}
	if err := db.WriteSeries(series); err != nil {
		t.Fatal(err)
	}

	// Execute a query and record all series found.
		q := mustParseQuery(`select myval from cpu_load`)
		if err := db.ExecuteQuery(q); err != nil {
			t.Fatal(err)
		}
	*/
}

// mustParseQuery parses a query string into a query object. Panic on error.
func mustParseQuery(s string) influxql.Query {
	q, err := influxql.Parse(s)
	if err != nil {
		panic(err.Error())
	}
	return q
}
