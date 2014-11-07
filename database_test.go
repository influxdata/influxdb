package influxdb_test

import (
	"reflect"
	"regexp"
	"testing"
	"time"

	"code.google.com/p/go.crypto/bcrypt"
	"github.com/influxdb/influxdb"
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

// Ensure the database can create a new shard space.
func TestDatabase_CreateShardSpace(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a database.
	if err := s.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}

	// Create a shard space on the database.
	ss := &influxdb.ShardSpace{
		Name:      "bar",
		Regex:     regexp.MustCompile(`myseries`),
		Duration:  time.Hour,
		Retention: time.Minute,
		ReplicaN:  2,
		SplitN:    3,
	}
	if err := s.Database("foo").CreateShardSpace(ss); err != nil {
		t.Fatal(err)
	}
	s.Restart()

	// Verify that the user exists.
	if o := s.Database("foo").ShardSpace("bar"); o == nil {
		t.Fatalf("shard space not found")
	} else if !reflect.DeepEqual(ss, o) {
		t.Fatalf("shard space mismatch: %#v", o)
	}
}

// Ensure the server returns an error when creating a shard space after db is dropped.
func TestDatabase_CreateShardSpace_ErrDatabaseNotFound(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a database & drop it.
	s.CreateDatabase("foo")
	db := s.Database("foo")
	s.DeleteDatabase("foo")

	// Create a shard space on the database.
	if err := db.CreateShardSpace(&influxdb.ShardSpace{Name: "bar"}); err != influxdb.ErrDatabaseNotFound {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when creating a shard space without a name.
func TestDatabase_CreateShardSpace_ErrShardSpaceNameRequired(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	if err := s.Database("foo").CreateShardSpace(&influxdb.ShardSpace{Name: ""}); err != influxdb.ErrShardSpaceNameRequired {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when creating a duplicate shard space.
func TestDatabase_CreateShardSpace_ErrShardSpaceExists(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	s.Database("foo").CreateShardSpace(&influxdb.ShardSpace{Name: "bar"})
	if err := s.Database("foo").CreateShardSpace(&influxdb.ShardSpace{Name: "bar"}); err != influxdb.ErrShardSpaceExists {
		t.Fatal(err)
	}
}

// Ensure the server can delete an existing shard space.
func TestDatabase_DeleteShardSpace(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a database and shard space.
	s.CreateDatabase("foo")
	db := s.Database("foo")
	if err := db.CreateShardSpace(&influxdb.ShardSpace{Name: "bar"}); err != nil {
		t.Fatal(err)
	} else if db.ShardSpace("bar") == nil {
		t.Fatal("shard space not created")
	}

	// Remove shard space from database.
	if err := db.DeleteShardSpace("bar"); err != nil {
		t.Fatal(err)
	} else if db.ShardSpace("bar") != nil {
		t.Fatal("shard space not deleted")
	}
	s.Restart()

	if s.Database("foo").ShardSpace("bar") != nil {
		t.Fatal("shard space not deleted after restart")
	}
}

// Ensure the server returns an error when deleting a shard space after db is dropped.
func TestDatabase_DeleteShardSpace_ErrDatabaseNotFound(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()

	// Create a database & drop it.
	s.CreateDatabase("foo")
	db := s.Database("foo")
	s.DeleteDatabase("foo")

	// Delete shard space on the database.
	if err := db.DeleteShardSpace("bar"); err != influxdb.ErrDatabaseNotFound {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when deleting a shard space without a name.
func TestDatabase_DeleteShardSpace_ErrShardSpaceNameRequired(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	if err := s.Database("foo").DeleteShardSpace(""); err != influxdb.ErrShardSpaceNameRequired {
		t.Fatal(err)
	}
}

// Ensure the server returns an error when deleting a non-existent shard space.
func TestDatabase_DeleteShardSpace_ErrShardSpaceNotFound(t *testing.T) {
	s := OpenServer(NewMessagingClient())
	defer s.Close()
	s.CreateDatabase("foo")
	if err := s.Database("foo").DeleteShardSpace("no_such_space"); err != influxdb.ErrShardSpaceNotFound {
		t.Fatal(err)
	}
}

// Ensure the database can execute a query and return the correct results.
func TestDatabase_ExecuteQuery(t *testing.T) { t.Skip("pending") }
