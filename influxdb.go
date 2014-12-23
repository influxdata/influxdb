package influxdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

var (
	// ErrServerOpen is returned when opening an already open server.
	ErrServerOpen = errors.New("server already open")

	// ErrServerClosed is returned when closing an already closed server.
	ErrServerClosed = errors.New("server already closed")

	// ErrPathRequired is returned when opening a server without a path.
	ErrPathRequired = errors.New("path required")

	// ErrDatabaseNameRequired is returned when creating a database without a name.
	ErrDatabaseNameRequired = errors.New("database name required")

	// ErrDatabaseExists is returned when creating a duplicate database.
	ErrDatabaseExists = errors.New("database exists")

	// ErrDatabaseNotFound is returned when dropping a non-existent database.
	ErrDatabaseNotFound = errors.New("database not found")

	// ErrDatabaseRequired is returned when using a blank database name.
	ErrDatabaseRequired = errors.New("database required")

	// ErrClusterAdminExists is returned when creating a duplicate admin.
	ErrClusterAdminExists = errors.New("cluster admin exists")

	// ErrClusterAdminNotFound is returned when deleting a non-existent admin.
	ErrClusterAdminNotFound = errors.New("cluster admin not found")

	// ErrUserExists is returned when creating a duplicate user.
	ErrUserExists = errors.New("user exists")

	// ErrUserNotFound is returned when deleting a non-existent user.
	ErrUserNotFound = errors.New("user not found")

	// ErrUsernameRequired is returned when using a blank username.
	ErrUsernameRequired = errors.New("username required")

	// ErrInvalidUsername is returned when using a username with invalid characters.
	ErrInvalidUsername = errors.New("invalid username")

	// ErrRetentionPolicyExists is returned when creating a duplicate shard space.
	ErrRetentionPolicyExists = errors.New("retention policy exists")

	// ErrRetentionPolicyNotFound is returned when deleting a non-existent shard space.
	ErrRetentionPolicyNotFound = errors.New("retention policy not found")

	// ErrRetentionPolicyNameRequired is returned using a blank shard space name.
	ErrRetentionPolicyNameRequired = errors.New("retention policy name required")

	// ErrShardNotFound is returned writing to a non-existent shard.
	ErrShardNotFound = errors.New("shard not found")

	// ErrReadAccessDenied is returned when a user attempts to read
	// data that he or she does not have permission to read.
	ErrReadAccessDenied = errors.New("read access denied")

	// ErrReadWritePermissionsRequired is returned when required read/write permissions aren't provided.
	ErrReadWritePermissionsRequired = errors.New("read/write permissions required")

	// ErrInvalidQuery is returned when executing an unknown query type.
	ErrInvalidQuery = errors.New("invalid query")

	// ErrSeriesNotFound is returned when looking up a non-existent series by database, name and tags
	ErrSeriesNotFound = errors.New("series not found")

	// ErrSeriesExists is returned when attempting to set the id of a series by database, name and tags that already exists
	ErrSeriesExists = errors.New("series already exists")
)

// mustMarshal encodes a value to JSON.
// This will panic if an error occurs. This should only be used internally when
// an invalid marshal will cause corruption and a panic is appropriate.
func mustMarshalJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("marshal: " + err.Error())
	}
	return b
}

// mustUnmarshalJSON decodes a value from JSON.
// This will panic if an error occurs. This should only be used internally when
// an invalid unmarshal will cause corruption and a panic is appropriate.
func mustUnmarshalJSON(b []byte, v interface{}) {
	if err := json.Unmarshal(b, v); err != nil {
		panic("unmarshal: " + err.Error())
	}
}

// assert will panic with a given formatted message if the given condition is false.
func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }
