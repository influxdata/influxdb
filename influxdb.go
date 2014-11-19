package influxdb

import (
	"errors"
)

var (
	// ErrServerOpen is returned when opening an already open server.
	ErrServerOpen = errors.New("server already open")

	// ErrServerClosed is returned when closing an already closed server.
	ErrServerClosed = errors.New("server already closed")

	// ErrPathRequired is returned when opening a server without a path.
	ErrPathRequired = errors.New("path required")

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

	// ErrInvalidQuery is returned when executing an unknown query type.
	ErrInvalidQuery = errors.New("invalid query")
)
