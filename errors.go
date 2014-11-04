package influxdb

import (
	"errors"
	"fmt"
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

	// ErrShardSpaceExists is returned when creating a duplicate shard space.
	ErrShardSpaceExists = errors.New("shard space exists")

	// ErrShardSpaceNotFound is returned when deleting a non-existent shard space.
	ErrShardSpaceNotFound = errors.New("shard space not found")

	// ErrShardSpaceNameRequired is returned using a blank shard space name.
	ErrShardSpaceNameRequired = errors.New("shard space name required")

	// ErrReadAccessDenied is returned when a user attempts to read
	// data that he or she does not have permission to read.
	ErrReadAccessDenied = errors.New("read access denied")
)

// AuthenticationError represents an error related to authentication.
type AuthenticationError string

// NewAuthenticationError returns a new AuthenticationError instance.
func NewAuthenticationError(formatStr string, args ...interface{}) AuthenticationError {
	return AuthenticationError(fmt.Sprintf(formatStr, args...))
}

// Error returns the string representation of the error.
func (e AuthenticationError) Error() string {
	return string(e)
}

// AuthorizationError represents an error related to authorization.
type AuthorizationError string

// NewAuthorizationError returns a new AuthorizationError instance.
func NewAuthorizationError(formatStr string, args ...interface{}) AuthorizationError {
	return AuthorizationError(fmt.Sprintf(formatStr, args...))
}

// Error returns the string representation of the error.
func (e AuthorizationError) Error() string {
	return string(e)
}

// DatabaseExistsError represents an error returned when creating an already
// existing database.
type DatabaseExistsError string

// NewDatabaseExistsError returns a new DatabaseExistsError instance.
func NewDatabaseExistsError(db string) DatabaseExistsError {
	return DatabaseExistsError(fmt.Sprintf("database %s exists", db))
}

// Error returns the string representation of the error.
func (e DatabaseExistsError) Error() string {
	return string(e)
}
