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

	// ErrDatabaseExists is returned when creating a database with the same
	// name as an existing database.
	ErrDatabaseExists = errors.New("database exists")

	// ErrDatabaseNotFound is returned when dropping a non-existent database.
	ErrDatabaseNotFound = errors.New("database not found")
)

const (
	WrongNumberOfArguments = iota
	InvalidArgument
	InternalError
)

// QueryError represents an error related to a query.
type QueryError struct {
	Code    int
	Message string
}

// NewQueryError returns a new QueryError instance.
func NewQueryError(code int, msg string, args ...interface{}) *QueryError {
	return &QueryError{code, fmt.Sprintf(msg, args...)}
}

// Error returns the string representation of the error.
func (e *QueryError) Error() string {
	return e.Message
}

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
