package common

import (
	"fmt"
)

const (
	WrongNumberOfArguments = iota
	InvalidArgument
	InternalError
)

type QueryError struct {
	ErrorCode int
	ErrorMsg  string
}

func (self *QueryError) Error() string {
	return self.ErrorMsg
}

func NewQueryError(code int, msg string, args ...interface{}) *QueryError {
	return &QueryError{code, fmt.Sprintf(msg, args...)}
}

type AuthenticationError string

func (self AuthenticationError) Error() string {
	return string(self)
}

func NewAuthenticationError(formatStr string, args ...interface{}) AuthenticationError {
	return AuthenticationError(fmt.Sprintf(formatStr, args...))
}

type AuthorizationError string

func (self AuthorizationError) Error() string {
	return string(self)
}

func NewAuthorizationError(formatStr string, args ...interface{}) AuthorizationError {
	return AuthorizationError(fmt.Sprintf(formatStr, args...))
}

type DatabaseExistsError string

func (self DatabaseExistsError) Error() string {
	return string(self)
}

func NewDatabaseExistsError(db string) DatabaseExistsError {
	return DatabaseExistsError(fmt.Sprintf("database %s exists", db))
}
