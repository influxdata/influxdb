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

type AuthorizationError string

func (self AuthorizationError) Error() string {
	return string(self)
}

func NewAuthorizationError(formatStr string, args ...interface{}) AuthorizationError {
	return AuthorizationError(fmt.Sprintf(formatStr, args...))
}
