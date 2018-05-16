package errors

import (
	"fmt"
	"net/http"
)

// TODO: move to base directory

const (
	// InternalError indicates an unexpected error condition.
	InternalError = 1
	// MalformedData indicates malformed input, such as unparsable JSON.
	MalformedData = 2
	// InvalidData indicates that data is well-formed, but invalid.
	InvalidData = 3
	// Forbidden indicates a forbidden operation.
	Forbidden = 4
)

// Error indicates an error with a reference code and an HTTP status code.
type Error struct {
	Reference int    `json:"referenceCode"`
	Code      int    `json:"statusCode"`
	Err       string `json:"err"`
}

// SetCode sets the http status code for an error.
func (e *Error) SetCode() {
	switch e.Reference {
	case InternalError:
		e.Code = http.StatusInternalServerError
	case InvalidData:
		e.Code = http.StatusUnprocessableEntity
	case MalformedData:
		e.Code = http.StatusBadRequest
	case Forbidden:
		e.Code = http.StatusForbidden
	default:
		e.Reference = InternalError
		e.Code = http.StatusInternalServerError
	}
}

// Error implements the error interface.
func (e Error) Error() string {
	return fmt.Sprintf("%v (error reference code: %d)", e.Err, e.Reference)
}

// Errorf constructs an Error with the given reference code and format.
func Errorf(ref int, format string, i ...interface{}) error {
	return &Error{
		Reference: ref,
		Err:       fmt.Sprintf(format, i...),
	}
}

// InternalErrorf constructs an InternalError with the given format.
func InternalErrorf(format string, i ...interface{}) error {
	return Errorf(InternalError, format, i...)
}

// MalformedDataf constructs a MalformedData error with the given format.
func MalformedDataf(format string, i ...interface{}) error {
	return Errorf(MalformedData, format, i...)
}

// InvalidDataf constructs an InvalidData error with the given format.
func InvalidDataf(format string, i ...interface{}) error {
	return Errorf(InvalidData, format, i...)
}

// Forbiddenf constructs a Forbidden error with the given format.
func Forbiddenf(format string, i ...interface{}) error {
	return Errorf(Forbidden, format, i...)
}
