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
	// NotFound indicates a resource was not found.
	NotFound = 5
)

// Error indicates an error with a reference code and an HTTP status code.
type Error struct {
	Reference int    `json:"referenceCode"`
	Code      int    `json:"statusCode"`
	Err       string `json:"err"`
}

// Error implements the error interface.
func (e Error) Error() string {
	return e.Err
}

// Errorf constructs an Error with the given reference code and format.
func Errorf(ref int, format string, i ...interface{}) error {
	return Error{
		Reference: ref,
		Err:       fmt.Sprintf(format, i...),
	}
}

// New creates a new error with a message and error code.
func New(msg string, ref ...int) error {
	refCode := InternalError
	if len(ref) == 1 {
		refCode = ref[0]
	}
	return Error{
		Reference: refCode,
		Err:       msg,
	}
}

func Wrap(err error, msg string, ref ...int) error {
	if err == nil {
		return nil
	}
	e, ok := err.(Error)
	if ok {
		refCode := e.Reference
		if len(ref) == 1 {
			refCode = ref[0]
		}
		return Error{
			Reference: refCode,
			Code:      e.Code,
			Err:       fmt.Sprintf("%s: %s", msg, e.Err),
		}
	}
	refCode := InternalError
	if len(ref) == 1 {
		refCode = ref[0]
	}
	return Error{
		Reference: refCode,
		Err:       fmt.Sprintf("%s: %s", msg, err.Error()),
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

func BadRequestError(msg string) error {
	return Error{
		Reference: InvalidData,
		Code:      http.StatusBadRequest,
		Err:       msg,
	}
}
