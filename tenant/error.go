package tenant

import (
	"fmt"
	"strings"

	"github.com/influxdata/influxdb/v2"
)

var (
	// ErrNameisEmpty is when a name is empty
	ErrNameisEmpty = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "name is empty",
	}

	// NotUniqueIDError is used when attempting to create an org or bucket that already
	// exists.
	NotUniqueIDError = &influxdb.Error{
		Code: influxdb.EConflict,
		Msg:  "ID already exists",
	}

	// ErrFailureGeneratingID occurs ony when the random number generator
	// cannot generate an ID in MaxIDGenerationN times.
	ErrFailureGeneratingID = &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  "unable to generate valid id",
	}

	// ErrOnboardingNotAllowed occurs when request to onboard comes in and we are not allowing this request
	ErrOnboardingNotAllowed = &influxdb.Error{
		Code: influxdb.EConflict,
		Msg:  "onboarding has already been completed",
	}

	ErrOnboardInvalid = &influxdb.Error{
		Code: influxdb.EEmptyValue,
		Msg:  "onboard failed, missing value",
	}

	ErrNotFound = &influxdb.Error{
		Code: influxdb.ENotFound,
		Msg:  "not found",
	}

	ErrPasswordTooShort = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "password must be at least 8 characters",
	}
)

// ErrCorruptID the ID stored in the Store is corrupt.
func ErrCorruptID(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "corrupt ID provided",
		Err:  err,
	}
}

// ErrInternalServiceError is used when the error comes from an internal system.
func ErrInternalServiceError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Err:  err,
	}
}

type errSlice []error

func (e errSlice) Error() string {
	l := len(e)
	sb := strings.Builder{}
	for i, err := range e {
		if i > 0 {
			sb.WriteRune('\n')
		}
		sb.WriteString(fmt.Sprintf("error %d/%d: %s", i+1, l, err.Error()))
	}
	return sb.String()
}

// AggregateError enables composing multiple errors.
// This is ideal in the case that you are applying functions with side effects to a slice of elements.
// E.g., deleting/updating a slice of resources.
type AggregateError struct {
	errs errSlice
}

// NewAggregateError returns a new AggregateError.
func NewAggregateError() *AggregateError {
	return &AggregateError{
		errs: make([]error, 0),
	}
}

// Add adds an error to the aggregate.
func (e *AggregateError) Add(err error) {
	if err == nil {
		return
	}
	e.errs = append(e.errs, err)
}

// Err returns a proper error from this aggregate error.
func (e *AggregateError) Err() error {
	if len(e.errs) > 0 {
		return e.errs
	}
	return nil
}
