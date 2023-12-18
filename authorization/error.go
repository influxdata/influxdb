package authorization

import (
	"errors"
	"fmt"

	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
)

var (
	// ErrInvalidAuthID is used when the Authorization's ID cannot be encoded
	ErrInvalidAuthID = &errors2.Error{
		Code: errors2.EInvalid,
		Msg:  "authorization ID is invalid",
	}

	// ErrAuthNotFound is used when the specified auth cannot be found
	ErrAuthNotFound = &errors2.Error{
		Code: errors2.ENotFound,
		Msg:  "authorization not found",
	}

	// NotUniqueIDError occurs when attempting to create an Authorization with an ID that already belongs to another one
	NotUniqueIDError = &errors2.Error{
		Code: errors2.EConflict,
		Msg:  "ID already exists",
	}

	// ErrFailureGeneratingID occurs ony when the random number generator
	// cannot generate an ID in MaxIDGenerationN times.
	ErrFailureGeneratingID = &errors2.Error{
		Code: errors2.EInternal,
		Msg:  "unable to generate valid id",
	}

	// ErrTokenAlreadyExistsError is used when attempting to create an authorization
	// with a token that already exists
	ErrTokenAlreadyExistsError = &errors2.Error{
		Code: errors2.EConflict,
		Msg:  "token already exists",
	}
)

// ErrInvalidAuthIDError is used when a service was provided an invalid ID.
func ErrInvalidAuthIDError(err error) *errors2.Error {
	return &errors2.Error{
		Code: errors2.EInvalid,
		Msg:  "auth id provided is invalid",
		Err:  err,
	}
}

// UnexpectedAuthIndexError is used when the error comes from an internal system.
func UnexpectedAuthIndexError(err error) *errors2.Error {
	var e *errors2.Error
	if !errors.As(err, &e) {
		e = &errors2.Error{
			Msg:  fmt.Sprintf("unexpected error retrieving auth index; Err: %v", err),
			Code: errors2.EInternal,
			Err:  err,
		}
	}
	if e.Code == "" {
		e.Code = errors2.EInternal
	}
	return e
}
