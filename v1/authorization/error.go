package authorization

import (
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

var (
	// ErrInvalidAuthID is used when the Authorization's ID cannot be encoded
	ErrInvalidAuthID = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "authorization ID is invalid",
	}

	// ErrAuthNotFound is used when the specified auth cannot be found
	ErrAuthNotFound = &errors.Error{
		Code: errors.ENotFound,
		Msg:  "authorization not found",
	}

	// NotUniqueIDError occurs when attempting to create an Authorization with an ID that already belongs to another one
	NotUniqueIDError = &errors.Error{
		Code: errors.EConflict,
		Msg:  "ID already exists",
	}

	// ErrFailureGeneratingID occurs ony when the random number generator
	// cannot generate an ID in MaxIDGenerationN times.
	ErrFailureGeneratingID = &errors.Error{
		Code: errors.EInternal,
		Msg:  "unable to generate valid id",
	}

	// ErrTokenAlreadyExistsError is used when attempting to create an authorization
	// with a token that already exists
	ErrTokenAlreadyExistsError = &errors.Error{
		Code: errors.EConflict,
		Msg:  "token already exists",
	}
)

// ErrInvalidAuthIDError is used when a service was provided an invalid ID.
func ErrInvalidAuthIDError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInvalid,
		Msg:  "auth id provided is invalid",
		Err:  err,
	}
}

// ErrInternalServiceError is used when the error comes from an internal system.
func ErrInternalServiceError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Err:  err,
	}
}

// UnexpectedAuthIndexError is used when the error comes from an internal system.
func UnexpectedAuthIndexError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving auth index; Err: %v", err),
	}
}
