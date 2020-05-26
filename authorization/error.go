package authorization

import (
	"fmt"

	"github.com/influxdata/influxdb/v2"
)

var (
	// ErrInvalidAuthID is used when the Authorization's ID cannot be encoded
	ErrInvalidAuthID = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "authorization ID is invalid",
	}

	// ErrAuthNotFound is used when the specified auth cannot be found
	ErrAuthNotFound = &influxdb.Error{
		Code: influxdb.ENotFound,
		Msg:  "authorization not found",
	}

	// NotUniqueIDError occurs when attempting to create an Authorization with an ID that already belongs to another one
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

	// ErrTokenAlreadyExistsError is used when attempting to create an authorization
	// with a token that already exists
	ErrTokenAlreadyExistsError = &influxdb.Error{
		Code: influxdb.EConflict,
		Msg:  fmt.Sprintf("token already exists"),
	}
)

// ErrInvalidAuthIDError is used when a service was provided an invalid ID.
func ErrInvalidAuthIDError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "auth id provided is invalid",
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

// UnexpectedAuthIndexError is used when the error comes from an internal system.
func UnexpectedAuthIndexError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving auth index; Err: %v", err),
	}
}
