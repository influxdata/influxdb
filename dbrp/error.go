package dbrp

import (
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
)

var (
	// ErrDBRPNotFound is used when the specified DBRP cannot be found.
	ErrDBRPNotFound = &errors.Error{
		Code: errors.ENotFound,
		Msg:  "unable to find DBRP",
	}

	// ErrNotUniqueID is used when the ID of the DBRP is not unique.
	ErrNotUniqueID = &errors.Error{
		Code: errors.EConflict,
		Msg:  "ID already exists",
	}

	// ErrFailureGeneratingID occurs ony when the random number generator
	// cannot generate an ID in MaxIDGenerationN times.
	ErrFailureGeneratingID = &errors.Error{
		Code: errors.EInternal,
		Msg:  "unable to generate valid id",
	}

	ErrNoOrgProvided = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "either 'org' or 'orgID' must be provided",
	}
)

// ErrOrgNotFound returns a more informative error about a 404 on org name.
func ErrOrgNotFound(org string) error {
	return &errors.Error{
		Code: errors.ENotFound,
		Msg:  fmt.Sprintf("invalid org %q", org),
		Err:  taskmodel.ErrOrgNotFound,
	}
}

// ErrInvalidOrgID returns a more informative error about a failure
// to decode an organization ID.
func ErrInvalidOrgID(id string, err error) error {
	return &errors.Error{
		Code: errors.EInvalid,
		Msg:  fmt.Sprintf("invalid org ID %q", id),
		Err:  err,
	}
}

// ErrInvalidBucketID returns a more informative error about a failure
// to decode a bucket ID.
func ErrInvalidBucketID(id string, err error) error {
	return &errors.Error{
		Code: errors.EInvalid,
		Msg:  fmt.Sprintf("invalid bucket ID %q", id),
		Err:  err,
	}
}

// ErrInvalidDBRPID is used when the ID of the DBRP cannot be encoded.
func ErrInvalidDBRPID(id string, err error) error {
	return &errors.Error{
		Code: errors.EInvalid,
		Msg:  fmt.Sprintf("invalid DBRP ID %q", id),
		Err:  err,
	}
}

// ErrInvalidDBRP is used when a service was provided an invalid DBRP.
func ErrInvalidDBRP(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInvalid,
		Msg:  "DBRP provided is invalid",
		Err:  err,
	}
}

// ErrInternalService is used when the error comes from an internal system.
func ErrInternalService(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Err:  err,
	}
}

// ErrDBRPAlreadyExists is used when there is a conflict in creating a new DBRP.
func ErrDBRPAlreadyExists(msg string) *errors.Error {
	if msg == "" {
		msg = "DBRP already exists"
	}
	return &errors.Error{
		Code: errors.EConflict,
		Msg:  msg,
	}
}
