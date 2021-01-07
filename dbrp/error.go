package dbrp

import (
	"fmt"

	"github.com/influxdata/influxdb/v2"
)

var (
	// ErrDBRPNotFound is used when the specified DBRP cannot be found.
	ErrDBRPNotFound = &influxdb.Error{
		Code: influxdb.ENotFound,
		Msg:  "unable to find DBRP",
	}

	// ErrNotUniqueID is used when the ID of the DBRP is not unique.
	ErrNotUniqueID = &influxdb.Error{
		Code: influxdb.EConflict,
		Msg:  "ID already exists",
	}

	// ErrFailureGeneratingID occurs ony when the random number generator
	// cannot generate an ID in MaxIDGenerationN times.
	ErrFailureGeneratingID = &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  "unable to generate valid id",
	}

	ErrNoOrgProvided = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "either 'org' or 'orgID' must be provided",
	}
)

// ErrOrgNotFound returns a more informative error about a 404 on org name.
func ErrOrgNotFound(org string) error {
	return &influxdb.Error{
		Code: influxdb.ENotFound,
		Msg:  fmt.Sprintf("invalid org %q", org),
		Err:  influxdb.ErrOrgNotFound,
	}
}

// ErrInvalidOrgID returns a more informative error about a failure
// to decode an organization ID.
func ErrInvalidOrgID(id string, err error) error {
	return &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  fmt.Sprintf("invalid org ID %q", id),
		Err:  err,
	}
}

// ErrInvalidBucketID returns a more informative error about a failure
// to decode a bucket ID.
func ErrInvalidBucketID(id string, err error) error {
	return &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  fmt.Sprintf("invalid bucket ID %q", id),
		Err:  err,
	}
}

// ErrInvalidDBRPID is used when the ID of the DBRP cannot be encoded.
func ErrInvalidDBRPID(id string, err error) error {
	return &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  fmt.Sprintf("invalid DBRP ID %q", id),
		Err:  err,
	}
}

// ErrInvalidDBRP is used when a service was provided an invalid DBRP.
func ErrInvalidDBRP(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "DBRP provided is invalid",
		Err:  err,
	}
}

// ErrInternalService is used when the error comes from an internal system.
func ErrInternalService(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Err:  err,
	}
}

// ErrDBRPAlreadyExists is used when there is a conflict in creating a new DBRP.
func ErrDBRPAlreadyExists(msg string) *influxdb.Error {
	if msg == "" {
		msg = "DBRP already exists"
	}
	return &influxdb.Error{
		Code: influxdb.EConflict,
		Msg:  msg,
	}
}
