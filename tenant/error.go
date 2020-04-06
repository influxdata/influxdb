package tenant

import (
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
