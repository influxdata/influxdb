package tenant

import (
	"github.com/influxdata/influxdb"
)

var (
	// ErrNameisEmpty is when a name is empty
	ErrNameisEmpty = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "name is empty",
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
