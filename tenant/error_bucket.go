package tenant

import (
	"fmt"

	"github.com/influxdata/influxdb"
)

var (
	invalidBucketListRequest = &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  "invalid bucket list action, call should be GetBucketByName",
		Op:   "kv/listBucket",
	}

	errRenameSystemBucket = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "system buckets cannot be renamed",
	}

	errDeleteSystemBucket = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "system buckets cannot be deleted",
	}

	ErrBucketNotFound = &influxdb.Error{
		Code: influxdb.ENotFound,
		Msg:  "bucket not found",
	}

	ErrBucketNameNotUnique = &influxdb.Error{
		Code: influxdb.EConflict,
		Msg:  "bucket name is not unique",
	}
)

// ErrBucketNotFoundByName is used when the user is not found.
func ErrBucketNotFoundByName(n string) *influxdb.Error {
	return &influxdb.Error{
		Msg:  fmt.Sprintf("bucket %q not found", n),
		Code: influxdb.ENotFound,
	}
}

// ErrCorruptBucket is used when the user cannot be unmarshalled from the bytes
// stored in the kv.
func ErrCorruptBucket(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  "user could not be unmarshalled",
		Err:  err,
		Op:   "kv/UnmarshalBucket",
	}
}

// BucketAlreadyExistsError is used when attempting to create a user with a name
// that already exists.
func BucketAlreadyExistsError(n string) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EConflict,
		Msg:  fmt.Sprintf("bucket with name %s already exists", n),
	}
}

// ErrUnprocessableBucket is used when a org is not able to be processed.
func ErrUnprocessableBucket(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EUnprocessableEntity,
		Msg:  "user could not be marshalled",
		Err:  err,
		Op:   "kv/MarshalBucket",
	}
}
