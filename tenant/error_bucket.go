package tenant

import (
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

var (
	invalidBucketListRequest = &errors.Error{
		Code: errors.EInternal,
		Msg:  "invalid bucket list action, call should be GetBucketByName",
		Op:   "kv/listBucket",
	}

	errRenameSystemBucket = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "system buckets cannot be renamed",
	}

	errDeleteSystemBucket = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "system buckets cannot be deleted",
	}

	ErrBucketNotFound = &errors.Error{
		Code: errors.ENotFound,
		Msg:  "bucket not found",
	}

	ErrBucketNameNotUnique = &errors.Error{
		Code: errors.EConflict,
		Msg:  "bucket name is not unique",
	}
)

// ErrBucketNotFoundByName is used when the user is not found.
func ErrBucketNotFoundByName(n string) *errors.Error {
	return &errors.Error{
		Msg:  fmt.Sprintf("bucket %q not found", n),
		Code: errors.ENotFound,
	}
}

// ErrCorruptBucket is used when the user cannot be unmarshalled from the bytes
// stored in the kv.
func ErrCorruptBucket(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  "user could not be unmarshalled",
		Err:  err,
		Op:   "kv/UnmarshalBucket",
	}
}

// BucketAlreadyExistsError is used when attempting to create a user with a name
// that already exists.
func BucketAlreadyExistsError(n string) *errors.Error {
	return &errors.Error{
		Code: errors.EConflict,
		Msg:  fmt.Sprintf("bucket with name %s already exists", n),
	}
}

// ErrUnprocessableBucket is used when a org is not able to be processed.
func ErrUnprocessableBucket(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EUnprocessableEntity,
		Msg:  "user could not be marshalled",
		Err:  err,
		Op:   "kv/MarshalBucket",
	}
}
