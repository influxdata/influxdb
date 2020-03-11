package tenant

import (
	"github.com/influxdata/influxdb"
)

var (
	invalidBucketListRequest = &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  "invalid bucket list action, call should be GetBucketByName",
		Op:   "kv/listBucket",
	}

	// ErrBucketNotFound is used when the user is not found.
	ErrBucketNotFound = &influxdb.Error{
		Msg:  "bucket not found",
		Code: influxdb.ENotFound,
	}
)

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

// ErrUnprocessableBucket is used when a org is not able to be processed.
func ErrUnprocessableBucket(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EUnprocessableEntity,
		Msg:  "user could not be marshalled",
		Err:  err,
		Op:   "kv/MarshalBucket",
	}
}
