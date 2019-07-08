package kv

import (
	"context"
	"fmt"

	influxdb "github.com/influxdata/influxdb"
)

// UnexpectedIndexError is used when the error comes from an internal system.
func UnexpectedIndexError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving index; Err: %v", err),
		Op:   "kv/index",
	}
}

// NotUniqueError is used when attempting to create a resource that already
// exists.
var NotUniqueError = &influxdb.Error{
	Code: influxdb.EConflict,
	Msg:  fmt.Sprintf("name already exists"),
}

func (s *Service) unique(ctx context.Context, tx Tx, indexBucket, indexKey []byte) error {
	bucket, err := tx.Bucket(indexBucket)
	if err != nil {
		return UnexpectedIndexError(err)
	}

	_, err = bucket.Get(indexKey)
	// if not found then this is  _unique_.
	if IsNotFound(err) {
		return nil
	}

	// no error means this is not unique
	if err == nil {
		return NotUniqueError
	}

	// any other error is some sort of internal server error
	return UnexpectedIndexError(err)
}
