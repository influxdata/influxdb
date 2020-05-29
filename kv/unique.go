package kv

import (
	"context"
	"fmt"

	influxdb "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
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
	Msg:  "name already exists",
}

// NotUniqueIDError is used when attempting to create an org or bucket that already
// exists.
var NotUniqueIDError = &influxdb.Error{
	Code: influxdb.EConflict,
	Msg:  "ID already exists",
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

func (s *Service) uniqueID(ctx context.Context, tx Tx, bucket []byte, id influxdb.ID) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	encodedID, err := id.Encode()
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	b, err := tx.Bucket(bucket)
	if err != nil {
		return err
	}

	_, err = b.Get(encodedID)
	if IsNotFound(err) {
		return nil
	}

	return NotUniqueIDError
}

// generateSafeID attempts to create ids for buckets
// and orgs that are without backslash, commas, and spaces, BUT ALSO do not already exist.
func (s *Service) generateSafeID(ctx context.Context, tx Tx, bucket []byte) (influxdb.ID, error) {
	for i := 0; i < MaxIDGenerationN; i++ {
		id := s.OrgBucketIDs.ID()
		// we have reserved a certain number of IDs
		// for orgs and buckets.
		if id < ReservedIDs {
			continue
		}
		err := s.uniqueID(ctx, tx, bucket, id)
		if err == nil {
			return id, nil
		}

		if err == NotUniqueIDError {
			continue
		}

		return influxdb.InvalidID(), err
	}
	return influxdb.InvalidID(), ErrFailureGeneratingID
}
