package influxdb

import "context"

// Predicate is something that can match on a series key.
type Predicate interface {
	Clone() Predicate
	Matches(key []byte) bool
	Marshal() ([]byte, error)
}

// DeleteService will delete a bucket from the range and predict.
type DeleteService interface {
	DeleteBucketRangePredicate(ctx context.Context, orgID, bucketID ID, min, max int64, pred Predicate, opts DeletePrefixRangeOptions) error
}

type DeletePrefixRangeOptions struct {
	// If true, does not delete underlying series when all data has been deleted.
	KeepSeries bool
}
