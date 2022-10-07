package influxdb

import (
	"context"

	"github.com/influxdata/influxql"

	"github.com/influxdata/influxdb/v2/kit/platform"
)

// Predicate is something that can match on a series key.
type Predicate interface {
	Clone() Predicate
	Matches(key []byte) bool
	Marshal() ([]byte, error)
}

// DeleteService will delete a bucket from the range and predict.
type DeleteService interface {
	DeleteBucketRangePredicate(ctx context.Context, orgID, bucketID platform.ID, min, max int64, pred Predicate, measurement influxql.Expr) error
}
