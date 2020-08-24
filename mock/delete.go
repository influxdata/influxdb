package mock

import (
	"context"

	"github.com/influxdata/influxdb/v2"
)

var _ influxdb.DeleteService = &DeleteService{}

// DeleteService is a mock delete server.
type DeleteService struct {
	DeleteBucketRangePredicateF func(tx context.Context, orgID, bucketID influxdb.ID, min, max int64, pred influxdb.Predicate, opts influxdb.DeletePrefixRangeOptions) error
}

// NewDeleteService returns a mock DeleteService where its methods will return
// zero values.
func NewDeleteService() DeleteService {
	return DeleteService{
		DeleteBucketRangePredicateF: func(tx context.Context, orgID, bucketID influxdb.ID, min, max int64, pred influxdb.Predicate, opts influxdb.DeletePrefixRangeOptions) error {
			return nil
		},
	}
}

//DeleteBucketRangePredicate calls DeleteBucketRangePredicateF.
func (s DeleteService) DeleteBucketRangePredicate(ctx context.Context, orgID, bucketID influxdb.ID, min, max int64, pred influxdb.Predicate, opts influxdb.DeletePrefixRangeOptions) error {
	return s.DeleteBucketRangePredicateF(ctx, orgID, bucketID, min, max, pred, opts)
}
