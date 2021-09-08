package replications

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"go.uber.org/zap"
)

type ReplicationDeleter interface {
	// DeleteBucketReplications deletes all replications registered to the local bucket
	// with the given ID.
	DeleteBucketReplications(context.Context, platform.ID) error
}

type bucketService struct {
	influxdb.BucketService
	logger             *zap.Logger
	replicationDeleter ReplicationDeleter
}

func NewBucketService(log *zap.Logger, bucketSvc influxdb.BucketService, deleter ReplicationDeleter) *bucketService {
	return &bucketService{
		BucketService:      bucketSvc,
		logger:             log,
		replicationDeleter: deleter,
	}
}

func (s *bucketService) DeleteBucket(ctx context.Context, id platform.ID) error {
	if err := s.BucketService.DeleteBucket(ctx, id); err != nil {
		return err
	}
	if err := s.replicationDeleter.DeleteBucketReplications(ctx, id); err != nil {
		s.logger.Error("Failed to delete replications for bucket",
			zap.String("bucket_id", id.String()), zap.Error(err))
		// TODO: Should we return some sort of error in this case? The DBRP service only logs.
	}
	return nil
}
