package dbrp

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
	"go.uber.org/zap"
)

type BucketService struct {
	influxdb.BucketService
	Logger             *zap.Logger
	DBRPMappingService influxdb.DBRPMappingServiceV2
}

func NewBucketService(logger *zap.Logger, bucketService influxdb.BucketService, dbrpService influxdb.DBRPMappingServiceV2) *BucketService {
	return &BucketService{
		Logger:             logger,
		BucketService:      bucketService,
		DBRPMappingService: dbrpService,
	}
}

func (s *BucketService) DeleteBucket(ctx context.Context, id platform.ID) error {
	bucket, err := s.BucketService.FindBucketByID(ctx, id)
	if err != nil {
		return err
	}
	if err := s.BucketService.DeleteBucket(ctx, id); err != nil {
		return err
	}

	logger := s.Logger.With(zap.String("bucket_id", id.String()))
	mappings, _, err := s.DBRPMappingService.FindMany(ctx, influxdb.DBRPMappingFilterV2{
		OrgID:    &bucket.OrgID,
		BucketID: &bucket.ID,
	})
	if err != nil {
		logger.Error("Failed to lookup DBRP mappings for Bucket.", zap.Error(err))
		return nil
	}
	for _, m := range mappings {
		if err := s.DBRPMappingService.Delete(ctx, bucket.OrgID, m.ID); err != nil {
			logger.Error("Failed to delete DBRP mapping for Bucket.", zap.Error(err))
		}
	}
	return nil
}
