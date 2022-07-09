package dbrp

import (
	"context"
	"strings"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"go.uber.org/zap"
)

type BucketService struct {
	influxdb.BucketService
	Logger             *zap.Logger
	DBRPMappingService influxdb.DBRPMappingService
}

func NewBucketService(logger *zap.Logger, bucketService influxdb.BucketService, dbrpService influxdb.DBRPMappingService) *BucketService {
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
	mappings, _, err := s.DBRPMappingService.FindMany(ctx, influxdb.DBRPMappingFilter{
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

// ParseDBRP parses DB and RP strings out of a bucket name
func ParseDBRP(bucketName string) (string, string) {
	db, rp, isCut := strings.Cut(bucketName, "/")
	if isCut {
		return db, rp
	}
	return bucketName, "autogen"
}

// CreateBucket creates a new bucket and sets b.ID with the new identifier.
// Also creates a corresponding auto-DBRP mapping.
func (s *BucketService) CreateBucket(ctx context.Context, b *influxdb.Bucket) error {
	if err := s.BucketService.CreateBucket(ctx, b); err != nil {
		return err
	}
	db, rp := ParseDBRP(b.Name)
	newDBRP := influxdb.DBRPMapping{
		Database:        db,
		RetentionPolicy: rp,
		OrganizationID:  b.OrgID,
		BucketID:        b.ID,
	}
	return s.DBRPMappingService.Create(ctx, &newDBRP)
}

// UpdateBucket updates a single bucket with changeset.
// Also updates the corresponding auto-DBRP mapping, or creates a new one if none exist.
// Returns the new bucket state after update.
func (s *BucketService) UpdateBucket(ctx context.Context, id platform.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
	logger := s.Logger.With(zap.String("bucket_id", id.String()))

	// update the auto-DBRP if name of bucket is changed
	oldB, err := s.BucketService.FindBucketByID(ctx, id)
	if err != nil {
		logger.Error("Failed to lookup bucket", zap.Error(err))
		return nil, err
	}
	oldName := oldB.Name

	updatedB, err := s.BucketService.UpdateBucket(ctx, id, upd)
	if err != nil {
		return nil, err
	}

	oldDb, oldRp := ParseDBRP(oldName)
	dbrps, _, err := s.DBRPMappingService.FindMany(ctx, influxdb.DBRPMappingFilter{
		OrgID:           &oldB.OrgID,
		BucketID:        &oldB.ID,
		Database:        &oldDb,
		RetentionPolicy: &oldRp,
	}, influxdb.FindOptions{Limit: 1})
	if err != nil {
		logger.Error("Failed to lookup DBRP mappings for Bucket.", zap.Error(err))
		return nil, err
	}
	newDb, newRp := ParseDBRP(*upd.Name)
	if len(dbrps) < 1 {
		if err := s.DBRPMappingService.Create(ctx, &influxdb.DBRPMapping{
			Database:        newDb,
			RetentionPolicy: newRp,
			OrganizationID:  updatedB.OrgID,
			BucketID:        updatedB.ID,
		}); err != nil {
			logger.Error("Failed to auto-create DBRP mapping for Bucket.", zap.Error(err))
			return nil, err
		}
	} else {
		dbrpToPatch := dbrps[0]
		dbrpToPatch.Database, dbrpToPatch.RetentionPolicy = ParseDBRP(*upd.Name)
		if err := s.DBRPMappingService.Update(ctx, dbrpToPatch); err != nil {
			logger.Error("Failed to auto-update DBRP mapping for Bucket.", zap.Error(err))
			return nil, err
		}
	}
	return updatedB, nil
}
