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
		logger.Debug("Failed to lookup DBRP mappings for bucket", zap.Error(err))
		return nil
	}
	for _, m := range mappings {
		if err := s.DBRPMappingService.Delete(ctx, bucket.OrgID, m.ID); err != nil {
			logger.Debug("Failed to delete DBRP mapping for bucket", zap.Error(err))
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
	logger := s.Logger.With(zap.String("bucket_name", b.Name))

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

	if err := s.DBRPMappingService.Create(ctx, &newDBRP); err != nil {
		logger.Debug("Failed to auto-create DBRP for bucket", zap.Error(err))
	}
	return nil
}

// UpdateBucket updates a single bucket with changeset.
// Also updates the corresponding auto-DBRP mapping, or creates a new one if none exist.
// Returns the new bucket state after update.
func (s *BucketService) UpdateBucket(ctx context.Context, id platform.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
	logger := s.Logger.With(zap.String("bucket_id", id.String()))

	// get the old bucket to know what has changed
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
	})
	if err != nil {
		logger.Debug("Failed to lookup DBRP mappings for bucket", zap.Error(err))
		return updatedB, nil
	}
	var newDb string
	var newRp string
	if upd.Name == nil {
		newDb = oldDb
		newRp = oldRp
	} else {
		newDb, newRp = ParseDBRP(*upd.Name)
	}
	logger.Debug("Update", zap.String("newDb", newDb), zap.String("newRp", newRp))
	mustCreate := false

	if len(dbrps) == 1 {
		dbrpToPatch := dbrps[0]
		// if an DBRP-Update-unchangeable field has changed, we need to delete and recreate
		if dbrpToPatch.Database != newDb {
			mustCreate = true
			if err := s.DBRPMappingService.Delete(ctx, dbrpToPatch.OrganizationID, dbrpToPatch.ID); err != nil {
				logger.Debug("Failed to delete outdated DBRP mapping for bucket", zap.Error(err))
			}
		} else {
			dbrpToPatch.Database, dbrpToPatch.RetentionPolicy = newDb, newRp
			if err := s.DBRPMappingService.Update(ctx, dbrpToPatch); err != nil {
				logger.Debug("Failed to auto-update DBRP mapping for bucket", zap.Error(err))
			}
		}
	}
	if mustCreate {
		newDbrp := influxdb.DBRPMapping{
			Database:        newDb,
			RetentionPolicy: newRp,
			OrganizationID:  updatedB.OrgID,
			BucketID:        updatedB.ID,
		}
		if err := s.DBRPMappingService.Create(ctx, &newDbrp); err != nil {
			logger.Debug("Failed to auto-create missing DBRP mapping for bucket", zap.Error(err))
		}
	}
	return updatedB, nil
}
