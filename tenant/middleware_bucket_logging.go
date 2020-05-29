package tenant

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2"
	"go.uber.org/zap"
)

type BucketLogger struct {
	logger        *zap.Logger
	bucketService influxdb.BucketService
}

// NewBucketLogger returns a logging service middleware for the Bucket Service.
func NewBucketLogger(log *zap.Logger, s influxdb.BucketService) *BucketLogger {
	return &BucketLogger{
		logger:        log,
		bucketService: s,
	}
}

var _ influxdb.BucketService = (*BucketLogger)(nil)

func (l *BucketLogger) CreateBucket(ctx context.Context, u *influxdb.Bucket) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to create bucket", zap.Error(err), dur)
			return
		}
		l.logger.Debug("bucket create", dur)
	}(time.Now())
	return l.bucketService.CreateBucket(ctx, u)
}

func (l *BucketLogger) FindBucketByID(ctx context.Context, id influxdb.ID) (u *influxdb.Bucket, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			msg := fmt.Sprintf("failed to find bucket with ID %v", id)
			l.logger.Debug(msg, zap.Error(err), dur)
			return
		}
		l.logger.Debug("bucket find by ID", dur)
	}(time.Now())
	return l.bucketService.FindBucketByID(ctx, id)
}

func (l *BucketLogger) FindBucketByName(ctx context.Context, orgID influxdb.ID, name string) (u *influxdb.Bucket, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			msg := fmt.Sprintf("failed to find bucket with name %v in org %v", name, orgID)
			l.logger.Debug(msg, zap.Error(err), dur)
			return
		}
		l.logger.Debug("bucket find by name", dur)
	}(time.Now())
	return l.bucketService.FindBucketByName(ctx, orgID, name)
}

func (l *BucketLogger) FindBucket(ctx context.Context, filter influxdb.BucketFilter) (u *influxdb.Bucket, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to find bucket matching the given filter", zap.Error(err), dur)
			return
		}
		l.logger.Debug("bucket find", dur)
	}(time.Now())
	return l.bucketService.FindBucket(ctx, filter)
}

func (l *BucketLogger) FindBuckets(ctx context.Context, filter influxdb.BucketFilter, opt ...influxdb.FindOptions) (buckets []*influxdb.Bucket, n int, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to find bucket matching the given filter", zap.Error(err), dur)
			return
		}
		l.logger.Debug("buckets find", dur)
	}(time.Now())
	return l.bucketService.FindBuckets(ctx, filter, opt...)
}

func (l *BucketLogger) UpdateBucket(ctx context.Context, id influxdb.ID, upd influxdb.BucketUpdate) (u *influxdb.Bucket, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to update bucket", zap.Error(err), dur)
			return
		}
		l.logger.Debug("bucket update", dur)
	}(time.Now())
	return l.bucketService.UpdateBucket(ctx, id, upd)
}

func (l *BucketLogger) DeleteBucket(ctx context.Context, id influxdb.ID) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			msg := fmt.Sprintf("failed to delete bucket with ID %v", id)
			l.logger.Debug(msg, zap.Error(err), dur)
			return
		}
		l.logger.Debug("bucket delete", dur)
	}(time.Now())
	return l.bucketService.DeleteBucket(ctx, id)
}
