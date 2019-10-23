package pkger

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/influxdb"
	"go.uber.org/zap"
)

type Service struct {
	logger    *zap.Logger
	bucketSVC influxdb.BucketService
}

func NewService(l *zap.Logger, bucketSVC influxdb.BucketService) *Service {
	svc := Service{
		logger:    zap.NewNop(),
		bucketSVC: bucketSVC,
	}

	if l != nil {
		svc.logger = l
	}
	return &svc
}

func (s *Service) Apply(ctx context.Context, orgID influxdb.ID, pkg *Pkg) (sum Summary, e error) {
	type rollback struct {
		resource string
		fn       func() error
	}

	var newBuckets []influxdb.Bucket
	rollbacks := []rollback{
		{
			resource: "bucket",
			fn:       func() error { return s.deleteBuckets(newBuckets) },
		},
	}
	defer func() {
		if e == nil {
			return
		}
		for _, r := range rollbacks {
			if err := r.fn(); err != nil {
				s.logger.Error("failed to delete "+r.resource, zap.Error(err))
			}
		}
	}()

	newBuckets, err := s.applyBuckets(ctx, orgID, pkg.buckets())
	if err != nil {
		return Summary{}, err
	}

	return pkg.Summary(), nil
}

func (s *Service) applyBuckets(ctx context.Context, orgID influxdb.ID, buckets []*bucket) ([]influxdb.Bucket, error) {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()

	influxBuckets := make([]influxdb.Bucket, 0, len(buckets))
	for i, b := range buckets {
		influxBucket := influxdb.Bucket{
			OrgID:           orgID,
			Description:     b.Description,
			Name:            b.Name,
			RetentionPeriod: b.RetentionPeriod,
		}

		err := s.bucketSVC.CreateBucket(ctx, &influxBucket)
		if err != nil {
			return influxBuckets, err
		}
		buckets[i].ID = influxBucket.ID
		buckets[i].OrgID = influxBucket.OrgID

		influxBuckets = append(influxBuckets, influxBucket)
	}

	return influxBuckets, nil
}

func (s *Service) deleteBuckets(buckets []influxdb.Bucket) error {
	var errs []string
	for _, b := range buckets {
		err := s.bucketSVC.DeleteBucket(context.Background(), b.ID)
		if err != nil {
			errs = append(errs, b.ID.String())
		}
	}

	if len(errs) > 0 {
		// TODO: fixup error
		return fmt.Errorf(`bucket_ids=[%s] err="unable to delete bucket"`, strings.Join(errs, ", "))
	}

	return nil
}
