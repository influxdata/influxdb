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
	labelSVC  influxdb.LabelService
	bucketSVC influxdb.BucketService
}

func NewService(l *zap.Logger, bucketSVC influxdb.BucketService, labelSVC influxdb.LabelService) *Service {
	svc := Service{
		logger:    zap.NewNop(),
		bucketSVC: bucketSVC,
		labelSVC:  labelSVC,
	}

	if l != nil {
		svc.logger = l
	}
	return &svc
}

func (s *Service) Apply(ctx context.Context, orgID influxdb.ID, pkg *Pkg) (sum Summary, e error) {
	var rollbacks []rollbacker
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

	runners := []applier{
		s.applyLabels(pkg.labels()),
		s.applyBuckets(pkg.buckets()),
	}

	for _, r := range runners {
		rollbacks = append(rollbacks, r.rollbacker)
		if err := r.creater(ctx, orgID); err != nil {
			return Summary{}, err
		}
	}

	return pkg.Summary(), nil
}

type applier struct {
	creater    creater
	rollbacker rollbacker
}

type rollbacker struct {
	resource string
	fn       func() error
}

type creater func(ctx context.Context, orgID influxdb.ID) error

func (s *Service) applyBuckets(buckets []*bucket) applier {
	const resource = "bucket"

	influxBuckets := make([]influxdb.Bucket, 0, len(buckets))
	createFn := func(ctx context.Context, orgID influxdb.ID) error {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
		defer cancel()

		for i, b := range buckets {
			influxBucket := influxdb.Bucket{
				OrgID:           orgID,
				Description:     b.Description,
				Name:            b.Name,
				RetentionPeriod: b.RetentionPeriod,
			}

			err := s.bucketSVC.CreateBucket(ctx, &influxBucket)
			if err != nil {
				return err
			}
			buckets[i].ID = influxBucket.ID
			buckets[i].OrgID = influxBucket.OrgID

			influxBuckets = append(influxBuckets, influxBucket)
		}

		return nil
	}

	return applier{
		creater: createFn,
		rollbacker: rollbacker{
			resource: resource,
			fn:       func() error { return s.deleteBuckets(influxBuckets) },
		},
	}
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

func (s *Service) applyLabels(labels []*label) applier {
	influxLabels := make([]influxdb.Label, 0, len(labels))
	createFn := func(ctx context.Context, orgID influxdb.ID) error {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
		defer cancel()

		for i, l := range labels {
			influxLabel := influxdb.Label{
				OrgID: orgID,
				Name:  l.Name,
				Properties: map[string]string{
					"color":       l.Color,
					"description": l.Description,
				},
			}
			err := s.labelSVC.CreateLabel(ctx, &influxLabel)
			if err != nil {
				return err
			}
			labels[i].ID = influxLabel.ID
			labels[i].OrgID = influxLabel.OrgID
			influxLabels = append(influxLabels, influxLabel)
		}

		return nil
	}

	return applier{
		creater: createFn,
		rollbacker: rollbacker{
			resource: "label",
			fn:       func() error { return s.deleteLabels(influxLabels) },
		},
	}
}

func (s *Service) deleteLabels(labels []influxdb.Label) error {
	var errs []string
	for _, l := range labels {
		err := s.labelSVC.DeleteLabel(context.Background(), l.ID)
		if err != nil {
			errs = append(errs, l.ID.String())
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf(`label_ids=[%s] err="unable to delete label"`, strings.Join(errs, ", "))
	}

	return nil
}
