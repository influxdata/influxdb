package pkger

import (
	"context"
	"errors"
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

	runTilEnd := func(appliers ...applier) error {
		var errs []string
		for _, r := range appliers {
			rollbacks = append(rollbacks, r.rollbacker)
			if err := r.creater(ctx, orgID); err != nil {
				errs = append(errs, err.Error())
			}
		}

		if len(errs) > 0 {
			// TODO: fix error up to be more actionable
			return errors.New(strings.Join(errs, "\n"))
		}
		return nil
	}

	runners := [][]applier{
		// each grouping here runs for its entirety, then returns an error that
		// is indicative of running all appliers provided. For instance, the labels
		// may have 1 label fail and one of the buckets fails. The errors aggregate so
		// the caller will be informed of both the failed label and the failed bucket.
		// the groupings here allow for steps to occur before exiting. The first step is
		// adding the primary resources. Here we get all the errors associated with them.
		// If those are all good, then we run the secondary(dependent) resources which
		// rely on the primary resources having been created.
		{
			// primary resources
			s.applyLabels(pkg.labels()),
			s.applyBuckets(pkg.buckets()),
		},
		{
			// secondary (dependent) resources
			s.applyLabelMappings(pkg),
		},
	}

	for _, appliers := range runners {
		if err := runTilEnd(appliers...); err != nil {
			return Summary{}, err
		}
	}

	return pkg.Summary(), nil
}

type (
	applier struct {
		creater    creater
		rollbacker rollbacker
	}

	rollbacker struct {
		resource string
		fn       func() error
	}

	creater func(ctx context.Context, orgID influxdb.ID) error
)

// TODO: clean up apply errors to inform the user in an actionable way
type applyErrBody struct {
	name string
	msg  string
}

type applyErrs []applyErrBody

func (a applyErrs) toError(resType, msg string) error {
	if len(a) == 0 {
		return nil
	}
	errMsg := fmt.Sprintf(`resource_type=%q err=%q`, resType, msg)
	for _, e := range a {
		errMsg += fmt.Sprintf("\n\tname=%q err_msg=%q", e.name, e.msg)
	}
	return errors.New(errMsg)
}

func (s *Service) applyBuckets(buckets []*bucket) applier {
	influxBuckets := make([]influxdb.Bucket, 0, len(buckets))
	createFn := func(ctx context.Context, orgID influxdb.ID) error {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
		defer cancel()

		var errs applyErrs
		for i, b := range buckets {
			influxBucket := influxdb.Bucket{
				OrgID:           orgID,
				Description:     b.Description,
				Name:            b.Name,
				RetentionPeriod: b.RetentionPeriod,
			}

			err := s.bucketSVC.CreateBucket(ctx, &influxBucket)
			if err != nil {
				errs = append(errs, applyErrBody{
					name: b.Name,
					msg:  err.Error(),
				})
				continue
			}
			buckets[i].ID = influxBucket.ID
			buckets[i].OrgID = influxBucket.OrgID

			influxBuckets = append(influxBuckets, influxBucket)
		}

		return errs.toError("bucket", "failed to create bucket")
	}

	return applier{
		creater: createFn,
		rollbacker: rollbacker{
			resource: "bucket",
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

		var errs applyErrs
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
				errs = append(errs, applyErrBody{
					name: l.Name,
					msg:  err.Error(),
				})
				continue
			}
			labels[i].ID = influxLabel.ID
			labels[i].OrgID = influxLabel.OrgID
			influxLabels = append(influxLabels, influxLabel)
		}

		return errs.toError("label", "failed to create label")
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

func (s *Service) applyLabelMappings(pkg *Pkg) applier {
	var mappings []influxdb.LabelMapping
	createFn := func(ctx context.Context, orgID influxdb.ID) error {
		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()

		labelMappings := pkg.labelMappings()
		for i := range labelMappings {
			mapping := labelMappings[i]
			err := s.labelSVC.CreateLabelMapping(ctx, &mapping)
			if err != nil {
				return err
			}
			mappings = append(mappings, mapping)
		}

		return nil
	}

	return applier{
		creater: createFn,
		rollbacker: rollbacker{
			resource: "label_mapping",
			fn:       func() error { return s.deleteLabelMappings(mappings) },
		},
	}
}

func (s *Service) deleteLabelMappings(mappings []influxdb.LabelMapping) error {
	var errs []string
	for i := range mappings {
		l := mappings[i]
		err := s.labelSVC.DeleteLabelMapping(context.Background(), &l)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s:%s", l.LabelID.String(), l.ResourceID.String()))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf(`label_resource_id_pairs=[%s] err="unable to delete label"`, strings.Join(errs, ", "))
	}

	return nil
}
