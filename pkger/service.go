package pkger

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/influxdata/influxdb"
	"go.uber.org/zap"
)

// Service provides the pkger business logic including all the dependencies to make
// this resource sausage.
type Service struct {
	logger    *zap.Logger
	labelSVC  influxdb.LabelService
	bucketSVC influxdb.BucketService
}

// NewService is a constructor for a pkger Service.
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

// DryRun provides a dry run of the pkg application. The pkg will be marked verified
// for later calls to Apply. This func will be run on an Apply if it has not been run
// already.
func (s *Service) DryRun(ctx context.Context, orgID influxdb.ID, pkg *Pkg) (Summary, Diff, error) {
	diffBuckets, err := s.dryRunBuckets(ctx, orgID, pkg)
	if err != nil {
		return Summary{}, Diff{}, err
	}

	diffLabels, err := s.dryRunLabels(ctx, orgID, pkg)
	if err != nil {
		return Summary{}, Diff{}, err
	}

	diffLabelMappings, err := s.dryRunLabelMappings(ctx, pkg)
	if err != nil {
		return Summary{}, Diff{}, err
	}

	// verify the pkg is verified by a dry run. when calling Service.Apply this
	// is required to have been run. if it is not true, then apply runs
	// the Dry run.
	pkg.isVerified = true

	diff := Diff{
		Buckets:       diffBuckets,
		Labels:        diffLabels,
		LabelMappings: diffLabelMappings,
	}
	return pkg.Summary(), diff, nil
}

func (s *Service) dryRunBuckets(ctx context.Context, orgID influxdb.ID, pkg *Pkg) ([]DiffBucket, error) {
	mExistingBkts := make(map[string]DiffBucket)
	bkts := pkg.buckets()
	for i := range bkts {
		b := bkts[i]
		existingBkt, err := s.bucketSVC.FindBucketByName(ctx, orgID, b.Name)
		switch err {
		// TODO: case for err not found here and another case handle where
		//  err isn't a not found (some other error)
		case nil:
			b.existing = existingBkt
			mExistingBkts[b.Name] = newDiffBucket(b, *existingBkt)
		default:
			mExistingBkts[b.Name] = newDiffBucket(b, influxdb.Bucket{})
		}
	}

	var diffs []DiffBucket
	for _, diff := range mExistingBkts {
		diffs = append(diffs, diff)
	}
	sort.Slice(diffs, func(i, j int) bool {
		return diffs[i].Name < diffs[j].Name
	})

	return diffs, nil
}

func (s *Service) dryRunLabels(ctx context.Context, orgID influxdb.ID, pkg *Pkg) ([]DiffLabel, error) {
	mExistingLabels := make(map[string]DiffLabel)
	labels := pkg.labels()
	for i := range labels {
		l := labels[i]
		existingLabels, err := s.labelSVC.FindLabels(ctx, influxdb.LabelFilter{
			Name:  l.Name,
			OrgID: &orgID,
		}, influxdb.FindOptions{Limit: 1})
		switch {
		// TODO: case for err not found here and another case handle where
		//  err isn't a not found (some other error)
		case err == nil && len(existingLabels) > 0:
			existingLabel := existingLabels[0]
			l.existing = existingLabel
			mExistingLabels[l.Name] = newDiffLabel(l, *existingLabel)
		default:
			mExistingLabels[l.Name] = newDiffLabel(l, influxdb.Label{})
		}
	}

	diffs := make([]DiffLabel, 0, len(mExistingLabels))
	for _, diff := range mExistingLabels {
		diffs = append(diffs, diff)
	}
	sort.Slice(diffs, func(i, j int) bool {
		return diffs[i].Name < diffs[j].Name
	})

	return diffs, nil
}

func (s *Service) dryRunLabelMappings(ctx context.Context, pkg *Pkg) ([]DiffLabelMapping, error) {
	var diffs []DiffLabelMapping
	for _, b := range pkg.buckets() {
		err := s.dryRunBucketLabelMapping(ctx, b, func(labelID influxdb.ID, labelName string, isNew bool) {
			pkg.mLabels[labelName].setBucketMapping(b, !isNew)
			diffs = append(diffs, DiffLabelMapping{
				IsNew:     isNew,
				ResType:   influxdb.BucketsResourceType,
				ResID:     b.ID(),
				ResName:   b.Name,
				LabelID:   labelID,
				LabelName: labelName,
			})
		})
		if err != nil {
			return nil, err
		}
	}

	// sort by res type ASC, then res name ASC, then label name ASC
	sort.Slice(diffs, func(i, j int) bool {
		n, m := diffs[i], diffs[j]
		if n.ResType < m.ResType {
			return true
		}
		if n.ResType > m.ResType {
			return false
		}
		if n.ResName < m.ResName {
			return true
		}
		if n.ResName > m.ResName {
			return false
		}
		return n.LabelName < m.LabelName
	})

	return diffs, nil
}

type labelMappingDiffFn func(labelID influxdb.ID, labelName string, isNew bool)

func (s *Service) dryRunBucketLabelMapping(ctx context.Context, b *bucket, mappingFn labelMappingDiffFn) error {
	if b.existing == nil {
		for _, l := range b.labels {
			mappingFn(l.ID(), l.Name, true)
		}
		return nil
	}
	// loop through and hit api for all labels associated with a bkt
	// lookup labels in pkg, add it to the label mapping, if exists in
	// the results from API, mark it exists
	labels, err := s.labelSVC.FindResourceLabels(ctx, influxdb.LabelMappingFilter{
		ResourceID:   b.ID(),
		ResourceType: influxdb.BucketsResourceType,
	})
	if err != nil {
		// TODO: inspect err, if its a not found error, do nothing, if any other error
		//  handle it better
		return err
	}

	pkgLabels := labelSlcToMap(b.labels)
	for _, l := range labels {
		// should ignore any labels that are not specified in pkg
		mappingFn(l.ID, l.Name, false)
		delete(pkgLabels, l.Name)
	}

	// now we add labels that were not apart of the existing labels
	for _, l := range pkgLabels {
		mappingFn(l.ID(), l.Name, true)
	}
	return nil
}

func labelSlcToMap(labels []*label) map[string]*label {
	m := make(map[string]*label)
	for i := range labels {
		m[labels[i].Name] = labels[i]
	}
	return m
}

// Apply will apply all the resources identified in the provided pkg. The entire pkg will be applied
// in its entirety. If a failure happens midway then the entire pkg will be rolled back to the state
// from before the pkg were applied.
func (s *Service) Apply(ctx context.Context, orgID influxdb.ID, pkg *Pkg) (sum Summary, e error) {
	if !pkg.isVerified {
		_, _, err := s.DryRun(ctx, orgID, pkg)
		if err != nil {
			return Summary{}, err
		}
	}

	coordinator := new(rollbackCoordinator)
	defer coordinator.rollback(s.logger, &e)

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
		err := coordinator.runTilEnd(ctx, orgID, appliers...)
		if err != nil {
			return Summary{}, err
		}
	}

	return pkg.Summary(), nil
}

func (s *Service) applyBuckets(buckets []*bucket) applier {
	rollbackBuckets := make([]*bucket, 0, len(buckets))
	createFn := func(ctx context.Context, orgID influxdb.ID) error {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
		defer cancel()

		var errs applyErrs
		for i, b := range buckets {
			buckets[i].OrgID = orgID
			influxBucket, err := s.applyBucket(ctx, b)
			if err != nil {
				errs = append(errs, applyErrBody{
					name: b.Name,
					msg:  err.Error(),
				})
				continue
			}
			buckets[i].id = influxBucket.ID
			rollbackBuckets = append(rollbackBuckets, buckets[i])
		}

		return errs.toError("bucket", "failed to create bucket")
	}

	return applier{
		creater: createFn,
		rollbacker: rollbacker{
			resource: "bucket",
			fn:       func() error { return s.rollbackBuckets(rollbackBuckets) },
		},
	}
}

func (s *Service) rollbackBuckets(buckets []*bucket) error {
	var errs []string
	for _, b := range buckets {
		if b.existing == nil {
			err := s.bucketSVC.DeleteBucket(context.Background(), b.ID())
			if err != nil {
				errs = append(errs, b.ID().String())
			}
			continue
		}

		_, err := s.bucketSVC.UpdateBucket(context.Background(), b.ID(), influxdb.BucketUpdate{
			Description:     &b.Description,
			RetentionPeriod: &b.RetentionPeriod,
		})
		if err != nil {
			errs = append(errs, b.ID().String())
		}
	}

	if len(errs) > 0 {
		// TODO: fixup error
		return fmt.Errorf(`bucket_ids=[%s] err="unable to delete bucket"`, strings.Join(errs, ", "))
	}

	return nil
}

func (s *Service) applyBucket(ctx context.Context, b *bucket) (influxdb.Bucket, error) {
	if b.existing != nil {
		influxBucket, err := s.bucketSVC.UpdateBucket(ctx, b.ID(), influxdb.BucketUpdate{
			Description:     &b.Description,
			RetentionPeriod: &b.RetentionPeriod,
		})
		if err != nil {
			return influxdb.Bucket{}, err
		}
		return *influxBucket, nil
	}

	influxBucket := influxdb.Bucket{
		OrgID:           b.OrgID,
		Description:     b.Description,
		Name:            b.Name,
		RetentionPeriod: b.RetentionPeriod,
	}
	err := s.bucketSVC.CreateBucket(ctx, &influxBucket)
	if err != nil {
		return influxdb.Bucket{}, err
	}

	return influxBucket, nil
}

func (s *Service) applyLabels(labels []*label) applier {
	rollBackLabels := make([]*label, 0, len(labels))
	createFn := func(ctx context.Context, orgID influxdb.ID) error {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
		defer cancel()

		var errs applyErrs
		for i, l := range labels {
			labels[i].OrgID = orgID
			influxLabel, err := s.applyLabel(ctx, l)
			if err != nil {
				errs = append(errs, applyErrBody{
					name: l.Name,
					msg:  err.Error(),
				})
				continue
			}
			labels[i].id = influxLabel.ID
			rollBackLabels = append(rollBackLabels, labels[i])
		}

		return errs.toError("label", "failed to create label")
	}

	return applier{
		creater: createFn,
		rollbacker: rollbacker{
			resource: "label",
			fn:       func() error { return s.rollbackLabels(rollBackLabels) },
		},
	}
}

func (s *Service) rollbackLabels(labels []*label) error {
	var errs []string
	for _, l := range labels {
		err := s.labelSVC.DeleteLabel(context.Background(), l.ID())
		if err != nil {
			errs = append(errs, l.ID().String())
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf(`label_ids=[%s] err="unable to delete label"`, strings.Join(errs, ", "))
	}

	return nil
}

func (s *Service) applyLabel(ctx context.Context, l *label) (influxdb.Label, error) {
	if l.existing != nil {
		updatedlabel, err := s.labelSVC.UpdateLabel(ctx, l.ID(), influxdb.LabelUpdate{
			Properties: l.properties(),
		})
		if err != nil {
			return influxdb.Label{}, err
		}
		return *updatedlabel, nil
	}

	influxLabel := influxdb.Label{
		OrgID:      l.OrgID,
		Name:       l.Name,
		Properties: l.properties(),
	}
	err := s.labelSVC.CreateLabel(ctx, &influxLabel)
	if err != nil {
		return influxdb.Label{}, err
	}

	return influxLabel, nil
}

func (s *Service) applyLabelMappings(pkg *Pkg) applier {
	var mappings []influxdb.LabelMapping
	createFn := func(ctx context.Context, orgID influxdb.ID) error {
		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()

		labelMappings := pkg.labelMappings()
		for i := range labelMappings {
			mapping := labelMappings[i]
			if mapping.exists {
				// this block here does 2 things, it does note write a
				// mapping when one exists. it also avoids having to worry
				// about deleting an existing mapping since it will not be
				// passed to the delete function below b/c it is never added
				// to the list of mappings that is referenced in the delete
				// call.
				continue
			}
			err := s.labelSVC.CreateLabelMapping(ctx, &mapping.LabelMapping)
			if err != nil {
				return err
			}
			mappings = append(mappings, mapping.LabelMapping)
		}

		return nil
	}

	return applier{
		creater: createFn,
		rollbacker: rollbacker{
			resource: "label_mapping",
			fn:       func() error { return s.rollbackLabelMappings(mappings) },
		},
	}
}

func (s *Service) rollbackLabelMappings(mappings []influxdb.LabelMapping) error {
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

type rollbackCoordinator struct {
	rollbacks []rollbacker
}

func (r *rollbackCoordinator) runTilEnd(ctx context.Context, orgID influxdb.ID, appliers ...applier) error {
	var errs []string
	for _, app := range appliers {
		r.rollbacks = append(r.rollbacks, app.rollbacker)
		if err := app.creater(ctx, orgID); err != nil {
			errs = append(errs, fmt.Sprintf("failed %s create: %s", app.rollbacker.resource, err.Error()))
		}
	}

	if len(errs) > 0 {
		// TODO: fix error up to be more actionable
		return errors.New(strings.Join(errs, "\n"))
	}
	return nil
}

func (r *rollbackCoordinator) rollback(l *zap.Logger, err *error) {
	if *err == nil {
		return
	}

	for _, r := range r.rollbacks {
		if err := r.fn(); err != nil {
			l.Error("failed to delete "+r.resource, zap.Error(err))
		}
	}
}

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
