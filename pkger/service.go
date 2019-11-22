package pkger

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/influxdata/influxdb"
	ierrors "github.com/influxdata/influxdb/kit/errors"
	"go.uber.org/zap"
)

// APIVersion marks the current APIVersion for influx packages.
const APIVersion = "0.1.0"

// SVC is the packages service interface.
type SVC interface {
	CreatePkg(ctx context.Context, setters ...CreatePkgSetFn) (*Pkg, error)
	DryRun(ctx context.Context, orgID influxdb.ID, pkg *Pkg) (Summary, Diff, error)
	Apply(ctx context.Context, orgID influxdb.ID, pkg *Pkg) (Summary, error)
}

type serviceOpt struct {
	logger *zap.Logger

	labelSVC  influxdb.LabelService
	bucketSVC influxdb.BucketService
	dashSVC   influxdb.DashboardService
	varSVC    influxdb.VariableService
}

// ServiceSetterFn is a means of setting dependencies on the Service type.
type ServiceSetterFn func(opt *serviceOpt)

// WithLogger sets the service logger.
func WithLogger(logger *zap.Logger) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.logger = logger
	}
}

// WithBucketSVC sets the bucket service.
func WithBucketSVC(bktSVC influxdb.BucketService) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.bucketSVC = bktSVC
	}
}

// WithDashboardSVC sets the dashboard service.
func WithDashboardSVC(dashSVC influxdb.DashboardService) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.dashSVC = dashSVC
	}
}

// WithLabelSVC sets the label service.
func WithLabelSVC(labelSVC influxdb.LabelService) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.labelSVC = labelSVC
	}
}

// WithVariableSVC sets the variable service.
func WithVariableSVC(varSVC influxdb.VariableService) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.varSVC = varSVC
	}
}

// Service provides the pkger business logic including all the dependencies to make
// this resource sausage.
type Service struct {
	logger *zap.Logger

	labelSVC  influxdb.LabelService
	bucketSVC influxdb.BucketService
	dashSVC   influxdb.DashboardService
	varSVC    influxdb.VariableService
}

// NewService is a constructor for a pkger Service.
func NewService(opts ...ServiceSetterFn) *Service {
	opt := &serviceOpt{
		logger: zap.NewNop(),
	}
	for _, o := range opts {
		o(opt)
	}

	return &Service{
		logger:    opt.logger,
		bucketSVC: opt.bucketSVC,
		labelSVC:  opt.labelSVC,
		dashSVC:   opt.dashSVC,
		varSVC:    opt.varSVC,
	}
}

// CreatePkgSetFn is a functional input for setting the pkg fields.
type CreatePkgSetFn func(opt *CreateOpt) error

// CreateOpt are the options for creating a new package.
type CreateOpt struct {
	Metadata  Metadata
	OrgIDs    map[influxdb.ID]bool
	Resources []ResourceToClone
}

// CreateWithMetadata sets the metadata on the pkg in a CreatePkg call.
func CreateWithMetadata(meta Metadata) CreatePkgSetFn {
	return func(opt *CreateOpt) error {
		opt.Metadata = meta
		return nil
	}
}

// CreateWithExistingResources allows the create method to clone existing resources.
func CreateWithExistingResources(resources ...ResourceToClone) CreatePkgSetFn {
	return func(opt *CreateOpt) error {
		for _, r := range resources {
			if err := r.OK(); err != nil {
				return err
			}
			r.Kind = NewKind(string(r.Kind))
		}
		opt.Resources = append(opt.Resources, resources...)
		return nil
	}
}

// CreateWithAllOrgResources allows the create method to clone all existing resources
// for the given organization.
func CreateWithAllOrgResources(orgID influxdb.ID) CreatePkgSetFn {
	return func(opt *CreateOpt) error {
		if orgID == 0 {
			return errors.New("orgID provided must not be zero")
		}
		if opt.OrgIDs == nil {
			opt.OrgIDs = make(map[influxdb.ID]bool)
		}
		opt.OrgIDs[orgID] = true
		return nil
	}
}

// CreatePkg will produce a pkg from the parameters provided.
func (s *Service) CreatePkg(ctx context.Context, setters ...CreatePkgSetFn) (*Pkg, error) {
	opt := new(CreateOpt)
	for _, setter := range setters {
		if err := setter(opt); err != nil {
			return nil, err
		}
	}

	pkg := &Pkg{
		APIVersion: APIVersion,
		Kind:       KindPackage,
		Metadata:   opt.Metadata,
		Spec: struct {
			Resources []Resource `yaml:"resources" json:"resources"`
		}{
			Resources: make([]Resource, 0, len(opt.Resources)),
		},
	}
	if pkg.Metadata.Name == "" {
		// sudo randomness, this is not an attempt at making charts unique
		// that is a problem for the consumer.
		pkg.Metadata.Name = fmt.Sprintf("new_%7d", rand.Int())
	}
	if pkg.Metadata.Version == "" {
		pkg.Metadata.Version = "v1"
	}

	cloneAssFn := s.resourceCloneAssociationsGen()
	for orgID := range opt.OrgIDs {
		resourcesToClone, err := s.cloneOrgResources(ctx, orgID)
		if err != nil {
			return nil, err
		}
		opt.Resources = append(opt.Resources, resourcesToClone...)
	}

	for _, r := range uniqResourcesToClone(opt.Resources) {
		newResources, err := s.resourceCloneToResource(ctx, r, cloneAssFn)
		if err != nil {
			return nil, err
		}
		pkg.Spec.Resources = append(pkg.Spec.Resources, newResources...)
	}

	pkg.Spec.Resources = uniqResources(pkg.Spec.Resources)

	if err := pkg.Validate(ValidWithoutResources()); err != nil {
		return nil, err
	}

	var kindPriorities = map[Kind]int{
		KindLabel:     1,
		KindBucket:    2,
		KindVariable:  3,
		KindDashboard: 4,
	}

	sort.Slice(pkg.Spec.Resources, func(i, j int) bool {
		iName, jName := pkg.Spec.Resources[i].Name(), pkg.Spec.Resources[j].Name()
		iKind, _ := pkg.Spec.Resources[i].kind()
		jKind, _ := pkg.Spec.Resources[j].kind()

		if iKind.is(jKind) {
			return iName < jName
		}
		return kindPriorities[iKind] < kindPriorities[jKind]
	})

	return pkg, nil
}

func (s *Service) cloneOrgResources(ctx context.Context, orgID influxdb.ID) ([]ResourceToClone, error) {
	resourceTypeGens := []struct {
		resType influxdb.ResourceType
		cloneFn func(context.Context, influxdb.ID) ([]ResourceToClone, error)
	}{
		{
			resType: influxdb.BucketsResourceType,
			cloneFn: s.cloneOrgBuckets,
		},
		{
			resType: influxdb.DashboardsResourceType,
			cloneFn: s.cloneOrgDashboards,
		},
		{
			resType: influxdb.LabelsResourceType,
			cloneFn: s.cloneOrgLabels,
		},
		{
			resType: influxdb.VariablesResourceType,
			cloneFn: s.cloneOrgVariables,
		},
	}

	var resources []ResourceToClone
	for _, resGen := range resourceTypeGens {
		existingResources, err := resGen.cloneFn(ctx, orgID)
		if err != nil {
			return nil, ierrors.Wrap(err, "finding "+string(resGen.resType))
		}
		resources = append(resources, existingResources...)
	}

	return resources, nil
}

func (s *Service) cloneOrgBuckets(ctx context.Context, orgID influxdb.ID) ([]ResourceToClone, error) {
	buckets, _, err := s.bucketSVC.FindBuckets(ctx, influxdb.BucketFilter{
		OrganizationID: &orgID,
	})
	if err != nil {
		return nil, err
	}

	resources := make([]ResourceToClone, 0, len(buckets))
	for _, b := range buckets {
		if b.Type == influxdb.BucketTypeSystem {
			continue
		}
		resources = append(resources, ResourceToClone{
			Kind: KindBucket,
			ID:   b.ID,
		})
	}
	return resources, nil
}

func (s *Service) cloneOrgDashboards(ctx context.Context, orgID influxdb.ID) ([]ResourceToClone, error) {
	dashs, _, err := s.dashSVC.FindDashboards(ctx, influxdb.DashboardFilter{
		OrganizationID: &orgID,
	}, influxdb.FindOptions{Limit: 100})
	if err != nil {
		return nil, err
	}

	resources := make([]ResourceToClone, 0, len(dashs))
	for _, d := range dashs {
		resources = append(resources, ResourceToClone{
			Kind: KindDashboard,
			ID:   d.ID,
		})
	}
	return resources, nil
}

func (s *Service) cloneOrgLabels(ctx context.Context, orgID influxdb.ID) ([]ResourceToClone, error) {
	labels, err := s.labelSVC.FindLabels(ctx, influxdb.LabelFilter{
		OrgID: &orgID,
	}, influxdb.FindOptions{Limit: 10000})
	if err != nil {
		return nil, ierrors.Wrap(err, "finding labels")
	}

	resources := make([]ResourceToClone, 0, len(labels))
	for _, l := range labels {
		resources = append(resources, ResourceToClone{
			Kind: KindLabel,
			ID:   l.ID,
		})
	}
	return resources, nil
}

func (s *Service) cloneOrgVariables(ctx context.Context, orgID influxdb.ID) ([]ResourceToClone, error) {
	vars, err := s.varSVC.FindVariables(ctx, influxdb.VariableFilter{
		OrganizationID: &orgID,
	}, influxdb.FindOptions{Limit: 10000})
	if err != nil {
		return nil, err
	}

	resources := make([]ResourceToClone, 0, len(vars))
	for _, v := range vars {
		resources = append(resources, ResourceToClone{
			Kind: KindVariable,
			ID:   v.ID,
		})
	}

	return resources, nil
}

func (s *Service) resourceCloneToResource(ctx context.Context, r ResourceToClone, cFn cloneAssociationsFn) ([]Resource, error) {
	var newResource Resource
	switch {
	case r.Kind.is(KindBucket):
		bkt, err := s.bucketSVC.FindBucketByID(ctx, r.ID)
		if err != nil {
			return nil, err
		}
		newResource = bucketToResource(*bkt, r.Name)
	case r.Kind.is(KindDashboard):
		dash, err := s.dashSVC.FindDashboardByID(ctx, r.ID)
		if err != nil {
			return nil, err
		}
		var cellViews []cellView
		for _, cell := range dash.Cells {
			v, err := s.dashSVC.GetDashboardCellView(ctx, r.ID, cell.ID)
			if err != nil {
				return nil, err
			}
			cellViews = append(cellViews, cellView{c: *cell, v: *v})
		}
		newResource = dashboardToResource(*dash, cellViews, r.Name)
	case r.Kind.is(KindLabel):
		l, err := s.labelSVC.FindLabelByID(ctx, r.ID)
		if err != nil {
			return nil, err
		}
		newResource = labelToResource(*l, r.Name)
	case r.Kind.is(KindVariable):
		v, err := s.varSVC.FindVariableByID(ctx, r.ID)
		if err != nil {
			return nil, err
		}
		newResource = variableToResource(*v, r.Name)
	default:
		return nil, errors.New("unsupported kind provided: " + string(r.Kind))
	}

	ass, err := cFn(ctx, r)
	if err != nil {
		return nil, err
	}
	if len(ass.associations) > 0 {
		newResource[fieldAssociations] = ass.associations
	}

	return append([]Resource{newResource}, ass.newLableResources...), nil
}

type (
	associations struct {
		associations      []Resource
		newLableResources []Resource
	}

	cloneAssociationsFn func(context.Context, ResourceToClone) (associations, error)
)

func (s *Service) resourceCloneAssociationsGen() cloneAssociationsFn {
	type key struct {
		id   influxdb.ID
		name string
	}
	// memoize the labels so we dont' create duplicates
	m := make(map[key]bool)
	return func(ctx context.Context, r ResourceToClone) (associations, error) {
		var iResType influxdb.ResourceType
		switch {
		case r.Kind.is(KindBucket):
			iResType = influxdb.BucketsResourceType
		case r.Kind.is(KindDashboard):
			iResType = influxdb.DashboardsResourceType
		case r.Kind.is(KindVariable):
			iResType = influxdb.VariablesResourceType
		default:
			return associations{}, nil
		}

		labels, err := s.labelSVC.FindResourceLabels(ctx, influxdb.LabelMappingFilter{
			ResourceID:   r.ID,
			ResourceType: iResType,
		})
		if err != nil {
			return associations{}, err
		}

		var ass associations
		for _, l := range labels {
			ass.associations = append(ass.associations, Resource{
				fieldKind: KindLabel.String(),
				fieldName: l.Name,
			})
			k := key{id: l.ID, name: l.Name}
			if m[k] {
				continue
			}
			m[k] = true
			ass.newLableResources = append(ass.newLableResources, labelToResource(*l, ""))
		}
		return ass, nil
	}
}

// DryRun provides a dry run of the pkg application. The pkg will be marked verified
// for later calls to Apply. This func will be run on an Apply if it has not been run
// already.
func (s *Service) DryRun(ctx context.Context, orgID influxdb.ID, pkg *Pkg) (Summary, Diff, error) {
	// so here's the deal, when we have issues with the parsing validation, we
	// continue to do the diff anyhow. any resource that does not have a name
	// will be skipped, and won't bleed into the dry run here. We can now return
	// a error (parseErr) and valid diff/summary.
	var parseErr error
	if !pkg.isParsed {
		err := pkg.Validate()
		if err != nil && !IsParseErr(err) {
			return Summary{}, Diff{}, err
		}
		parseErr = err
	}

	diffBuckets, err := s.dryRunBuckets(ctx, orgID, pkg)
	if err != nil {
		return Summary{}, Diff{}, err
	}

	diffDashes, err := s.dryRunDashboards(ctx, orgID, pkg)
	if err != nil {
		return Summary{}, Diff{}, err
	}

	diffLabels, err := s.dryRunLabels(ctx, orgID, pkg)
	if err != nil {
		return Summary{}, Diff{}, err
	}

	diffVars, err := s.dryRunVariables(ctx, orgID, pkg)
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
		Dashboards:    diffDashes,
		Labels:        diffLabels,
		LabelMappings: diffLabelMappings,
		Variables:     diffVars,
	}
	return pkg.Summary(), diff, parseErr
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
			mExistingBkts[b.Name] = newDiffBucket(b, existingBkt)
		default:
			mExistingBkts[b.Name] = newDiffBucket(b, nil)
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

func (s *Service) dryRunDashboards(ctx context.Context, orgID influxdb.ID, pkg *Pkg) ([]DiffDashboard, error) {
	var diffs []DiffDashboard
	for _, d := range pkg.dashboards() {
		diffs = append(diffs, newDiffDashboard(d))
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
		pkgLabel := labels[i]
		existingLabels, err := s.labelSVC.FindLabels(ctx, influxdb.LabelFilter{
			Name:  pkgLabel.Name,
			OrgID: &orgID,
		}, influxdb.FindOptions{Limit: 1})
		switch {
		// TODO: case for err not found here and another case handle where
		//  err isn't a not found (some other error)
		case err == nil && len(existingLabels) > 0:
			existingLabel := existingLabels[0]
			pkgLabel.existing = existingLabel
			mExistingLabels[pkgLabel.Name] = newDiffLabel(pkgLabel, existingLabel)
		default:
			mExistingLabels[pkgLabel.Name] = newDiffLabel(pkgLabel, nil)
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

func (s *Service) dryRunVariables(ctx context.Context, orgID influxdb.ID, pkg *Pkg) ([]DiffVariable, error) {
	mExistingLabels := make(map[string]DiffVariable)
	variables := pkg.variables()

VarLoop:
	for i := range variables {
		pkgVar := variables[i]
		existingLabels, err := s.varSVC.FindVariables(ctx, influxdb.VariableFilter{
			OrganizationID: &orgID,
			// TODO: would be ideal to extend find variables to allow for a name matcher
			//  since names are unique for vars within an org, meanwhile, make large limit
			// 	returned vars, should be more than enough for the time being.
		}, influxdb.FindOptions{Limit: 10000})
		switch {
		case err == nil && len(existingLabels) > 0:
			for i := range existingLabels {
				existingVar := existingLabels[i]
				if existingVar.Name != pkgVar.Name {
					continue
				}
				pkgVar.existing = existingVar
				mExistingLabels[pkgVar.Name] = newDiffVariable(pkgVar, existingVar)
				continue VarLoop
			}
			// fallthrough here for when the variable is not found, it'll fall to the
			// default case and add it as new.
			fallthrough
		default:
			mExistingLabels[pkgVar.Name] = newDiffVariable(pkgVar, nil)
		}
	}

	diffs := make([]DiffVariable, 0, len(mExistingLabels))
	for _, diff := range mExistingLabels {
		diffs = append(diffs, diff)
	}
	sort.Slice(diffs, func(i, j int) bool {
		return diffs[i].Name < diffs[j].Name
	})

	return diffs, nil
}

type (
	labelMappingDiffFn func(labelID influxdb.ID, labelName string, isNew bool)

	labelAssociater interface {
		ID() influxdb.ID
		ResourceType() influxdb.ResourceType
		Exists() bool
	}
)

func (s *Service) dryRunLabelMappings(ctx context.Context, pkg *Pkg) ([]DiffLabelMapping, error) {
	var diffs []DiffLabelMapping
	for _, b := range pkg.buckets() {
		err := s.dryRunResourceLabelMapping(ctx, b, b.labels, func(labelID influxdb.ID, labelName string, isNew bool) {
			if l, ok := pkg.mLabels[labelName]; ok {
				l.setBucketMapping(b, !isNew)
			}
			diffs = append(diffs, DiffLabelMapping{
				IsNew:     isNew,
				ResType:   b.ResourceType(),
				ResID:     SafeID(b.ID()),
				ResName:   b.Name,
				LabelID:   SafeID(labelID),
				LabelName: labelName,
			})
		})
		if err != nil {
			return nil, err
		}
	}

	for _, d := range pkg.dashboards() {
		err := s.dryRunResourceLabelMapping(ctx, d, d.labels, func(labelID influxdb.ID, labelName string, isNew bool) {
			pkg.mLabels[labelName].setDashboardMapping(d)
			diffs = append(diffs, DiffLabelMapping{
				IsNew:     isNew,
				ResType:   d.ResourceType(),
				ResID:     SafeID(d.ID()),
				ResName:   d.Name,
				LabelID:   SafeID(labelID),
				LabelName: labelName,
			})
		})
		if err != nil {
			return nil, err
		}
	}

	for _, v := range pkg.variables() {
		err := s.dryRunResourceLabelMapping(ctx, v, v.labels, func(labelID influxdb.ID, labelName string, isNew bool) {
			pkg.mLabels[labelName].setVariableMapping(v, !isNew)
			diffs = append(diffs, DiffLabelMapping{
				IsNew:     isNew,
				ResType:   v.ResourceType(),
				ResID:     SafeID(v.ID()),
				ResName:   v.Name,
				LabelID:   SafeID(labelID),
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

func (s *Service) dryRunResourceLabelMapping(ctx context.Context, la labelAssociater, labels []*label, mappingFn labelMappingDiffFn) error {
	if !la.Exists() {
		for _, l := range labels {
			mappingFn(l.ID(), l.Name, true)
		}
		return nil
	}

	// loop through and hit api for all labels associated with a bkt
	// lookup labels in pkg, add it to the label mapping, if exists in
	// the results from API, mark it exists
	existingLabels, err := s.labelSVC.FindResourceLabels(ctx, influxdb.LabelMappingFilter{
		ResourceID:   la.ID(),
		ResourceType: la.ResourceType(),
	})
	if err != nil {
		// TODO: inspect err, if its a not found error, do nothing, if any other error
		//  handle it better
		return err
	}

	pkgLabels := labelSlcToMap(labels)
	for _, l := range existingLabels {
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

// Apply will apply all the resources identified in the provided pkg. The entire pkg will be applied
// in its entirety. If a failure happens midway then the entire pkg will be rolled back to the state
// from before the pkg were applied.
func (s *Service) Apply(ctx context.Context, orgID influxdb.ID, pkg *Pkg) (sum Summary, e error) {
	if !pkg.isParsed {
		if err := pkg.Validate(); err != nil {
			return Summary{}, err
		}
	}

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
			s.applyVariables(pkg.variables()),
			s.applyBuckets(pkg.buckets()),
			s.applyDashboards(pkg.dashboards()),
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
	const resource = "bucket"

	rollbackBuckets := make([]*bucket, 0, len(buckets))
	createFn := func(ctx context.Context, orgID influxdb.ID) error {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
		defer cancel()

		var errs applyErrs
		for i, b := range buckets {
			buckets[i].OrgID = orgID
			if !b.shouldApply() {
				continue
			}
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

		return errs.toError(resource, "failed to create bucket")
	}

	return applier{
		creater: createFn,
		rollbacker: rollbacker{
			resource: resource,
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

		rp := b.RetentionRules.RP()
		_, err := s.bucketSVC.UpdateBucket(context.Background(), b.ID(), influxdb.BucketUpdate{
			Description:     &b.Description,
			RetentionPeriod: &rp,
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
	rp := b.RetentionRules.RP()
	if b.existing != nil {
		influxBucket, err := s.bucketSVC.UpdateBucket(ctx, b.ID(), influxdb.BucketUpdate{
			Description:     &b.Description,
			RetentionPeriod: &rp,
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
		RetentionPeriod: rp,
	}
	err := s.bucketSVC.CreateBucket(ctx, &influxBucket)
	if err != nil {
		return influxdb.Bucket{}, err
	}

	return influxBucket, nil
}

func (s *Service) applyDashboards(dashboards []*dashboard) applier {
	const resource = "dashboard"

	rollbackDashboards := make([]*dashboard, 0, len(dashboards))
	createFn := func(ctx context.Context, orgID influxdb.ID) error {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
		defer cancel()

		var errs applyErrs
		for i := range dashboards {
			d := dashboards[i]
			d.OrgID = orgID
			influxBucket, err := s.applyDashboard(ctx, d)
			if err != nil {
				errs = append(errs, applyErrBody{
					name: d.Name,
					msg:  err.Error(),
				})
				continue
			}
			d.id = influxBucket.ID
			rollbackDashboards = append(rollbackDashboards, d)
		}

		return errs.toError(resource, "failed to create bucket")
	}

	return applier{
		creater: createFn,
		rollbacker: rollbacker{
			resource: resource,
			fn:       func() error { return s.rollbackDashboards(rollbackDashboards) },
		},
	}
}

func (s *Service) rollbackDashboards(dashboards []*dashboard) error {
	var errs []string
	for _, d := range dashboards {
		err := s.dashSVC.DeleteDashboard(context.Background(), d.ID())
		if err != nil {
			errs = append(errs, d.ID().String())
		}
	}

	if len(errs) > 0 {
		// TODO: fixup error
		return fmt.Errorf(`dashboard_ids=[%s] err="unable to delete dashboard"`, strings.Join(errs, ", "))
	}

	return nil
}

func (s *Service) applyDashboard(ctx context.Context, d *dashboard) (influxdb.Dashboard, error) {
	cells, cellChartMap := convertChartsToCells(d.Charts)
	influxDashboard := influxdb.Dashboard{
		OrganizationID: d.OrgID,
		Description:    d.Description,
		Name:           d.Name,
		Cells:          cells,
	}
	err := s.dashSVC.CreateDashboard(ctx, &influxDashboard)
	if err != nil {
		return influxdb.Dashboard{}, err
	}

	for cell, i := range cellChartMap {
		ch := d.Charts[i]

		_, err := s.dashSVC.UpdateDashboardCellView(ctx, influxDashboard.ID, cell.ID, influxdb.ViewUpdate{
			ViewContentsUpdate: influxdb.ViewContentsUpdate{
				Name: &ch.Name,
			},
			Properties: ch.properties(),
		})
		if err != nil {
			return influxdb.Dashboard{}, err
		}
	}

	return influxDashboard, nil
}

func convertChartsToCells(ch []chart) ([]*influxdb.Cell, map[*influxdb.Cell]int) {
	cellChartMap := make(map[*influxdb.Cell]int)
	icells := make([]*influxdb.Cell, 0, len(ch))
	for i, c := range ch {
		icell := &influxdb.Cell{
			CellProperty: influxdb.CellProperty{
				X: int32(c.XPos),
				Y: int32(c.YPos),
				H: int32(c.Height),
				W: int32(c.Width),
			},
		}
		cellChartMap[icell] = i
		icells = append(icells, icell)
	}
	return icells, cellChartMap
}

func (s *Service) applyLabels(labels []*label) applier {
	const resource = "label"

	rollBackLabels := make([]*label, 0, len(labels))
	createFn := func(ctx context.Context, orgID influxdb.ID) error {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
		defer cancel()

		var errs applyErrs
		for i, l := range labels {
			labels[i].OrgID = orgID
			if !l.shouldApply() {
				continue
			}
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

		return errs.toError(resource, "failed to create label")
	}

	return applier{
		creater: createFn,
		rollbacker: rollbacker{
			resource: resource,
			fn:       func() error { return s.rollbackLabels(rollBackLabels) },
		},
	}
}

func (s *Service) rollbackLabels(labels []*label) error {
	var errs []string
	for _, l := range labels {
		if l.existing == nil {
			err := s.labelSVC.DeleteLabel(context.Background(), l.ID())
			if err != nil {
				errs = append(errs, l.ID().String())
			}
			continue
		}

		_, err := s.labelSVC.UpdateLabel(context.Background(), l.ID(), influxdb.LabelUpdate{
			Properties: l.existing.Properties,
		})
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

func (s *Service) applyVariables(vars []*variable) applier {
	const resource = "variable"

	rollBackVars := make([]*variable, 0, len(vars))
	createFn := func(ctx context.Context, orgID influxdb.ID) error {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
		defer cancel()

		var errs applyErrs
		for i, v := range vars {
			vars[i].OrgID = orgID
			if !v.shouldApply() {
				continue
			}
			influxVar, err := s.applyVariable(ctx, v)
			if err != nil {
				errs = append(errs, applyErrBody{
					name: v.Name,
					msg:  err.Error(),
				})
				continue
			}
			vars[i].id = influxVar.ID
			rollBackVars = append(rollBackVars, vars[i])
		}

		return errs.toError(resource, "failed to create variable")
	}

	return applier{
		creater: createFn,
		rollbacker: rollbacker{
			resource: resource,
			fn:       func() error { return s.rollbackVariables(rollBackVars) },
		},
	}
}

func (s *Service) rollbackVariables(variables []*variable) error {
	var errs []string
	for _, v := range variables {
		if v.existing == nil {
			err := s.varSVC.DeleteVariable(context.Background(), v.ID())
			if err != nil {
				errs = append(errs, v.ID().String())
			}
			continue
		}

		_, err := s.varSVC.UpdateVariable(context.Background(), v.ID(), &influxdb.VariableUpdate{
			Description: v.existing.Description,
			Arguments:   v.existing.Arguments,
		})
		if err != nil {
			errs = append(errs, v.ID().String())
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf(`variable_ids=[%s] err="unable to delete variable"`, strings.Join(errs, ", "))
	}

	return nil
}

func (s *Service) applyVariable(ctx context.Context, v *variable) (influxdb.Variable, error) {
	if v.existing != nil {
		updatedVar, err := s.varSVC.UpdateVariable(ctx, v.ID(), &influxdb.VariableUpdate{
			Description: v.Description,
			Arguments:   v.influxVarArgs(),
		})
		if err != nil {
			return influxdb.Variable{}, err
		}
		return *updatedVar, nil
	}

	influxVar := influxdb.Variable{
		OrganizationID: v.OrgID,
		Name:           v.Name,
		Description:    v.Description,
		Arguments:      v.influxVarArgs(),
	}
	err := s.varSVC.CreateVariable(ctx, &influxVar)
	if err != nil {
		return influxdb.Variable{}, err
	}

	return influxVar, nil
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

func labelSlcToMap(labels []*label) map[string]*label {
	m := make(map[string]*label)
	for i := range labels {
		m[labels[i].Name] = labels[i]
	}
	return m
}
