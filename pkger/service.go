package pkger

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-stack/stack"
	"github.com/influxdata/influxdb/v2"
	ierrors "github.com/influxdata/influxdb/v2/kit/errors"
	"github.com/influxdata/influxdb/v2/kit/platform"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
	icheck "github.com/influxdata/influxdb/v2/notification/check"
	"github.com/influxdata/influxdb/v2/notification/rule"
	"github.com/influxdata/influxdb/v2/pkger/internal/wordplay"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/task/options"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"go.uber.org/zap"
)

// APIVersion marks the current APIVersion for influx packages.
const APIVersion = "influxdata.com/v2alpha1"
const APIVersion2 = "influxdata.com/v2alpha2"

// Stack is an identifier for stateful application of a package(s). This stack
// will map created resources from the template(s) to existing resources on the
// platform. This stack is updated only after side effects of applying a template.
// If the template is applied, and no changes are had, then the stack is not updated.
type Stack struct {
	ID        platform.ID
	OrgID     platform.ID
	CreatedAt time.Time `json:"createdAt"`
	Events    []StackEvent
}

func (s Stack) LatestEvent() StackEvent {
	if len(s.Events) == 0 {
		return StackEvent{}
	}
	sort.Slice(s.Events, func(i, j int) bool {
		return s.Events[i].UpdatedAt.Before(s.Events[j].UpdatedAt)
	})
	return s.Events[len(s.Events)-1]
}

type (
	StackEvent struct {
		EventType    StackEventType
		Name         string
		Description  string
		Sources      []string
		TemplateURLs []string
		Resources    []StackResource
		UpdatedAt    time.Time `json:"updatedAt"`
	}

	StackCreate struct {
		OrgID        platform.ID
		Name         string
		Description  string
		Sources      []string
		TemplateURLs []string
		Resources    []StackResource
	}

	// StackResource is a record for an individual resource side effect generated from
	// applying a template.
	StackResource struct {
		APIVersion   string
		ID           platform.ID
		Name         string
		Kind         Kind
		MetaName     string
		Associations []StackResourceAssociation
	}

	// StackResourceAssociation associates a stack resource with another stack resource.
	StackResourceAssociation struct {
		Kind     Kind
		MetaName string
	}

	// StackUpdate provides a means to update an existing stack.
	StackUpdate struct {
		ID                  platform.ID
		Name                *string
		Description         *string
		TemplateURLs        []string
		AdditionalResources []StackAdditionalResource
	}

	StackAdditionalResource struct {
		APIVersion string
		ID         platform.ID
		Kind       Kind
		MetaName   string
	}
)

type StackEventType uint

const (
	StackEventCreate StackEventType = iota
	StackEventUpdate
	StackEventUninstalled
)

func (e StackEventType) String() string {
	switch e {
	case StackEventCreate:
		return "create"
	case StackEventUninstalled:
		return "uninstall"
	case StackEventUpdate:
		return "update"
	default:
		return "unknown"
	}
}

const ResourceTypeStack influxdb.ResourceType = "stack"

// SVC is the packages service interface.
type SVC interface {
	InitStack(ctx context.Context, userID platform.ID, stack StackCreate) (Stack, error)
	UninstallStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID platform.ID }) (Stack, error)
	DeleteStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID platform.ID }) error
	ListStacks(ctx context.Context, orgID platform.ID, filter ListFilter) ([]Stack, error)
	ReadStack(ctx context.Context, id platform.ID) (Stack, error)
	UpdateStack(ctx context.Context, upd StackUpdate) (Stack, error)

	Export(ctx context.Context, opts ...ExportOptFn) (*Template, error)
	DryRun(ctx context.Context, orgID, userID platform.ID, opts ...ApplyOptFn) (ImpactSummary, error)
	Apply(ctx context.Context, orgID, userID platform.ID, opts ...ApplyOptFn) (ImpactSummary, error)
}

// SVCMiddleware is a service middleware func.
type SVCMiddleware func(SVC) SVC

type serviceOpt struct {
	logger *zap.Logger

	applyReqLimit int
	client        *http.Client
	idGen         platform.IDGenerator
	nameGen       NameGenerator
	timeGen       influxdb.TimeGenerator
	store         Store

	bucketSVC   influxdb.BucketService
	checkSVC    influxdb.CheckService
	dashSVC     influxdb.DashboardService
	labelSVC    influxdb.LabelService
	endpointSVC influxdb.NotificationEndpointService
	orgSVC      influxdb.OrganizationService
	ruleSVC     influxdb.NotificationRuleStore
	secretSVC   influxdb.SecretService
	taskSVC     taskmodel.TaskService
	teleSVC     influxdb.TelegrafConfigStore
	varSVC      influxdb.VariableService
}

// ServiceSetterFn is a means of setting dependencies on the Service type.
type ServiceSetterFn func(opt *serviceOpt)

// WithHTTPClient sets the http client for the service.
func WithHTTPClient(c *http.Client) ServiceSetterFn {
	return func(o *serviceOpt) {
		o.client = c
	}
}

// WithLogger sets the logger for the service.
func WithLogger(log *zap.Logger) ServiceSetterFn {
	return func(o *serviceOpt) {
		o.logger = log
	}
}

// WithIDGenerator sets the id generator for the service.
func WithIDGenerator(idGen platform.IDGenerator) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.idGen = idGen
	}
}

// WithTimeGenerator sets the time generator for the service.
func WithTimeGenerator(timeGen influxdb.TimeGenerator) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.timeGen = timeGen
	}
}

// WithStore sets the store for the service.
func WithStore(store Store) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.store = store
	}
}

// WithBucketSVC sets the bucket service.
func WithBucketSVC(bktSVC influxdb.BucketService) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.bucketSVC = bktSVC
	}
}

// WithCheckSVC sets the check service.
func WithCheckSVC(checkSVC influxdb.CheckService) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.checkSVC = checkSVC
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

func withNameGen(nameGen NameGenerator) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.nameGen = nameGen
	}
}

// WithNotificationEndpointSVC sets the endpoint notification service.
func WithNotificationEndpointSVC(endpointSVC influxdb.NotificationEndpointService) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.endpointSVC = endpointSVC
	}
}

// WithNotificationRuleSVC sets the endpoint rule service.
func WithNotificationRuleSVC(ruleSVC influxdb.NotificationRuleStore) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.ruleSVC = ruleSVC
	}
}

// WithOrganizationService sets the organization service for the service.
func WithOrganizationService(orgSVC influxdb.OrganizationService) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.orgSVC = orgSVC
	}
}

// WithSecretSVC sets the secret service.
func WithSecretSVC(secretSVC influxdb.SecretService) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.secretSVC = secretSVC
	}
}

// WithTaskSVC sets the task service.
func WithTaskSVC(taskSVC taskmodel.TaskService) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.taskSVC = taskSVC
	}
}

// WithTelegrafSVC sets the telegraf service.
func WithTelegrafSVC(telegrafSVC influxdb.TelegrafConfigStore) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.teleSVC = telegrafSVC
	}
}

// WithVariableSVC sets the variable service.
func WithVariableSVC(varSVC influxdb.VariableService) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.varSVC = varSVC
	}
}

// Store is the storage behavior the Service depends on.
type Store interface {
	CreateStack(ctx context.Context, stack Stack) error
	ListStacks(ctx context.Context, orgID platform.ID, filter ListFilter) ([]Stack, error)
	ReadStackByID(ctx context.Context, id platform.ID) (Stack, error)
	UpdateStack(ctx context.Context, stack Stack) error
	DeleteStack(ctx context.Context, id platform.ID) error
}

// Service provides the template business logic including all the dependencies to make
// this resource sausage.
type Service struct {
	log *zap.Logger

	// internal dependencies
	applyReqLimit int
	client        *http.Client
	idGen         platform.IDGenerator
	nameGen       NameGenerator
	store         Store
	timeGen       influxdb.TimeGenerator

	// external service dependencies
	bucketSVC   influxdb.BucketService
	checkSVC    influxdb.CheckService
	dashSVC     influxdb.DashboardService
	labelSVC    influxdb.LabelService
	endpointSVC influxdb.NotificationEndpointService
	orgSVC      influxdb.OrganizationService
	ruleSVC     influxdb.NotificationRuleStore
	secretSVC   influxdb.SecretService
	taskSVC     taskmodel.TaskService
	teleSVC     influxdb.TelegrafConfigStore
	varSVC      influxdb.VariableService
}

var _ SVC = (*Service)(nil)

// NewService is a constructor for a template Service.
func NewService(opts ...ServiceSetterFn) *Service {
	opt := &serviceOpt{
		logger:        zap.NewNop(),
		applyReqLimit: 5,
		idGen:         snowflake.NewDefaultIDGenerator(),
		nameGen:       wordplay.GetRandomName,
		timeGen:       influxdb.RealTimeGenerator{},
	}
	for _, o := range opts {
		o(opt)
	}

	return &Service{
		log: opt.logger,

		applyReqLimit: opt.applyReqLimit,
		client:        opt.client,
		idGen:         opt.idGen,
		nameGen:       opt.nameGen,
		store:         opt.store,
		timeGen:       opt.timeGen,

		bucketSVC:   opt.bucketSVC,
		checkSVC:    opt.checkSVC,
		labelSVC:    opt.labelSVC,
		dashSVC:     opt.dashSVC,
		endpointSVC: opt.endpointSVC,
		orgSVC:      opt.orgSVC,
		ruleSVC:     opt.ruleSVC,
		secretSVC:   opt.secretSVC,
		taskSVC:     opt.taskSVC,
		teleSVC:     opt.teleSVC,
		varSVC:      opt.varSVC,
	}
}

// InitStack will create a new stack for the given user and its given org. The stack can be created
// with urls that point to the location of packages that are included as part of the stack when
// it is applied.
func (s *Service) InitStack(ctx context.Context, userID platform.ID, stCreate StackCreate) (Stack, error) {
	if err := validURLs(stCreate.TemplateURLs); err != nil {
		return Stack{}, err
	}

	// Reject use of server-side jsonnet with stack templates
	for _, u := range stCreate.TemplateURLs {
		// While things like '.%6Aonnet' evaluate to the default encoding (yaml), let's unescape and catch those too
		decoded, err := url.QueryUnescape(u)
		if err != nil {
			msg := fmt.Sprintf("stack template from url[%q] had an issue", u)
			return Stack{}, influxErr(errors2.EInvalid, msg)
		}

		if strings.HasSuffix(strings.ToLower(decoded), "jsonnet") {
			msg := fmt.Sprintf("stack template from url[%q] had an issue: %s", u, ErrInvalidEncoding.Error())
			return Stack{}, influxErr(errors2.EUnprocessableEntity, msg)
		}
	}

	if _, err := s.orgSVC.FindOrganizationByID(ctx, stCreate.OrgID); err != nil {
		if errors2.ErrorCode(err) == errors2.ENotFound {
			msg := fmt.Sprintf("organization dependency does not exist for id[%q]", stCreate.OrgID.String())
			return Stack{}, influxErr(errors2.EConflict, msg)
		}
		return Stack{}, internalErr(err)
	}

	now := s.timeGen.Now()
	newStack := Stack{
		ID:        s.idGen.ID(),
		OrgID:     stCreate.OrgID,
		CreatedAt: now,
		Events: []StackEvent{
			{
				EventType:    StackEventCreate,
				Name:         stCreate.Name,
				Description:  stCreate.Description,
				Resources:    stCreate.Resources,
				TemplateURLs: stCreate.TemplateURLs,
				UpdatedAt:    now,
			},
		},
	}
	if err := s.store.CreateStack(ctx, newStack); err != nil {
		return Stack{}, internalErr(err)
	}

	return newStack, nil
}

// UninstallStack will remove all resources associated with the stack.
func (s *Service) UninstallStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID platform.ID }) (Stack, error) {
	uninstalledStack, err := s.uninstallStack(ctx, identifiers)
	if err != nil {
		return Stack{}, err
	}

	ev := uninstalledStack.LatestEvent()
	ev.EventType = StackEventUninstalled
	ev.Resources = nil
	ev.UpdatedAt = s.timeGen.Now()

	uninstalledStack.Events = append(uninstalledStack.Events, ev)
	if err := s.store.UpdateStack(ctx, uninstalledStack); err != nil {
		s.log.Error("unable to update stack after uninstalling resources", zap.Error(err))
	}
	return uninstalledStack, nil
}

// DeleteStack removes a stack and all the resources that have are associated with the stack.
func (s *Service) DeleteStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID platform.ID }) (e error) {
	deletedStack, err := s.uninstallStack(ctx, identifiers)
	if errors2.ErrorCode(err) == errors2.ENotFound {
		return nil
	}
	if err != nil {
		return err
	}

	return s.store.DeleteStack(ctx, deletedStack.ID)
}

func (s *Service) uninstallStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID platform.ID }) (_ Stack, e error) {
	stack, err := s.store.ReadStackByID(ctx, identifiers.StackID)
	if err != nil {
		return Stack{}, err
	}
	if stack.OrgID != identifiers.OrgID {
		return Stack{}, &errors2.Error{
			Code: errors2.EConflict,
			Msg:  "you do not have access to given stack ID",
		}
	}

	// providing empty template will remove all applied resources
	state, err := s.dryRun(ctx, identifiers.OrgID, new(Template), applyOptFromOptFns(ApplyWithStackID(identifiers.StackID)))
	if err != nil {
		return Stack{}, err
	}

	coordinator := newRollbackCoordinator(s.log, s.applyReqLimit)
	defer coordinator.rollback(s.log, &e, identifiers.OrgID)

	err = s.applyState(ctx, coordinator, identifiers.OrgID, identifiers.UserID, state, nil)
	if err != nil {
		return Stack{}, err
	}
	return stack, nil
}

// ListFilter are filter options for filtering stacks from being returned.
type ListFilter struct {
	StackIDs []platform.ID
	Names    []string
}

// ListStacks returns a list of stacks.
func (s *Service) ListStacks(ctx context.Context, orgID platform.ID, f ListFilter) ([]Stack, error) {
	return s.store.ListStacks(ctx, orgID, f)
}

// ReadStack returns a stack that matches the given id.
func (s *Service) ReadStack(ctx context.Context, id platform.ID) (Stack, error) {
	return s.store.ReadStackByID(ctx, id)
}

// UpdateStack updates the stack by the given parameters.
func (s *Service) UpdateStack(ctx context.Context, upd StackUpdate) (Stack, error) {
	existing, err := s.ReadStack(ctx, upd.ID)
	if err != nil {
		return Stack{}, err
	}

	// Reject use of server-side jsonnet with stack templates
	for _, u := range upd.TemplateURLs {
		// While things like '.%6Aonnet' evaluate to the default encoding (yaml), let's unescape and catch those too
		decoded, err := url.QueryUnescape(u)
		if err != nil {
			msg := fmt.Sprintf("stack template from url[%q] had an issue", u)
			return Stack{}, influxErr(errors2.EInvalid, msg)
		}

		if strings.HasSuffix(strings.ToLower(decoded), "jsonnet") {
			msg := fmt.Sprintf("stack template from url[%q] had an issue: %s", u, ErrInvalidEncoding.Error())
			return Stack{}, influxErr(errors2.EUnprocessableEntity, msg)
		}
	}

	updatedStack := s.applyStackUpdate(existing, upd)
	if err := s.store.UpdateStack(ctx, updatedStack); err != nil {
		return Stack{}, err
	}

	return updatedStack, nil
}

func (s *Service) applyStackUpdate(existing Stack, upd StackUpdate) Stack {
	ev := existing.LatestEvent()
	ev.EventType = StackEventUpdate
	ev.UpdatedAt = s.timeGen.Now()
	if upd.Name != nil {
		ev.Name = *upd.Name
	}
	if upd.Description != nil {
		ev.Description = *upd.Description
	}
	if upd.TemplateURLs != nil {
		ev.TemplateURLs = upd.TemplateURLs
	}

	type key struct {
		k  Kind
		id platform.ID
	}
	mExistingResources := make(map[key]bool)
	mExistingNames := make(map[string]bool)
	for _, r := range ev.Resources {
		k := key{k: r.Kind, id: r.ID}
		mExistingResources[k] = true
		mExistingNames[r.MetaName] = true
	}

	var out []StackResource
	for _, r := range upd.AdditionalResources {
		k := key{k: r.Kind, id: r.ID}
		if mExistingResources[k] {
			continue
		}

		sr := StackResource{
			APIVersion: r.APIVersion,
			ID:         r.ID,
			Kind:       r.Kind,
		}

		metaName := r.MetaName
		if metaName == "" || mExistingNames[metaName] {
			metaName = uniqMetaName(s.nameGen, s.idGen, mExistingNames)
		}
		mExistingNames[metaName] = true
		sr.MetaName = metaName

		out = append(out, sr)
	}

	ev.Resources = append(ev.Resources, out...)
	sort.Slice(ev.Resources, func(i, j int) bool {
		iName, jName := ev.Resources[i].MetaName, ev.Resources[j].MetaName
		iKind, jKind := ev.Resources[i].Kind, ev.Resources[j].Kind

		if iKind.is(jKind) {
			return iName < jName
		}
		return kindPriorities[iKind] > kindPriorities[jKind]
	})

	existing.Events = append(existing.Events, ev)
	return existing
}

type (
	// ExportOptFn is a functional input for setting the template fields.
	ExportOptFn func(opt *ExportOpt) error

	// ExportOpt are the options for creating a new package.
	ExportOpt struct {
		StackID   platform.ID
		OrgIDs    []ExportByOrgIDOpt
		Resources []ResourceToClone
	}

	// ExportByOrgIDOpt identifies an org to export resources for and provides
	// multiple filtering options.
	ExportByOrgIDOpt struct {
		OrgID         platform.ID
		LabelNames    []string
		ResourceKinds []Kind
	}
)

// ExportWithExistingResources allows the create method to clone existing resources.
func ExportWithExistingResources(resources ...ResourceToClone) ExportOptFn {
	return func(opt *ExportOpt) error {
		for _, r := range resources {
			if err := r.OK(); err != nil {
				return err
			}
		}
		opt.Resources = append(opt.Resources, resources...)
		return nil
	}
}

// ExportWithAllOrgResources allows the create method to clone all existing resources
// for the given organization.
func ExportWithAllOrgResources(orgIDOpt ExportByOrgIDOpt) ExportOptFn {
	return func(opt *ExportOpt) error {
		if orgIDOpt.OrgID == 0 {
			return errors.New("orgID provided must not be zero")
		}
		for _, k := range orgIDOpt.ResourceKinds {
			if err := k.OK(); err != nil {
				return err
			}
		}
		opt.OrgIDs = append(opt.OrgIDs, orgIDOpt)
		return nil
	}
}

// ExportWithStackID provides an export for the given stack ID.
func ExportWithStackID(stackID platform.ID) ExportOptFn {
	return func(opt *ExportOpt) error {
		opt.StackID = stackID
		return nil
	}
}

func exportOptFromOptFns(opts []ExportOptFn) (ExportOpt, error) {
	var opt ExportOpt
	for _, setter := range opts {
		if err := setter(&opt); err != nil {
			return ExportOpt{}, err
		}
	}
	return opt, nil
}

// Export will produce a templates from the parameters provided.
func (s *Service) Export(ctx context.Context, setters ...ExportOptFn) (*Template, error) {
	opt, err := exportOptFromOptFns(setters)
	if err != nil {
		return nil, err
	}

	var stack Stack
	if opt.StackID != 0 {
		stack, err = s.store.ReadStackByID(ctx, opt.StackID)
		if err != nil {
			return nil, err
		}

		var opts []ExportOptFn
		for _, r := range stack.LatestEvent().Resources {
			opts = append(opts, ExportWithExistingResources(ResourceToClone{
				Kind:     r.Kind,
				ID:       r.ID,
				MetaName: r.MetaName,
				Name:     r.Name,
			}))
		}

		opt, err = exportOptFromOptFns(append(setters, opts...))
		if err != nil {
			return nil, err
		}
	}

	exporter := newResourceExporter(s)

	for _, orgIDOpt := range opt.OrgIDs {
		resourcesToClone, err := s.cloneOrgResources(ctx, orgIDOpt.OrgID, orgIDOpt.ResourceKinds)
		if err != nil {
			return nil, internalErr(err)
		}

		if err := exporter.Export(ctx, resourcesToClone, orgIDOpt.LabelNames...); err != nil {
			return nil, internalErr(err)
		}
	}

	if err := exporter.Export(ctx, opt.Resources); err != nil {
		return nil, internalErr(err)
	}

	template := &Template{Objects: exporter.Objects()}
	if err := template.Validate(ValidWithoutResources()); err != nil {
		return nil, failedValidationErr(err)
	}

	return template, nil
}

func (s *Service) cloneOrgResources(ctx context.Context, orgID platform.ID, resourceKinds []Kind) ([]ResourceToClone, error) {
	var resources []ResourceToClone
	for _, resGen := range s.filterOrgResourceKinds(resourceKinds) {
		existingResources, err := resGen.cloneFn(ctx, orgID)
		if err != nil {
			return nil, ierrors.Wrap(err, "finding "+string(resGen.resType))
		}
		resources = append(resources, existingResources...)
	}

	return resources, nil
}

func (s *Service) cloneOrgBuckets(ctx context.Context, orgID platform.ID) ([]ResourceToClone, error) {
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
			Name: b.Name,
		})
	}
	return resources, nil
}

func (s *Service) cloneOrgChecks(ctx context.Context, orgID platform.ID) ([]ResourceToClone, error) {
	checks, _, err := s.checkSVC.FindChecks(ctx, influxdb.CheckFilter{
		OrgID: &orgID,
	})
	if err != nil {
		return nil, err
	}

	resources := make([]ResourceToClone, 0, len(checks))
	for _, c := range checks {
		resources = append(resources, ResourceToClone{
			Kind: KindCheck,
			ID:   c.GetID(),
			Name: c.GetName(),
		})
	}
	return resources, nil
}

func (s *Service) cloneOrgDashboards(ctx context.Context, orgID platform.ID) ([]ResourceToClone, error) {
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

func (s *Service) cloneOrgLabels(ctx context.Context, orgID platform.ID) ([]ResourceToClone, error) {
	filter := influxdb.LabelFilter{
		OrgID: &orgID,
	}

	labels, err := s.labelSVC.FindLabels(ctx, filter, influxdb.FindOptions{Limit: 100})
	if err != nil {
		return nil, ierrors.Wrap(err, "finding labels")
	}

	resources := make([]ResourceToClone, 0, len(labels))
	for _, l := range labels {
		resources = append(resources, ResourceToClone{
			Kind: KindLabel,
			ID:   l.ID,
			Name: l.Name,
		})
	}
	return resources, nil
}

func (s *Service) cloneOrgNotificationEndpoints(ctx context.Context, orgID platform.ID) ([]ResourceToClone, error) {
	endpoints, _, err := s.endpointSVC.FindNotificationEndpoints(ctx, influxdb.NotificationEndpointFilter{
		OrgID: &orgID,
	})
	if err != nil {
		return nil, err
	}

	resources := make([]ResourceToClone, 0, len(endpoints))
	for _, e := range endpoints {
		resources = append(resources, ResourceToClone{
			Kind: KindNotificationEndpoint,
			ID:   e.GetID(),
			Name: e.GetName(),
		})
	}
	return resources, nil
}

func (s *Service) cloneOrgNotificationRules(ctx context.Context, orgID platform.ID) ([]ResourceToClone, error) {
	rules, _, err := s.ruleSVC.FindNotificationRules(ctx, influxdb.NotificationRuleFilter{
		OrgID: &orgID,
	})
	if err != nil {
		return nil, err
	}

	resources := make([]ResourceToClone, 0, len(rules))
	for _, r := range rules {
		resources = append(resources, ResourceToClone{
			Kind: KindNotificationRule,
			ID:   r.GetID(),
			Name: r.GetName(),
		})
	}
	return resources, nil
}

func (s *Service) cloneOrgTasks(ctx context.Context, orgID platform.ID) ([]ResourceToClone, error) {
	tasks, err := s.getAllTasks(ctx, orgID)
	if err != nil {
		return nil, err
	}

	if len(tasks) == 0 {
		return nil, nil
	}

	checks, err := s.getAllChecks(ctx, orgID)
	if err != nil {
		return nil, err
	}

	rules, err := s.getNotificationRules(ctx, orgID)
	if err != nil {
		return nil, err
	}

	mTasks := make(map[platform.ID]*taskmodel.Task)
	for i := range tasks {
		t := tasks[i]
		if t.Type != taskmodel.TaskSystemType {
			continue
		}
		mTasks[t.ID] = t
	}
	for _, c := range checks {
		delete(mTasks, c.GetTaskID())
	}
	for _, r := range rules {
		delete(mTasks, r.GetTaskID())
	}

	resources := make([]ResourceToClone, 0, len(mTasks))
	for _, t := range mTasks {
		resources = append(resources, ResourceToClone{
			Kind: KindTask,
			ID:   t.ID,
		})
	}
	return resources, nil
}

func (s *Service) cloneOrgTelegrafs(ctx context.Context, orgID platform.ID) ([]ResourceToClone, error) {
	teles, _, err := s.teleSVC.FindTelegrafConfigs(ctx, influxdb.TelegrafConfigFilter{OrgID: &orgID})
	if err != nil {
		return nil, err
	}

	resources := make([]ResourceToClone, 0, len(teles))
	for _, t := range teles {
		resources = append(resources, ResourceToClone{
			Kind: KindTelegraf,
			ID:   t.ID,
		})
	}
	return resources, nil
}

func (s *Service) cloneOrgVariables(ctx context.Context, orgID platform.ID) ([]ResourceToClone, error) {
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

type (
	cloneResFn func(context.Context, platform.ID) ([]ResourceToClone, error)
	resClone   struct {
		resType influxdb.ResourceType
		cloneFn cloneResFn
	}
)

func (s *Service) filterOrgResourceKinds(resourceKindFilters []Kind) []resClone {
	mKinds := map[Kind]cloneResFn{
		KindBucket:               s.cloneOrgBuckets,
		KindCheck:                s.cloneOrgChecks,
		KindDashboard:            s.cloneOrgDashboards,
		KindLabel:                s.cloneOrgLabels,
		KindNotificationEndpoint: s.cloneOrgNotificationEndpoints,
		KindNotificationRule:     s.cloneOrgNotificationRules,
		KindTask:                 s.cloneOrgTasks,
		KindTelegraf:             s.cloneOrgTelegrafs,
		KindVariable:             s.cloneOrgVariables,
	}

	newResGen := func(resType influxdb.ResourceType, cloneFn cloneResFn) resClone {
		return resClone{
			resType: resType,
			cloneFn: cloneFn,
		}
	}

	var resourceTypeGens []resClone
	if len(resourceKindFilters) == 0 {
		for k, cloneFn := range mKinds {
			resourceTypeGens = append(resourceTypeGens, newResGen(k.ResourceType(), cloneFn))
		}
		return resourceTypeGens
	}

	seenKinds := make(map[Kind]bool)
	for _, k := range resourceKindFilters {
		cloneFn, ok := mKinds[k]
		if !ok || seenKinds[k] {
			continue
		}
		seenKinds[k] = true
		resourceTypeGens = append(resourceTypeGens, newResGen(k.ResourceType(), cloneFn))
	}

	return resourceTypeGens
}

// ImpactSummary represents the impact the application of a template will have on the system.
type ImpactSummary struct {
	Sources []string
	StackID platform.ID
	Diff    Diff
	Summary Summary
}

var reCommunityTemplatesValidAddr = regexp.MustCompile(`(?:https://raw\.githubusercontent\.com/influxdata/community-templates/master/)(?P<name>\w+)(?:/.*)`)

func (i *ImpactSummary) communityName() string {
	if len(i.Sources) == 0 {
		return "custom"
	}

	// pull name `name` from community url https://raw.githubusercontent.com/influxdata/community-templates/master/name/name_template.yml
	for j := range i.Sources {
		finds := reCommunityTemplatesValidAddr.FindStringSubmatch(i.Sources[j])
		if len(finds) == 2 {
			return finds[1]
		}
	}

	return "custom"
}

// DryRun provides a dry run of the template application. The template will be marked verified
// for later calls to Apply. This func will be run on an Apply if it has not been run
// already.
func (s *Service) DryRun(ctx context.Context, orgID, userID platform.ID, opts ...ApplyOptFn) (ImpactSummary, error) {
	opt := applyOptFromOptFns(opts...)
	template, err := s.templateFromApplyOpts(ctx, opt)
	if err != nil {
		return ImpactSummary{}, err
	}

	state, err := s.dryRun(ctx, orgID, template, opt)
	if err != nil {
		return ImpactSummary{}, err
	}

	return ImpactSummary{
		Sources: template.sources,
		StackID: opt.StackID,
		Diff:    state.diff(),
		Summary: newSummaryFromStateTemplate(state, template),
	}, nil
}

func (s *Service) dryRun(ctx context.Context, orgID platform.ID, template *Template, opt ApplyOpt) (*stateCoordinator, error) {
	// so here's the deal, when we have issues with the parsing validation, we
	// continue to do the diff anyhow. any resource that does not have a name
	// will be skipped, and won't bleed into the dry run here. We can now return
	// a error (parseErr) and valid diff/summary.
	var parseErr error
	err := template.Validate(ValidWithoutResources())
	if err != nil && !IsParseErr(err) {
		return nil, internalErr(err)
	}
	parseErr = err

	if len(opt.EnvRefs) > 0 {
		err := template.applyEnvRefs(opt.EnvRefs)
		if err != nil && !IsParseErr(err) {
			return nil, internalErr(err)
		}
		parseErr = err
	}

	state := newStateCoordinator(template, resourceActions{
		skipKinds:     opt.KindsToSkip,
		skipResources: opt.ResourcesToSkip,
	})

	if opt.StackID > 0 {
		if err := s.addStackState(ctx, opt.StackID, state); err != nil {
			return nil, internalErr(err)
		}
	}

	if err := s.dryRunSecrets(ctx, orgID, template); err != nil {
		return nil, err
	}

	s.dryRunBuckets(ctx, orgID, state.mBuckets)
	s.dryRunChecks(ctx, orgID, state.mChecks)
	s.dryRunDashboards(ctx, orgID, state.mDashboards)
	s.dryRunLabels(ctx, orgID, state.mLabels)
	s.dryRunTasks(ctx, orgID, state.mTasks)
	s.dryRunTelegrafConfigs(ctx, orgID, state.mTelegrafs)
	s.dryRunVariables(ctx, orgID, state.mVariables)

	err = s.dryRunNotificationEndpoints(ctx, orgID, state.mEndpoints)
	if err != nil {
		return nil, ierrors.Wrap(err, "failed to dry run notification endpoints")
	}

	err = s.dryRunNotificationRules(ctx, orgID, state.mRules, state.mEndpoints)
	if err != nil {
		return nil, err
	}

	stateLabelMappings, err := s.dryRunLabelMappings(ctx, state)
	if err != nil {
		return nil, err
	}
	state.labelMappings = stateLabelMappings

	return state, parseErr
}

func (s *Service) dryRunBuckets(ctx context.Context, orgID platform.ID, bkts map[string]*stateBucket) {
	for _, stateBkt := range bkts {
		stateBkt.orgID = orgID
		var existing *influxdb.Bucket
		if stateBkt.ID() != 0 {
			existing, _ = s.bucketSVC.FindBucketByID(ctx, stateBkt.ID())
		} else {
			existing, _ = s.bucketSVC.FindBucketByName(ctx, orgID, stateBkt.parserBkt.Name())
		}
		if IsNew(stateBkt.stateStatus) && existing != nil {
			stateBkt.stateStatus = StateStatusExists
		}
		stateBkt.existing = existing
	}
}

func (s *Service) dryRunChecks(ctx context.Context, orgID platform.ID, checks map[string]*stateCheck) {
	for _, c := range checks {
		c.orgID = orgID

		var existing influxdb.Check
		if c.ID() != 0 {
			existing, _ = s.checkSVC.FindCheckByID(ctx, c.ID())
		} else {
			name := c.parserCheck.Name()
			existing, _ = s.checkSVC.FindCheck(ctx, influxdb.CheckFilter{
				Name:  &name,
				OrgID: &orgID,
			})
		}
		if IsNew(c.stateStatus) && existing != nil {
			c.stateStatus = StateStatusExists
		}
		c.existing = existing
	}
}

func (s *Service) dryRunDashboards(ctx context.Context, orgID platform.ID, dashs map[string]*stateDashboard) {
	for _, stateDash := range dashs {
		stateDash.orgID = orgID
		var existing *influxdb.Dashboard
		if stateDash.ID() != 0 {
			existing, _ = s.dashSVC.FindDashboardByID(ctx, stateDash.ID())
		}
		if IsNew(stateDash.stateStatus) && existing != nil {
			stateDash.stateStatus = StateStatusExists
		}
		stateDash.existing = existing
	}
}

func (s *Service) dryRunLabels(ctx context.Context, orgID platform.ID, labels map[string]*stateLabel) {
	for _, l := range labels {
		l.orgID = orgID
		existingLabel, _ := s.findLabel(ctx, orgID, l)
		if IsNew(l.stateStatus) && existingLabel != nil {
			l.stateStatus = StateStatusExists
		}
		l.existing = existingLabel
	}
}

func (s *Service) dryRunNotificationEndpoints(ctx context.Context, orgID platform.ID, endpoints map[string]*stateEndpoint) error {
	existingEndpoints, _, err := s.endpointSVC.FindNotificationEndpoints(ctx, influxdb.NotificationEndpointFilter{
		OrgID: &orgID,
	}) // grab em all
	if err != nil {
		return internalErr(err)
	}

	mExistingByName := make(map[string]influxdb.NotificationEndpoint)
	mExistingByID := make(map[platform.ID]influxdb.NotificationEndpoint)
	for i := range existingEndpoints {
		e := existingEndpoints[i]
		mExistingByName[e.GetName()] = e
		mExistingByID[e.GetID()] = e
	}

	findEndpoint := func(e *stateEndpoint) influxdb.NotificationEndpoint {
		if iExisting, ok := mExistingByID[e.ID()]; ok {
			return iExisting
		}
		if iExisting, ok := mExistingByName[e.parserEndpoint.Name()]; ok {
			return iExisting
		}
		return nil
	}

	for _, newEndpoint := range endpoints {
		existing := findEndpoint(newEndpoint)
		if IsNew(newEndpoint.stateStatus) && existing != nil {
			newEndpoint.stateStatus = StateStatusExists
		}
		newEndpoint.existing = existing
	}

	return nil
}

func (s *Service) dryRunNotificationRules(ctx context.Context, orgID platform.ID, rules map[string]*stateRule, endpoints map[string]*stateEndpoint) error {
	for _, rule := range rules {
		rule.orgID = orgID
		var existing influxdb.NotificationRule
		if rule.ID() != 0 {
			existing, _ = s.ruleSVC.FindNotificationRuleByID(ctx, rule.ID())
		}
		rule.existing = existing
	}

	for _, r := range rules {
		if r.associatedEndpoint != nil {
			continue
		}

		e, ok := endpoints[r.parserRule.endpointMetaName()]
		if !IsRemoval(r.stateStatus) && !ok {
			err := fmt.Errorf("failed to find notification endpoint %q dependency for notification rule %q", r.parserRule.endpointName, r.parserRule.MetaName())
			return &errors2.Error{
				Code: errors2.EUnprocessableEntity,
				Err:  err,
			}
		}
		r.associatedEndpoint = e
	}

	return nil
}

func (s *Service) dryRunSecrets(ctx context.Context, orgID platform.ID, template *Template) error {
	templateSecrets := template.mSecrets
	if len(templateSecrets) == 0 {
		return nil
	}

	existingSecrets, err := s.secretSVC.GetSecretKeys(ctx, orgID)
	if err != nil {
		return &errors2.Error{Code: errors2.EInternal, Err: err}
	}

	for _, secret := range existingSecrets {
		templateSecrets[secret] = true // marked true since it exists in the platform
	}

	return nil
}

func (s *Service) dryRunTasks(ctx context.Context, orgID platform.ID, tasks map[string]*stateTask) {
	for _, stateTask := range tasks {
		stateTask.orgID = orgID
		var existing *taskmodel.Task
		if stateTask.ID() != 0 {
			existing, _ = s.taskSVC.FindTaskByID(ctx, stateTask.ID())
		}
		if IsNew(stateTask.stateStatus) && existing != nil {
			stateTask.stateStatus = StateStatusExists
		}
		stateTask.existing = existing
	}
}

func (s *Service) dryRunTelegrafConfigs(ctx context.Context, orgID platform.ID, teleConfigs map[string]*stateTelegraf) {
	for _, stateTele := range teleConfigs {
		stateTele.orgID = orgID
		var existing *influxdb.TelegrafConfig
		if stateTele.ID() != 0 {
			existing, _ = s.teleSVC.FindTelegrafConfigByID(ctx, stateTele.ID())
		}
		if IsNew(stateTele.stateStatus) && existing != nil {
			stateTele.stateStatus = StateStatusExists
		}
		stateTele.existing = existing
	}
}

func (s *Service) dryRunVariables(ctx context.Context, orgID platform.ID, vars map[string]*stateVariable) {
	existingVars, _ := s.getAllPlatformVariables(ctx, orgID)

	mIDs := make(map[platform.ID]*influxdb.Variable)
	mNames := make(map[string]*influxdb.Variable)
	for _, v := range existingVars {
		mIDs[v.ID] = v
		mNames[v.Name] = v
	}

	for _, v := range vars {
		existing := mNames[v.parserVar.Name()]
		if v.ID() != 0 {
			existing = mIDs[v.ID()]
		}
		if IsNew(v.stateStatus) && existing != nil {
			v.stateStatus = StateStatusExists
		}
		v.existing = existing
	}
}

func (s *Service) dryRunLabelMappings(ctx context.Context, state *stateCoordinator) ([]stateLabelMapping, error) {
	stateLabelsByResName := make(map[string]*stateLabel)
	for _, l := range state.mLabels {
		if IsRemoval(l.stateStatus) {
			continue
		}
		stateLabelsByResName[l.parserLabel.Name()] = l
	}

	var mappings []stateLabelMapping
	for _, b := range state.mBuckets {
		if IsRemoval(b.stateStatus) {
			continue
		}
		mm, err := s.dryRunResourceLabelMapping(ctx, state, stateLabelsByResName, b)
		if err != nil {
			return nil, err
		}
		mappings = append(mappings, mm...)
	}

	for _, c := range state.mChecks {
		if IsRemoval(c.stateStatus) {
			continue
		}
		mm, err := s.dryRunResourceLabelMapping(ctx, state, stateLabelsByResName, c)
		if err != nil {
			return nil, err
		}
		mappings = append(mappings, mm...)
	}

	for _, d := range state.mDashboards {
		if IsRemoval(d.stateStatus) {
			continue
		}
		mm, err := s.dryRunResourceLabelMapping(ctx, state, stateLabelsByResName, d)
		if err != nil {
			return nil, err
		}
		mappings = append(mappings, mm...)
	}

	for _, e := range state.mEndpoints {
		if IsRemoval(e.stateStatus) {
			continue
		}
		mm, err := s.dryRunResourceLabelMapping(ctx, state, stateLabelsByResName, e)
		if err != nil {
			return nil, err
		}
		mappings = append(mappings, mm...)
	}

	for _, r := range state.mRules {
		if IsRemoval(r.stateStatus) {
			continue
		}
		mm, err := s.dryRunResourceLabelMapping(ctx, state, stateLabelsByResName, r)
		if err != nil {
			return nil, err
		}
		mappings = append(mappings, mm...)
	}

	for _, t := range state.mTasks {
		if IsRemoval(t.stateStatus) {
			continue
		}
		mm, err := s.dryRunResourceLabelMapping(ctx, state, stateLabelsByResName, t)
		if err != nil {
			return nil, err
		}
		mappings = append(mappings, mm...)
	}

	for _, t := range state.mTelegrafs {
		if IsRemoval(t.stateStatus) {
			continue
		}
		mm, err := s.dryRunResourceLabelMapping(ctx, state, stateLabelsByResName, t)
		if err != nil {
			return nil, err
		}
		mappings = append(mappings, mm...)
	}

	for _, v := range state.mVariables {
		if IsRemoval(v.stateStatus) {
			continue
		}
		mm, err := s.dryRunResourceLabelMapping(ctx, state, stateLabelsByResName, v)
		if err != nil {
			return nil, err
		}
		mappings = append(mappings, mm...)
	}

	return mappings, nil
}

func (s *Service) dryRunResourceLabelMapping(ctx context.Context, state *stateCoordinator, stateLabelsByResName map[string]*stateLabel, associatedResource interface {
	labels() []*stateLabel
	stateIdentity() stateIdentity
}) ([]stateLabelMapping, error) {

	ident := associatedResource.stateIdentity()
	templateResourceLabels := associatedResource.labels()

	var mappings []stateLabelMapping
	if !ident.exists() {
		for _, l := range templateResourceLabels {
			mappings = append(mappings, stateLabelMapping{
				status:   StateStatusNew,
				resource: associatedResource,
				label:    l,
			})
		}
		return mappings, nil
	}

	existingLabels, err := s.labelSVC.FindResourceLabels(ctx, influxdb.LabelMappingFilter{
		ResourceID:   ident.id,
		ResourceType: ident.resourceType,
	})
	if err != nil && errors2.ErrorCode(err) != errors2.ENotFound {
		msgFmt := fmt.Sprintf("failed to find labels mappings for %s resource[%q]", ident.resourceType, ident.id)
		return nil, ierrors.Wrap(err, msgFmt)
	}

	templateLabels := labelSlcToMap(templateResourceLabels)
	for _, l := range existingLabels {
		// if label is found in state then we track the mapping and mark it existing
		// otherwise we continue on
		delete(templateLabels, l.Name)
		if sLabel, ok := stateLabelsByResName[l.Name]; ok {
			mappings = append(mappings, stateLabelMapping{
				status:   StateStatusExists,
				resource: associatedResource,
				label:    sLabel,
			})
		}
	}

	// now we add labels that do not exist
	for _, l := range templateLabels {
		stLabel, found := state.getLabelByMetaName(l.MetaName())
		if !found {
			continue
		}
		mappings = append(mappings, stateLabelMapping{
			status:   StateStatusNew,
			resource: associatedResource,
			label:    stLabel,
		})
	}

	return mappings, nil
}

func (s *Service) addStackState(ctx context.Context, stackID platform.ID, state *stateCoordinator) error {
	stack, err := s.store.ReadStackByID(ctx, stackID)
	if err != nil {
		return ierrors.Wrap(err, "reading stack")
	}

	state.addStackState(stack)
	return nil
}

type (
	// ApplyOpt is an option for applying a package.
	ApplyOpt struct {
		Templates       []*Template
		EnvRefs         map[string]interface{}
		MissingSecrets  map[string]string
		StackID         platform.ID
		ResourcesToSkip map[ActionSkipResource]bool
		KindsToSkip     map[Kind]bool
	}

	// ActionSkipResource provides an action from the consumer to use the template with
	// modifications to the resource kind and template name that will be applied.
	ActionSkipResource struct {
		Kind     Kind   `json:"kind"`
		MetaName string `json:"resourceTemplateName"`
	}

	// ActionSkipKind provides an action from the consumer to use the template with
	// modifications to the resource kinds will be applied.
	ActionSkipKind struct {
		Kind Kind `json:"kind"`
	}

	// ApplyOptFn updates the ApplyOpt per the functional option.
	ApplyOptFn func(opt *ApplyOpt)
)

// ApplyWithEnvRefs provides env refs to saturate the missing reference fields in the template.
func ApplyWithEnvRefs(envRefs map[string]interface{}) ApplyOptFn {
	return func(o *ApplyOpt) {
		o.EnvRefs = envRefs
	}
}

// ApplyWithTemplate provides a template to the application/dry run.
func ApplyWithTemplate(template *Template) ApplyOptFn {
	return func(opt *ApplyOpt) {
		opt.Templates = append(opt.Templates, template)
	}
}

// ApplyWithResourceSkip provides an action skip a resource in the application of a template.
func ApplyWithResourceSkip(action ActionSkipResource) ApplyOptFn {
	return func(opt *ApplyOpt) {
		if opt.ResourcesToSkip == nil {
			opt.ResourcesToSkip = make(map[ActionSkipResource]bool)
		}
		switch action.Kind {
		case KindCheckDeadman, KindCheckThreshold:
			action.Kind = KindCheck
		case KindNotificationEndpointHTTP,
			KindNotificationEndpointPagerDuty,
			KindNotificationEndpointSlack:
			action.Kind = KindNotificationEndpoint
		}
		opt.ResourcesToSkip[action] = true
	}
}

// ApplyWithKindSkip provides an action skip a kidn in the application of a template.
func ApplyWithKindSkip(action ActionSkipKind) ApplyOptFn {
	return func(opt *ApplyOpt) {
		if opt.KindsToSkip == nil {
			opt.KindsToSkip = make(map[Kind]bool)
		}
		switch action.Kind {
		case KindCheckDeadman, KindCheckThreshold:
			action.Kind = KindCheck
		case KindNotificationEndpointHTTP,
			KindNotificationEndpointPagerDuty,
			KindNotificationEndpointSlack:
			action.Kind = KindNotificationEndpoint
		}
		opt.KindsToSkip[action.Kind] = true
	}
}

// ApplyWithSecrets provides secrets to the platform that the template will need.
func ApplyWithSecrets(secrets map[string]string) ApplyOptFn {
	return func(o *ApplyOpt) {
		o.MissingSecrets = secrets
	}
}

// ApplyWithStackID associates the application of a template with a stack.
func ApplyWithStackID(stackID platform.ID) ApplyOptFn {
	return func(o *ApplyOpt) {
		o.StackID = stackID
	}
}

func applyOptFromOptFns(opts ...ApplyOptFn) ApplyOpt {
	var opt ApplyOpt
	for _, o := range opts {
		o(&opt)
	}
	return opt
}

// Apply will apply all the resources identified in the provided template. The entire template will be applied
// in its entirety. If a failure happens midway then the entire template will be rolled back to the state
// from before the template were applied.
func (s *Service) Apply(ctx context.Context, orgID, userID platform.ID, opts ...ApplyOptFn) (impact ImpactSummary, e error) {
	opt := applyOptFromOptFns(opts...)

	template, err := s.templateFromApplyOpts(ctx, opt)
	if err != nil {
		return ImpactSummary{}, err
	}

	if err := template.Validate(ValidWithoutResources()); err != nil {
		return ImpactSummary{}, failedValidationErr(err)
	}

	if err := template.applyEnvRefs(opt.EnvRefs); err != nil {
		return ImpactSummary{}, failedValidationErr(err)
	}

	state, err := s.dryRun(ctx, orgID, template, opt)
	if err != nil {
		return ImpactSummary{}, err
	}

	stackID := opt.StackID
	// if stackID is not provided, a stack will be provided for the application.
	if stackID == 0 {
		newStack, err := s.InitStack(ctx, userID, StackCreate{OrgID: orgID})
		if err != nil {
			return ImpactSummary{}, err
		}
		stackID = newStack.ID
	}

	defer func(stackID platform.ID) {
		updateStackFn := s.updateStackAfterSuccess
		if e != nil {
			updateStackFn = s.updateStackAfterRollback
			if opt.StackID == 0 {
				if err := s.store.DeleteStack(ctx, stackID); err != nil {
					s.log.Error("failed to delete created stack", zap.Error(err))
				}
			}
		}

		err := updateStackFn(ctx, stackID, state, template.Sources())
		if err != nil {
			s.log.Error("failed to update stack", zap.Error(err))
		}
	}(stackID)

	coordinator := newRollbackCoordinator(s.log, s.applyReqLimit)
	defer coordinator.rollback(s.log, &e, orgID)

	err = s.applyState(ctx, coordinator, orgID, userID, state, opt.MissingSecrets)
	if err != nil {
		return ImpactSummary{}, err
	}

	template.applySecrets(opt.MissingSecrets)

	return ImpactSummary{
		Sources: template.sources,
		StackID: stackID,
		Diff:    state.diff(),
		Summary: newSummaryFromStateTemplate(state, template),
	}, nil
}

func (s *Service) applyState(ctx context.Context, coordinator *rollbackCoordinator, orgID, userID platform.ID, state *stateCoordinator, missingSecrets map[string]string) (e error) {
	endpointApp, ruleApp, err := s.applyNotificationGenerator(ctx, userID, state.rules(), state.endpoints())
	if err != nil {
		return ierrors.Wrap(err, "failed to setup notification generator")
	}

	// each grouping here runs for its entirety, then returns an error that
	// is indicative of running all appliers provided. For instance, the labels
	// may have 1 variable fail and one of the buckets fails. The errors aggregate so
	// the caller will be informed of both the failed label variable the failed bucket.
	// the groupings here allow for steps to occur before exiting. The first step is
	// adding the dependencies, resources that are associated by other resources. Then the
	// primary resources. Here we get all the errors associated with them.
	// If those are all good, then we run the secondary(dependent) resources which
	// rely on the primary resources having been created.
	appliers := [][]applier{
		{
			// adds secrets that are referenced it the template, this allows user to
			// provide data that does not rest in the template.
			s.applySecrets(missingSecrets),
		},
		{
			// deps for primary resources
			s.applyLabels(ctx, state.labels()),
		},
		{
			// primary resources, can have relationships to labels
			s.applyVariables(ctx, state.variables()),
			s.applyBuckets(ctx, state.buckets()),
			s.applyChecks(ctx, state.checks()),
			s.applyDashboards(ctx, state.dashboards()),
			endpointApp,
			s.applyTasks(ctx, state.tasks()),
			s.applyTelegrafs(ctx, userID, state.telegrafConfigs()),
		},
	}

	for _, group := range appliers {
		if err := coordinator.runTilEnd(ctx, orgID, userID, group...); err != nil {
			return internalErr(err)
		}
	}

	// this has to be run after the above primary resources, because it relies on
	// notification endpoints already being applied.
	if err := coordinator.runTilEnd(ctx, orgID, userID, ruleApp); err != nil {
		return err
	}

	// secondary resources
	// this last grouping relies on the above 2 steps having completely successfully
	secondary := []applier{
		s.applyLabelMappings(ctx, state.labelMappings),
		s.removeLabelMappings(ctx, state.labelMappingsToRemove),
	}
	if err := coordinator.runTilEnd(ctx, orgID, userID, secondary...); err != nil {
		return internalErr(err)
	}

	return nil
}

func (s *Service) applyBuckets(ctx context.Context, buckets []*stateBucket) applier {
	const resource = "bucket"

	mutex := new(doMutex)
	rollbackBuckets := make([]*stateBucket, 0, len(buckets))

	createFn := func(ctx context.Context, i int, orgID, userID platform.ID) *applyErrBody {
		var b *stateBucket
		mutex.Do(func() {
			buckets[i].orgID = orgID
			b = buckets[i]
		})
		if !b.shouldApply() {
			return nil
		}

		influxBucket, err := s.applyBucket(ctx, b)
		if err != nil {
			return &applyErrBody{
				name: b.parserBkt.MetaName(),
				msg:  err.Error(),
			}
		}

		mutex.Do(func() {
			buckets[i].id = influxBucket.ID
			rollbackBuckets = append(rollbackBuckets, buckets[i])
		})

		return nil
	}

	return applier{
		creater: creater{
			entries: len(buckets),
			fn:      createFn,
		},
		rollbacker: rollbacker{
			resource: resource,
			fn:       func(_ platform.ID) error { return s.rollbackBuckets(ctx, rollbackBuckets) },
		},
	}
}

func (s *Service) rollbackBuckets(ctx context.Context, buckets []*stateBucket) error {
	rollbackFn := func(b *stateBucket) error {
		if !IsNew(b.stateStatus) && b.existing == nil || isSystemBucket(b.existing) {
			return nil
		}

		var err error
		switch {
		case IsRemoval(b.stateStatus):
			err = ierrors.Wrap(s.bucketSVC.CreateBucket(ctx, b.existing), "rolling back removed bucket")
		case IsExisting(b.stateStatus):
			_, err = s.bucketSVC.UpdateBucket(ctx, b.ID(), influxdb.BucketUpdate{
				Description:     &b.existing.Description,
				RetentionPeriod: &b.existing.RetentionPeriod,
			})
			err = ierrors.Wrap(err, "rolling back existing bucket to previous state")
		default:
			err = ierrors.Wrap(s.bucketSVC.DeleteBucket(ctx, b.ID()), "rolling back new bucket")
		}
		return err
	}

	var errs []string
	for _, b := range buckets {
		if err := rollbackFn(b); err != nil {
			errs = append(errs, fmt.Sprintf("error for bucket[%q]: %s", b.ID(), err))
		}
	}

	if len(errs) > 0 {
		// TODO: fixup error
		return errors.New(strings.Join(errs, ", "))
	}

	return nil
}

func (s *Service) applyBucket(ctx context.Context, b *stateBucket) (influxdb.Bucket, error) {
	if isSystemBucket(b.existing) {
		return *b.existing, nil
	}
	switch {
	case IsRemoval(b.stateStatus):
		if err := s.bucketSVC.DeleteBucket(ctx, b.ID()); err != nil {
			if errors2.ErrorCode(err) == errors2.ENotFound {
				return influxdb.Bucket{}, nil
			}
			return influxdb.Bucket{}, applyFailErr("delete", b.stateIdentity(), err)
		}
		return *b.existing, nil
	case IsExisting(b.stateStatus) && b.existing != nil:
		rp := b.parserBkt.RetentionRules.RP()
		newName := b.parserBkt.Name()
		influxBucket, err := s.bucketSVC.UpdateBucket(ctx, b.ID(), influxdb.BucketUpdate{
			Description:     &b.parserBkt.Description,
			Name:            &newName,
			RetentionPeriod: &rp,
		})
		if err != nil {
			return influxdb.Bucket{}, applyFailErr("update", b.stateIdentity(), err)
		}
		return *influxBucket, nil
	default:
		rp := b.parserBkt.RetentionRules.RP()
		influxBucket := influxdb.Bucket{
			OrgID:           b.orgID,
			Description:     b.parserBkt.Description,
			Name:            b.parserBkt.Name(),
			RetentionPeriod: rp,
		}
		err := s.bucketSVC.CreateBucket(ctx, &influxBucket)
		if err != nil {
			return influxdb.Bucket{}, applyFailErr("create", b.stateIdentity(), err)
		}
		return influxBucket, nil
	}
}

func (s *Service) applyChecks(ctx context.Context, checks []*stateCheck) applier {
	const resource = "check"

	mutex := new(doMutex)
	rollbackChecks := make([]*stateCheck, 0, len(checks))

	createFn := func(ctx context.Context, i int, orgID, userID platform.ID) *applyErrBody {
		var c *stateCheck
		mutex.Do(func() {
			checks[i].orgID = orgID
			c = checks[i]
		})

		influxCheck, err := s.applyCheck(ctx, c, userID)
		if err != nil {
			return &applyErrBody{
				name: c.parserCheck.MetaName(),
				msg:  err.Error(),
			}
		}

		mutex.Do(func() {
			checks[i].id = influxCheck.GetID()
			rollbackChecks = append(rollbackChecks, checks[i])
		})

		return nil
	}

	return applier{
		creater: creater{
			entries: len(checks),
			fn:      createFn,
		},
		rollbacker: rollbacker{
			resource: resource,
			fn:       func(_ platform.ID) error { return s.rollbackChecks(ctx, rollbackChecks) },
		},
	}
}

func (s *Service) rollbackChecks(ctx context.Context, checks []*stateCheck) error {
	rollbackFn := func(c *stateCheck) error {
		var err error
		switch {
		case IsRemoval(c.stateStatus):
			err = s.checkSVC.CreateCheck(
				ctx,
				influxdb.CheckCreate{
					Check:  c.existing,
					Status: c.parserCheck.Status(),
				},
				c.existing.GetOwnerID(),
			)
			c.id = c.existing.GetID()
		case IsExisting(c.stateStatus):
			if c.existing == nil {
				return nil
			}
			_, err = s.checkSVC.UpdateCheck(ctx, c.ID(), influxdb.CheckCreate{
				Check:  c.summarize().Check,
				Status: influxdb.Status(c.parserCheck.status),
			})
		default:
			err = s.checkSVC.DeleteCheck(ctx, c.ID())
		}
		return err
	}

	var errs []string
	for _, c := range checks {
		if err := rollbackFn(c); err != nil {
			errs = append(errs, fmt.Sprintf("error for check[%q]: %s", c.ID(), err))
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}

	return nil
}

func (s *Service) applyCheck(ctx context.Context, c *stateCheck, userID platform.ID) (influxdb.Check, error) {
	switch {
	case IsRemoval(c.stateStatus):
		if err := s.checkSVC.DeleteCheck(ctx, c.ID()); err != nil {
			if errors2.ErrorCode(err) == errors2.ENotFound {
				return &icheck.Threshold{Base: icheck.Base{ID: c.ID()}}, nil
			}
			return nil, applyFailErr("delete", c.stateIdentity(), err)
		}
		return c.existing, nil
	case IsExisting(c.stateStatus) && c.existing != nil:
		influxCheck, err := s.checkSVC.UpdateCheck(ctx, c.ID(), influxdb.CheckCreate{
			Check:  c.summarize().Check,
			Status: c.parserCheck.Status(),
		})
		if err != nil {
			return nil, applyFailErr("update", c.stateIdentity(), err)
		}
		return influxCheck, nil
	default:
		checkStub := influxdb.CheckCreate{
			Check:  c.summarize().Check,
			Status: c.parserCheck.Status(),
		}
		err := s.checkSVC.CreateCheck(ctx, checkStub, userID)
		if err != nil {
			return nil, applyFailErr("create", c.stateIdentity(), err)
		}
		return checkStub.Check, nil
	}
}

func (s *Service) applyDashboards(ctx context.Context, dashboards []*stateDashboard) applier {
	const resource = "dashboard"

	mutex := new(doMutex)
	rollbackDashboards := make([]*stateDashboard, 0, len(dashboards))

	createFn := func(ctx context.Context, i int, orgID, userID platform.ID) *applyErrBody {
		var d *stateDashboard
		mutex.Do(func() {
			dashboards[i].orgID = orgID
			d = dashboards[i]
		})

		influxBucket, err := s.applyDashboard(ctx, d)
		if err != nil {
			return &applyErrBody{
				name: d.parserDash.MetaName(),
				msg:  err.Error(),
			}
		}

		mutex.Do(func() {
			dashboards[i].id = influxBucket.ID
			rollbackDashboards = append(rollbackDashboards, dashboards[i])
		})
		return nil
	}

	return applier{
		creater: creater{
			entries: len(dashboards),
			fn:      createFn,
		},
		rollbacker: rollbacker{
			resource: resource,
			fn: func(_ platform.ID) error {
				return s.rollbackDashboards(ctx, rollbackDashboards)
			},
		},
	}
}

func (s *Service) applyDashboard(ctx context.Context, d *stateDashboard) (influxdb.Dashboard, error) {
	switch {
	case IsRemoval(d.stateStatus):
		if err := s.dashSVC.DeleteDashboard(ctx, d.ID()); err != nil {
			if errors2.ErrorCode(err) == errors2.ENotFound {
				return influxdb.Dashboard{}, nil
			}
			return influxdb.Dashboard{}, applyFailErr("delete", d.stateIdentity(), err)
		}
		return *d.existing, nil
	case IsExisting(d.stateStatus) && d.existing != nil:
		name := d.parserDash.Name()
		cells := convertChartsToCells(d.parserDash.Charts)
		dash, err := s.dashSVC.UpdateDashboard(ctx, d.ID(), influxdb.DashboardUpdate{
			Name:        &name,
			Description: &d.parserDash.Description,
			Cells:       &cells,
		})
		if err != nil {
			return influxdb.Dashboard{}, applyFailErr("update", d.stateIdentity(), err)
		}
		return *dash, nil
	default:
		cells := convertChartsToCells(d.parserDash.Charts)
		influxDashboard := influxdb.Dashboard{
			OrganizationID: d.orgID,
			Description:    d.parserDash.Description,
			Name:           d.parserDash.Name(),
			Cells:          cells,
		}
		err := s.dashSVC.CreateDashboard(ctx, &influxDashboard)
		if err != nil {
			return influxdb.Dashboard{}, applyFailErr("create", d.stateIdentity(), err)
		}
		return influxDashboard, nil
	}
}

func (s *Service) rollbackDashboards(ctx context.Context, dashs []*stateDashboard) error {
	rollbackFn := func(d *stateDashboard) error {
		if !IsNew(d.stateStatus) && d.existing == nil {
			return nil
		}

		var err error
		switch {
		case IsRemoval(d.stateStatus):
			err = ierrors.Wrap(s.dashSVC.CreateDashboard(ctx, d.existing), "rolling back removed dashboard")
		case IsExisting(d.stateStatus):
			_, err := s.dashSVC.UpdateDashboard(ctx, d.ID(), influxdb.DashboardUpdate{
				Name:        &d.existing.Name,
				Description: &d.existing.Description,
				Cells:       &d.existing.Cells,
			})
			return ierrors.Wrap(err, "failed to update dashboard")
		default:
			err = ierrors.Wrap(s.dashSVC.DeleteDashboard(ctx, d.ID()), "rolling back new dashboard")
		}
		return err
	}

	var errs []string
	for _, d := range dashs {
		if err := rollbackFn(d); err != nil {
			errs = append(errs, fmt.Sprintf("error for dashboard[%q]: %s", d.ID(), err))
		}
	}

	if len(errs) > 0 {
		// TODO: fixup error
		return errors.New(strings.Join(errs, ", "))
	}

	return nil
}

func convertChartsToCells(ch []*chart) []*influxdb.Cell {
	icells := make([]*influxdb.Cell, 0, len(ch))
	for _, c := range ch {
		icell := &influxdb.Cell{
			CellProperty: influxdb.CellProperty{
				X: int32(c.XPos),
				Y: int32(c.YPos),
				H: int32(c.Height),
				W: int32(c.Width),
			},
			View: &influxdb.View{
				ViewContents: influxdb.ViewContents{Name: c.Name},
				Properties:   c.properties(),
			},
		}
		icells = append(icells, icell)
	}
	return icells
}

func (s *Service) applyLabels(ctx context.Context, labels []*stateLabel) applier {
	const resource = "label"

	mutex := new(doMutex)
	rollBackLabels := make([]*stateLabel, 0, len(labels))

	createFn := func(ctx context.Context, i int, orgID, userID platform.ID) *applyErrBody {
		var l *stateLabel
		mutex.Do(func() {
			labels[i].orgID = orgID
			l = labels[i]
		})
		if !l.shouldApply() {
			return nil
		}

		influxLabel, err := s.applyLabel(ctx, l)
		if err != nil {
			return &applyErrBody{
				name: l.parserLabel.MetaName(),
				msg:  err.Error(),
			}
		}

		mutex.Do(func() {
			labels[i].id = influxLabel.ID
			rollBackLabels = append(rollBackLabels, labels[i])
		})

		return nil
	}

	return applier{
		creater: creater{
			entries: len(labels),
			fn:      createFn,
		},
		rollbacker: rollbacker{
			resource: resource,
			fn:       func(_ platform.ID) error { return s.rollbackLabels(ctx, rollBackLabels) },
		},
	}
}

func (s *Service) rollbackLabels(ctx context.Context, labels []*stateLabel) error {
	rollbackFn := func(l *stateLabel) error {
		if !IsNew(l.stateStatus) && l.existing == nil {
			return nil
		}

		var err error
		switch {
		case IsRemoval(l.stateStatus):
			err = s.labelSVC.CreateLabel(ctx, l.existing)
		case IsExisting(l.stateStatus):
			_, err = s.labelSVC.UpdateLabel(ctx, l.ID(), influxdb.LabelUpdate{
				Name:       l.parserLabel.Name(),
				Properties: l.existing.Properties,
			})
		default:
			err = s.labelSVC.DeleteLabel(ctx, l.ID())
		}
		return err
	}

	var errs []string
	for _, l := range labels {
		if err := rollbackFn(l); err != nil {
			errs = append(errs, fmt.Sprintf("error for label[%q]: %s", l.ID(), err))
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ", "))
	}

	return nil
}

func (s *Service) applyLabel(ctx context.Context, l *stateLabel) (influxdb.Label, error) {
	var (
		influxLabel *influxdb.Label
		err         error
	)
	switch {
	case IsRemoval(l.stateStatus):
		influxLabel, err = l.existing, s.labelSVC.DeleteLabel(ctx, l.ID())
	case IsExisting(l.stateStatus) && l.existing != nil:
		influxLabel, err = s.labelSVC.UpdateLabel(ctx, l.ID(), influxdb.LabelUpdate{
			Name:       l.parserLabel.Name(),
			Properties: l.properties(),
		})
		err = ierrors.Wrap(err, "updating")
	default:
		creatLabel := l.toInfluxLabel()
		influxLabel = &creatLabel
		err = ierrors.Wrap(s.labelSVC.CreateLabel(ctx, &creatLabel), "creating")
	}
	if errors2.ErrorCode(err) == errors2.ENotFound {
		return influxdb.Label{}, nil
	}
	if err != nil || influxLabel == nil {
		return influxdb.Label{}, err
	}

	return *influxLabel, nil
}

func (s *Service) applyNotificationEndpoints(ctx context.Context, userID platform.ID, endpoints []*stateEndpoint) (applier, func(platform.ID) error) {
	mutex := new(doMutex)
	rollbackEndpoints := make([]*stateEndpoint, 0, len(endpoints))

	createFn := func(ctx context.Context, i int, orgID, userID platform.ID) *applyErrBody {
		var endpoint *stateEndpoint
		mutex.Do(func() {
			endpoints[i].orgID = orgID
			endpoint = endpoints[i]
		})

		influxEndpoint, err := s.applyNotificationEndpoint(ctx, endpoint, userID)
		if err != nil {
			return &applyErrBody{
				name: endpoint.parserEndpoint.MetaName(),
				msg:  err.Error(),
			}
		}

		mutex.Do(func() {
			if influxEndpoint != nil {
				endpoints[i].id = influxEndpoint.GetID()
				for _, secret := range influxEndpoint.SecretFields() {
					switch {
					case strings.HasSuffix(secret.Key, "-routing-key"):
						if endpoints[i].parserEndpoint.routingKey == nil {
							endpoints[i].parserEndpoint.routingKey = new(references)
						}
						endpoints[i].parserEndpoint.routingKey.Secret = secret.Key
					case strings.HasSuffix(secret.Key, "-token"):
						if endpoints[i].parserEndpoint.token == nil {
							endpoints[i].parserEndpoint.token = new(references)
						}
						endpoints[i].parserEndpoint.token.Secret = secret.Key
					case strings.HasSuffix(secret.Key, "-username"):
						if endpoints[i].parserEndpoint.username == nil {
							endpoints[i].parserEndpoint.username = new(references)
						}
						endpoints[i].parserEndpoint.username.Secret = secret.Key
					case strings.HasSuffix(secret.Key, "-password"):
						if endpoints[i].parserEndpoint.password == nil {
							endpoints[i].parserEndpoint.password = new(references)
						}
						endpoints[i].parserEndpoint.password.Secret = secret.Key
					}
				}
			}
			rollbackEndpoints = append(rollbackEndpoints, endpoints[i])
		})

		return nil
	}

	rollbackFn := func(_ platform.ID) error {
		return s.rollbackNotificationEndpoints(ctx, userID, rollbackEndpoints)
	}

	return applier{
		creater: creater{
			entries: len(endpoints),
			fn:      createFn,
		},
		rollbacker: rollbacker{
			fn: func(_ platform.ID) error {
				return nil
			},
		},
	}, rollbackFn
}

func (s *Service) applyNotificationEndpoint(ctx context.Context, e *stateEndpoint, userID platform.ID) (influxdb.NotificationEndpoint, error) {
	switch {
	case IsRemoval(e.stateStatus):
		_, _, err := s.endpointSVC.DeleteNotificationEndpoint(ctx, e.ID())
		if err != nil && errors2.ErrorCode(err) != errors2.ENotFound {
			return nil, applyFailErr("delete", e.stateIdentity(), err)
		}
		return e.existing, nil
	case IsExisting(e.stateStatus) && e.existing != nil:
		// stub out userID since we're always using hte http client which will fill it in for us with the token
		// feels a bit broken that is required.
		// TODO: look into this userID requirement
		end, err := s.endpointSVC.UpdateNotificationEndpoint(
			ctx,
			e.ID(),
			e.summarize().NotificationEndpoint,
			userID,
		)
		return end, applyFailErr("update", e.stateIdentity(), err)
	default:
		actual := e.summarize().NotificationEndpoint
		err := s.endpointSVC.CreateNotificationEndpoint(ctx, actual, userID)
		if err != nil {
			return nil, applyFailErr("create", e.stateIdentity(), err)
		}
		return actual, nil
	}
}

func (s *Service) rollbackNotificationEndpoints(ctx context.Context, userID platform.ID, endpoints []*stateEndpoint) error {
	rollbackFn := func(e *stateEndpoint) error {
		if !IsNew(e.stateStatus) && e.existing == nil {
			return nil
		}
		var err error
		switch e.stateStatus {
		case StateStatusRemove:
			err = s.endpointSVC.CreateNotificationEndpoint(ctx, e.existing, userID)
			err = ierrors.Wrap(err, "failed to rollback removed endpoint")
		case StateStatusExists:
			_, err = s.endpointSVC.UpdateNotificationEndpoint(ctx, e.ID(), e.existing, userID)
			err = ierrors.Wrap(err, "failed to rollback updated endpoint")
		default:
			_, _, err = s.endpointSVC.DeleteNotificationEndpoint(ctx, e.ID())
			err = ierrors.Wrap(err, "failed to rollback created endpoint")
		}
		return err
	}

	var errs []string
	for _, e := range endpoints {
		if err := rollbackFn(e); err != nil {
			errs = append(errs, fmt.Sprintf("error for notification endpoint[%q]: %s", e.ID(), err))
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}

	return nil
}

func (s *Service) applyNotificationGenerator(ctx context.Context, userID platform.ID, rules []*stateRule, stateEndpoints []*stateEndpoint) (endpointApplier applier, ruleApplier applier, err error) {
	mEndpoints := make(map[string]*stateEndpoint)
	for _, e := range stateEndpoints {
		mEndpoints[e.parserEndpoint.MetaName()] = e
	}

	var errs applyErrs
	for _, r := range rules {
		if IsRemoval(r.stateStatus) {
			continue
		}
		v, ok := mEndpoints[r.endpointTemplateName()]
		if !ok {
			errs = append(errs, &applyErrBody{
				name: r.parserRule.MetaName(),
				msg:  fmt.Sprintf("notification rule endpoint dependency does not exist; endpointName=%q", r.parserRule.associatedEndpoint.MetaName()),
			})
			continue
		}
		r.associatedEndpoint = v
	}

	err = errs.toError("notification_rules", "failed to find dependency")
	if err != nil {
		return applier{}, applier{}, err
	}

	endpointApp, endpointRollbackFn := s.applyNotificationEndpoints(ctx, userID, stateEndpoints)
	ruleApp, ruleRollbackFn := s.applyNotificationRules(ctx, userID, rules)

	// here we have to couple the endpoints to rules b/c of the dependency here when rolling back
	// a deleted endpoint and rule. This forces the endpoints to be rolled back first so the
	// reference for the rule has settled. The dependency has to be available before rolling back
	// notification rules.
	endpointApp.rollbacker = rollbacker{
		fn: func(orgID platform.ID) error {
			if err := endpointRollbackFn(orgID); err != nil {
				s.log.Error("failed to roll back endpoints", zap.Error(err))
			}
			return ruleRollbackFn(orgID)
		},
	}

	return endpointApp, ruleApp, nil
}

func (s *Service) applyNotificationRules(ctx context.Context, userID platform.ID, rules []*stateRule) (applier, func(platform.ID) error) {
	mutex := new(doMutex)
	rollbackEndpoints := make([]*stateRule, 0, len(rules))

	createFn := func(ctx context.Context, i int, orgID, userID platform.ID) *applyErrBody {
		var rule *stateRule
		mutex.Do(func() {
			rules[i].orgID = orgID
			rule = rules[i]
		})

		influxRule, err := s.applyNotificationRule(ctx, rule, userID)
		if err != nil {
			return &applyErrBody{
				name: rule.parserRule.MetaName(),
				msg:  err.Error(),
			}
		}

		mutex.Do(func() {
			if influxRule != nil {
				rules[i].id = influxRule.GetID()
			}
			rollbackEndpoints = append(rollbackEndpoints, rules[i])
		})

		return nil
	}

	rollbackFn := func(_ platform.ID) error {
		return s.rollbackNotificationRules(ctx, userID, rollbackEndpoints)
	}

	return applier{
		creater: creater{
			entries: len(rules),
			fn:      createFn,
		},
		rollbacker: rollbacker{
			fn: func(_ platform.ID) error { return nil },
		},
	}, rollbackFn
}

func (s *Service) applyNotificationRule(ctx context.Context, r *stateRule, userID platform.ID) (influxdb.NotificationRule, error) {
	switch {
	case IsRemoval(r.stateStatus):
		if err := s.ruleSVC.DeleteNotificationRule(ctx, r.ID()); err != nil {
			if errors2.ErrorCode(err) == errors2.ENotFound {
				return nil, nil
			}
			return nil, applyFailErr("delete", r.stateIdentity(), err)
		}
		return r.existing, nil
	case IsExisting(r.stateStatus) && r.existing != nil:
		ruleCreate := influxdb.NotificationRuleCreate{
			NotificationRule: r.toInfluxRule(),
			Status:           r.parserRule.Status(),
		}
		influxRule, err := s.ruleSVC.UpdateNotificationRule(ctx, r.ID(), ruleCreate, userID)
		if err != nil {
			return nil, applyFailErr("update", r.stateIdentity(), err)
		}
		return influxRule, nil
	default:
		influxRule := influxdb.NotificationRuleCreate{
			NotificationRule: r.toInfluxRule(),
			Status:           r.parserRule.Status(),
		}
		err := s.ruleSVC.CreateNotificationRule(ctx, influxRule, userID)
		if err != nil {
			return nil, applyFailErr("create", r.stateIdentity(), err)
		}
		return influxRule.NotificationRule, nil
	}
}

func (s *Service) rollbackNotificationRules(ctx context.Context, userID platform.ID, rules []*stateRule) error {
	rollbackFn := func(r *stateRule) error {
		if !IsNew(r.stateStatus) && r.existing == nil {
			return nil
		}

		existingRuleFn := func(endpointID platform.ID) influxdb.NotificationRule {
			switch rr := r.existing.(type) {
			case *rule.HTTP:
				rr.EndpointID = endpointID
			case *rule.PagerDuty:
				rr.EndpointID = endpointID
			case *rule.Slack:
				rr.EndpointID = endpointID
			}
			return r.existing
		}

		// setting status to unknown b/c these resources for two reasons:
		//	1. we have no ability to find status via the Service, only to set it...
		//	2. we have no way of inspecting an existing rule and pulling status from it
		//	3. since this is a fallback condition, we set things to inactive as a user
		//		is likely to follow up this failure by fixing their template up then reapplying
		unknownStatus := influxdb.Inactive

		var err error
		switch r.stateStatus {
		case StateStatusRemove:
			if r.associatedEndpoint == nil {
				return internalErr(errors.New("failed to find endpoint dependency to rollback existing notification rule"))
			}
			influxRule := influxdb.NotificationRuleCreate{
				NotificationRule: existingRuleFn(r.endpointID()),
				Status:           unknownStatus,
			}
			err = s.ruleSVC.CreateNotificationRule(ctx, influxRule, userID)
			err = ierrors.Wrap(err, "failed to rollback created notification rule")
		case StateStatusExists:
			if r.associatedEndpoint == nil {
				return internalErr(errors.New("failed to find endpoint dependency to rollback existing notification rule"))
			}

			influxRule := influxdb.NotificationRuleCreate{
				NotificationRule: existingRuleFn(r.endpointID()),
				Status:           unknownStatus,
			}
			_, err = s.ruleSVC.UpdateNotificationRule(ctx, r.ID(), influxRule, r.existing.GetOwnerID())
			err = ierrors.Wrap(err, "failed to rollback updated notification rule")
		default:
			err = s.ruleSVC.DeleteNotificationRule(ctx, r.ID())
			err = ierrors.Wrap(err, "failed to rollback created notification rule")
		}
		return err
	}

	var errs []string
	for _, r := range rules {
		if err := rollbackFn(r); err != nil {
			errs = append(errs, fmt.Sprintf("error for notification rule[%q]: %s", r.ID(), err))
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (s *Service) applySecrets(secrets map[string]string) applier {
	const resource = "secrets"

	if len(secrets) == 0 {
		return applier{
			rollbacker: rollbacker{fn: func(orgID platform.ID) error { return nil }},
		}
	}

	mutex := new(doMutex)
	rollbackSecrets := make([]string, 0)

	createFn := func(ctx context.Context, i int, orgID, userID platform.ID) *applyErrBody {
		err := s.secretSVC.PutSecrets(ctx, orgID, secrets)
		if err != nil {
			return &applyErrBody{name: "secrets", msg: err.Error()}
		}

		mutex.Do(func() {
			for key := range secrets {
				rollbackSecrets = append(rollbackSecrets, key)
			}
		})

		return nil
	}

	return applier{
		creater: creater{
			entries: 1,
			fn:      createFn,
		},
		rollbacker: rollbacker{
			resource: resource,
			fn: func(orgID platform.ID) error {
				return s.secretSVC.DeleteSecret(context.Background(), orgID)
			},
		},
	}
}

func (s *Service) applyTasks(ctx context.Context, tasks []*stateTask) applier {
	const resource = "tasks"

	mutex := new(doMutex)
	rollbackTasks := make([]*stateTask, 0, len(tasks))

	createFn := func(ctx context.Context, i int, orgID, userID platform.ID) *applyErrBody {
		var t *stateTask
		mutex.Do(func() {
			tasks[i].orgID = orgID
			t = tasks[i]
		})

		newTask, err := s.applyTask(ctx, userID, t)
		if err != nil {
			return &applyErrBody{
				name: t.parserTask.MetaName(),
				msg:  err.Error(),
			}
		}

		mutex.Do(func() {
			tasks[i].id = newTask.ID
			rollbackTasks = append(rollbackTasks, tasks[i])
		})

		return nil
	}

	return applier{
		creater: creater{
			entries: len(tasks),
			fn:      createFn,
		},
		rollbacker: rollbacker{
			resource: resource,
			fn: func(_ platform.ID) error {
				return s.rollbackTasks(ctx, rollbackTasks)
			},
		},
	}
}

func (s *Service) applyTask(ctx context.Context, userID platform.ID, t *stateTask) (taskmodel.Task, error) {
	if isRestrictedTask(t.existing) {
		return *t.existing, nil
	}
	switch {
	case IsRemoval(t.stateStatus):
		if err := s.taskSVC.DeleteTask(ctx, t.ID()); err != nil {
			if errors2.ErrorCode(err) == errors2.ENotFound {
				return taskmodel.Task{}, nil
			}
			return taskmodel.Task{}, applyFailErr("delete", t.stateIdentity(), err)
		}
		return *t.existing, nil
	case IsExisting(t.stateStatus) && t.existing != nil:
		newFlux := t.parserTask.flux()
		newStatus := string(t.parserTask.Status())
		opt := options.Options{
			Name: t.parserTask.Name(),
			Cron: t.parserTask.cron,
		}
		if every := t.parserTask.every; every > 0 {
			opt.Every.Parse(every.String())
		}
		if offset := t.parserTask.offset; offset > 0 {
			var off options.Duration
			if err := off.Parse(offset.String()); err == nil {
				opt.Offset = &off
			}
		}

		updatedTask, err := s.taskSVC.UpdateTask(ctx, t.ID(), taskmodel.TaskUpdate{
			Flux:        &newFlux,
			Status:      &newStatus,
			Description: &t.parserTask.description,
			Options:     opt,
		})
		if err != nil {
			return taskmodel.Task{}, applyFailErr("update", t.stateIdentity(), err)
		}
		return *updatedTask, nil
	default:
		newTask, err := s.taskSVC.CreateTask(ctx, taskmodel.TaskCreate{
			Type:           taskmodel.TaskSystemType,
			Flux:           t.parserTask.flux(),
			OwnerID:        userID,
			Description:    t.parserTask.description,
			Status:         string(t.parserTask.Status()),
			OrganizationID: t.orgID,
		})
		if err != nil {
			return taskmodel.Task{}, applyFailErr("create", t.stateIdentity(), err)
		}
		return *newTask, nil
	}
}

func (s *Service) rollbackTasks(ctx context.Context, tasks []*stateTask) error {
	rollbackFn := func(t *stateTask) error {
		if !IsNew(t.stateStatus) && t.existing == nil || isRestrictedTask(t.existing) {
			return nil
		}

		var err error
		switch t.stateStatus {
		case StateStatusRemove:
			newTask, err := s.taskSVC.CreateTask(ctx, taskmodel.TaskCreate{
				Type:           t.existing.Type,
				Flux:           t.existing.Flux,
				OwnerID:        t.existing.OwnerID,
				Description:    t.existing.Description,
				Status:         t.existing.Status,
				OrganizationID: t.orgID,
				Metadata:       t.existing.Metadata,
			})
			if err != nil {
				return ierrors.Wrap(err, "failed to rollback removed task")
			}
			t.existing = newTask
		case StateStatusExists:
			opt := options.Options{
				Name: t.existing.Name,
				Cron: t.existing.Cron,
			}
			if every := t.existing.Every; every != "" {
				opt.Every.Parse(every)
			}
			if offset := t.existing.Offset; offset > 0 {
				var off options.Duration
				if err := off.Parse(offset.String()); err == nil {
					opt.Offset = &off
				}
			}

			_, err = s.taskSVC.UpdateTask(ctx, t.ID(), taskmodel.TaskUpdate{
				Flux:        &t.existing.Flux,
				Status:      &t.existing.Status,
				Description: &t.existing.Description,
				Metadata:    t.existing.Metadata,
				Options:     opt,
			})
			err = ierrors.Wrap(err, "failed to rollback updated task")
		default:
			err = s.taskSVC.DeleteTask(ctx, t.ID())
			err = ierrors.Wrap(err, "failed to rollback created task")
		}
		return err
	}

	var errs []string
	for _, d := range tasks {
		if err := rollbackFn(d); err != nil {
			errs = append(errs, fmt.Sprintf("error for task[%q]: %s", d.ID(), err))
		}
	}

	if len(errs) > 0 {
		// TODO: fixup error
		return errors.New(strings.Join(errs, ", "))
	}

	return nil
}

func (s *Service) applyTelegrafs(ctx context.Context, userID platform.ID, teles []*stateTelegraf) applier {
	const resource = "telegrafs"

	mutex := new(doMutex)
	rollbackTelegrafs := make([]*stateTelegraf, 0, len(teles))

	createFn := func(ctx context.Context, i int, orgID, userID platform.ID) *applyErrBody {
		var t *stateTelegraf
		mutex.Do(func() {
			teles[i].orgID = orgID
			t = teles[i]
		})

		existing, err := s.applyTelegrafConfig(ctx, userID, t)
		if err != nil {
			return &applyErrBody{
				name: t.parserTelegraf.MetaName(),
				msg:  err.Error(),
			}
		}

		mutex.Do(func() {
			teles[i].id = existing.ID
			rollbackTelegrafs = append(rollbackTelegrafs, teles[i])
		})

		return nil
	}

	return applier{
		creater: creater{
			entries: len(teles),
			fn:      createFn,
		},
		rollbacker: rollbacker{
			resource: resource,
			fn: func(_ platform.ID) error {
				return s.rollbackTelegrafConfigs(ctx, userID, rollbackTelegrafs)
			},
		},
	}
}

func (s *Service) applyTelegrafConfig(ctx context.Context, userID platform.ID, t *stateTelegraf) (influxdb.TelegrafConfig, error) {
	switch {
	case IsRemoval(t.stateStatus):
		if err := s.teleSVC.DeleteTelegrafConfig(ctx, t.ID()); err != nil {
			if errors2.ErrorCode(err) == errors2.ENotFound {
				return influxdb.TelegrafConfig{}, nil
			}
			return influxdb.TelegrafConfig{}, applyFailErr("delete", t.stateIdentity(), err)
		}
		return *t.existing, nil
	case IsExisting(t.stateStatus) && t.existing != nil:
		cfg := t.summarize().TelegrafConfig
		updatedConfig, err := s.teleSVC.UpdateTelegrafConfig(ctx, t.ID(), &cfg, userID)
		if err != nil {
			return influxdb.TelegrafConfig{}, applyFailErr("update", t.stateIdentity(), err)
		}
		return *updatedConfig, nil
	default:
		cfg := t.summarize().TelegrafConfig
		err := s.teleSVC.CreateTelegrafConfig(ctx, &cfg, userID)
		if err != nil {
			return influxdb.TelegrafConfig{}, applyFailErr("create", t.stateIdentity(), err)
		}
		return cfg, nil
	}
}

func (s *Service) rollbackTelegrafConfigs(ctx context.Context, userID platform.ID, cfgs []*stateTelegraf) error {
	rollbackFn := func(t *stateTelegraf) error {
		if !IsNew(t.stateStatus) && t.existing == nil {
			return nil
		}

		var err error
		switch t.stateStatus {
		case StateStatusRemove:
			err = ierrors.Wrap(s.teleSVC.CreateTelegrafConfig(ctx, t.existing, userID), "rolling back removed telegraf config")
		case StateStatusExists:
			_, err = s.teleSVC.UpdateTelegrafConfig(ctx, t.ID(), t.existing, userID)
			err = ierrors.Wrap(err, "rolling back updated telegraf config")
		default:
			err = ierrors.Wrap(s.teleSVC.DeleteTelegrafConfig(ctx, t.ID()), "rolling back created telegraf config")
		}
		return err
	}

	var errs []string
	for _, v := range cfgs {
		if err := rollbackFn(v); err != nil {
			errs = append(errs, fmt.Sprintf("error for variable[%q]: %s", v.ID(), err))
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}

	return nil
}

func (s *Service) applyVariables(ctx context.Context, vars []*stateVariable) applier {
	const resource = "variable"

	mutex := new(doMutex)
	rollBackVars := make([]*stateVariable, 0, len(vars))

	createFn := func(ctx context.Context, i int, orgID, userID platform.ID) *applyErrBody {
		var v *stateVariable
		mutex.Do(func() {
			vars[i].orgID = orgID
			v = vars[i]
		})
		if !v.shouldApply() {
			return nil
		}
		influxVar, err := s.applyVariable(ctx, v)
		if err != nil {
			return &applyErrBody{
				name: v.parserVar.MetaName(),
				msg:  err.Error(),
			}
		}

		mutex.Do(func() {
			vars[i].id = influxVar.ID
			rollBackVars = append(rollBackVars, vars[i])
		})
		return nil
	}

	return applier{
		creater: creater{
			entries: len(vars),
			fn:      createFn,
		},
		rollbacker: rollbacker{
			resource: resource,
			fn:       func(_ platform.ID) error { return s.rollbackVariables(ctx, rollBackVars) },
		},
	}
}

func (s *Service) rollbackVariables(ctx context.Context, variables []*stateVariable) error {
	rollbackFn := func(v *stateVariable) error {
		var err error
		switch {
		case IsRemoval(v.stateStatus):
			if v.existing == nil {
				return nil
			}
			err = ierrors.Wrap(s.varSVC.CreateVariable(ctx, v.existing), "rolling back removed variable")
		case IsExisting(v.stateStatus):
			if v.existing == nil {
				return nil
			}
			_, err = s.varSVC.UpdateVariable(ctx, v.ID(), &influxdb.VariableUpdate{
				Name:        v.existing.Name,
				Description: v.existing.Description,
				Selected:    v.existing.Selected,
				Arguments:   v.existing.Arguments,
			})
			err = ierrors.Wrap(err, "rolling back updated variable")
		default:
			err = ierrors.Wrap(s.varSVC.DeleteVariable(ctx, v.ID()), "rolling back created variable")
		}
		return err
	}

	var errs []string
	for _, v := range variables {
		if err := rollbackFn(v); err != nil {
			errs = append(errs, fmt.Sprintf("error for variable[%q]: %s", v.ID(), err))
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}

	return nil
}

func (s *Service) applyVariable(ctx context.Context, v *stateVariable) (influxdb.Variable, error) {
	switch {
	case IsRemoval(v.stateStatus):
		if err := s.varSVC.DeleteVariable(ctx, v.id); err != nil && errors2.ErrorCode(err) != errors2.ENotFound {
			return influxdb.Variable{}, applyFailErr("delete", v.stateIdentity(), err)
		}
		if v.existing == nil {
			return influxdb.Variable{}, nil
		}
		return *v.existing, nil
	case IsExisting(v.stateStatus) && v.existing != nil:
		updatedVar, err := s.varSVC.UpdateVariable(ctx, v.ID(), &influxdb.VariableUpdate{
			Name:        v.parserVar.Name(),
			Selected:    v.parserVar.Selected(),
			Description: v.parserVar.Description,
			Arguments:   v.parserVar.influxVarArgs(),
		})
		if err != nil {
			return influxdb.Variable{}, applyFailErr("update", v.stateIdentity(), err)
		}
		return *updatedVar, nil
	default:
		// when an existing variable (referenced in stack) has been deleted by a user
		// then the resource is created anew to get it back to the expected state.
		influxVar := influxdb.Variable{
			OrganizationID: v.orgID,
			Name:           v.parserVar.Name(),
			Selected:       v.parserVar.Selected(),
			Description:    v.parserVar.Description,
			Arguments:      v.parserVar.influxVarArgs(),
		}
		err := s.varSVC.CreateVariable(ctx, &influxVar)
		if err != nil {
			return influxdb.Variable{}, applyFailErr("create", v.stateIdentity(), err)
		}
		return influxVar, nil
	}
}

func (s *Service) removeLabelMappings(ctx context.Context, labelMappings []stateLabelMappingForRemoval) applier {
	const resource = "removed_label_mapping"

	var rollbackMappings []stateLabelMappingForRemoval

	mutex := new(doMutex)
	createFn := func(ctx context.Context, i int, orgID, userID platform.ID) *applyErrBody {
		var mapping stateLabelMappingForRemoval
		mutex.Do(func() {
			mapping = labelMappings[i]
		})

		err := s.labelSVC.DeleteLabelMapping(ctx, &influxdb.LabelMapping{
			LabelID:      mapping.LabelID,
			ResourceID:   mapping.ResourceID,
			ResourceType: mapping.ResourceType,
		})
		if err != nil && errors2.ErrorCode(err) != errors2.ENotFound {
			return &applyErrBody{
				name: fmt.Sprintf("%s:%s:%s", mapping.ResourceType, mapping.ResourceID, mapping.LabelID),
				msg:  err.Error(),
			}
		}

		mutex.Do(func() {
			rollbackMappings = append(rollbackMappings, mapping)
		})
		return nil
	}

	return applier{
		creater: creater{
			entries: len(labelMappings),
			fn:      createFn,
		},
		rollbacker: rollbacker{
			resource: resource,
			fn:       func(_ platform.ID) error { return s.rollbackRemoveLabelMappings(ctx, rollbackMappings) },
		},
	}
}

func (s *Service) rollbackRemoveLabelMappings(ctx context.Context, mappings []stateLabelMappingForRemoval) error {
	var errs []string
	for _, m := range mappings {
		err := s.labelSVC.CreateLabelMapping(ctx, &influxdb.LabelMapping{
			LabelID:      m.LabelID,
			ResourceID:   m.ResourceID,
			ResourceType: m.ResourceType,
		})
		if err != nil {
			errs = append(errs,
				fmt.Sprintf(
					"error for label mapping: resource_type=%s resource_id=%s label_id=%s err=%s",
					m.ResourceType,
					m.ResourceID,
					m.LabelID,
					err,
				))
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}

	return nil
}

func (s *Service) applyLabelMappings(ctx context.Context, labelMappings []stateLabelMapping) applier {
	const resource = "label_mapping"

	mutex := new(doMutex)
	rollbackMappings := make([]stateLabelMapping, 0, len(labelMappings))

	createFn := func(ctx context.Context, i int, orgID, userID platform.ID) *applyErrBody {
		var mapping stateLabelMapping
		mutex.Do(func() {
			mapping = labelMappings[i]
		})

		ident := mapping.resource.stateIdentity()
		if IsExisting(mapping.status) || mapping.label.ID() == 0 || ident.id == 0 {
			// this block here does 2 things, it does not write a
			// mapping when one exists. it also avoids having to worry
			// about deleting an existing mapping since it will not be
			// passed to the delete function below b/c it is never added
			// to the list of mappings that is referenced in the delete
			// call.
			return nil
		}

		m := influxdb.LabelMapping{
			LabelID:      mapping.label.ID(),
			ResourceID:   ident.id,
			ResourceType: ident.resourceType,
		}
		err := s.labelSVC.CreateLabelMapping(ctx, &m)
		if err != nil {
			return &applyErrBody{
				name: fmt.Sprintf("%s:%s:%s", ident.resourceType, ident.id, mapping.label.ID()),
				msg:  err.Error(),
			}
		}

		mutex.Do(func() {
			rollbackMappings = append(rollbackMappings, mapping)
		})

		return nil
	}

	return applier{
		creater: creater{
			entries: len(labelMappings),
			fn:      createFn,
		},
		rollbacker: rollbacker{
			resource: resource,
			fn:       func(_ platform.ID) error { return s.rollbackLabelMappings(ctx, rollbackMappings) },
		},
	}
}

func (s *Service) rollbackLabelMappings(ctx context.Context, mappings []stateLabelMapping) error {
	var errs []string
	for _, stateMapping := range mappings {
		influxMapping := stateLabelMappingToInfluxLabelMapping(stateMapping)
		err := s.labelSVC.DeleteLabelMapping(ctx, &influxMapping)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s:%s", stateMapping.label.ID(), stateMapping.resource.stateIdentity().id))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf(`label_resource_id_pairs=[%s] err="unable to delete label"`, strings.Join(errs, ", "))
	}

	return nil
}

func (s *Service) templateFromApplyOpts(ctx context.Context, opt ApplyOpt) (*Template, error) {
	if opt.StackID != 0 {
		remotes, err := s.getStackRemoteTemplates(ctx, opt.StackID)
		if err != nil {
			return nil, err
		}
		opt.Templates = append(opt.Templates, remotes...)
	}

	return Combine(opt.Templates, ValidWithoutResources())
}

func (s *Service) getStackRemoteTemplates(ctx context.Context, stackID platform.ID) ([]*Template, error) {
	stack, err := s.store.ReadStackByID(ctx, stackID)
	if err != nil {
		return nil, err
	}

	lastEvent := stack.LatestEvent()
	var remotes []*Template
	for _, rawURL := range lastEvent.TemplateURLs {
		u, err := url.Parse(rawURL)
		if err != nil {
			return nil, &errors2.Error{
				Code: errors2.EInternal,
				Msg:  "failed to parse url",
				Err:  err,
			}
		}

		encoding := EncodingSource
		switch path.Ext(u.String()) {
		case ".jsonnet":
			encoding = EncodingJsonnet
		case ".json":
			encoding = EncodingJSON
		case ".yaml", ".yml":
			encoding = EncodingYAML
		}

		readerFn := FromHTTPRequest(u.String(), s.client)
		if u.Scheme == "file" {
			readerFn = FromFile(u.Path)
		}

		template, err := Parse(encoding, readerFn)
		if err != nil {
			return nil, err
		}
		remotes = append(remotes, template)
	}
	return remotes, nil
}

func (s *Service) updateStackAfterSuccess(ctx context.Context, stackID platform.ID, state *stateCoordinator, sources []string) error {
	stack, err := s.store.ReadStackByID(ctx, stackID)
	if err != nil {
		return err
	}

	var stackResources []StackResource
	for _, b := range state.mBuckets {
		if IsRemoval(b.stateStatus) || isSystemBucket(b.existing) {
			continue
		}
		stackResources = append(stackResources, StackResource{
			APIVersion:   APIVersion,
			ID:           b.ID(),
			Kind:         KindBucket,
			MetaName:     b.parserBkt.MetaName(),
			Associations: stateLabelsToStackAssociations(b.labels()),
		})
	}
	for _, c := range state.mChecks {
		if IsRemoval(c.stateStatus) {
			continue
		}
		stackResources = append(stackResources, StackResource{
			APIVersion:   APIVersion,
			ID:           c.ID(),
			Kind:         KindCheck,
			MetaName:     c.parserCheck.MetaName(),
			Associations: stateLabelsToStackAssociations(c.labels()),
		})
	}
	for _, d := range state.mDashboards {
		if IsRemoval(d.stateStatus) {
			continue
		}
		stackResources = append(stackResources, StackResource{
			APIVersion:   APIVersion,
			ID:           d.ID(),
			Kind:         KindDashboard,
			MetaName:     d.parserDash.MetaName(),
			Associations: stateLabelsToStackAssociations(d.labels()),
		})
	}
	for _, n := range state.mEndpoints {
		if IsRemoval(n.stateStatus) {
			continue
		}
		stackResources = append(stackResources, StackResource{
			APIVersion:   APIVersion,
			ID:           n.ID(),
			Kind:         KindNotificationEndpoint,
			MetaName:     n.parserEndpoint.MetaName(),
			Associations: stateLabelsToStackAssociations(n.labels()),
		})
	}
	for _, l := range state.mLabels {
		if IsRemoval(l.stateStatus) {
			continue
		}
		stackResources = append(stackResources, StackResource{
			APIVersion: APIVersion,
			ID:         l.ID(),
			Kind:       KindLabel,
			MetaName:   l.parserLabel.MetaName(),
		})
	}
	for _, r := range state.mRules {
		if IsRemoval(r.stateStatus) {
			continue
		}
		stackResources = append(stackResources, StackResource{
			APIVersion: APIVersion,
			ID:         r.ID(),
			Kind:       KindNotificationRule,
			MetaName:   r.parserRule.MetaName(),
			Associations: append(
				stateLabelsToStackAssociations(r.labels()),
				r.endpointAssociation(),
			),
		})
	}
	for _, t := range state.mTasks {
		if IsRemoval(t.stateStatus) || isRestrictedTask(t.existing) {
			continue
		}
		stackResources = append(stackResources, StackResource{
			APIVersion:   APIVersion,
			ID:           t.ID(),
			Kind:         KindTask,
			MetaName:     t.parserTask.MetaName(),
			Associations: stateLabelsToStackAssociations(t.labels()),
		})
	}
	for _, t := range state.mTelegrafs {
		if IsRemoval(t.stateStatus) {
			continue
		}
		stackResources = append(stackResources, StackResource{
			APIVersion:   APIVersion,
			ID:           t.ID(),
			Kind:         KindTelegraf,
			MetaName:     t.parserTelegraf.MetaName(),
			Associations: stateLabelsToStackAssociations(t.labels()),
		})
	}
	for _, v := range state.mVariables {
		if IsRemoval(v.stateStatus) {
			continue
		}
		stackResources = append(stackResources, StackResource{
			APIVersion:   APIVersion,
			ID:           v.ID(),
			Kind:         KindVariable,
			MetaName:     v.parserVar.MetaName(),
			Associations: stateLabelsToStackAssociations(v.labels()),
		})
	}
	ev := stack.LatestEvent()
	ev.EventType = StackEventUpdate
	ev.Resources = stackResources
	ev.Sources = sources
	ev.UpdatedAt = s.timeGen.Now()
	stack.Events = append(stack.Events, ev)
	return s.store.UpdateStack(ctx, stack)
}

func (s *Service) updateStackAfterRollback(ctx context.Context, stackID platform.ID, state *stateCoordinator, sources []string) error {
	stack, err := s.store.ReadStackByID(ctx, stackID)
	if err != nil {
		return err
	}

	type key struct {
		k        Kind
		metaName string
	}
	newKey := func(k Kind, metaName string) key {
		return key{k: k, metaName: metaName}
	}

	latestEvent := stack.LatestEvent()
	existingResources := make(map[key]*StackResource)
	for i := range latestEvent.Resources {
		res := latestEvent.Resources[i]
		existingResources[newKey(res.Kind, res.MetaName)] = &latestEvent.Resources[i]
	}

	hasChanges := false
	{
		// these are the case where a deletion happens and is rolled back creating a new resource.
		// when resource is not to be removed this is a nothing burger, as it should be
		// rolled back to previous state.
		for _, b := range state.mBuckets {
			res, ok := existingResources[newKey(KindBucket, b.parserBkt.MetaName())]
			if ok && res.ID != b.ID() {
				hasChanges = true
				res.ID = b.existing.ID
			}
		}
		for _, c := range state.mChecks {
			res, ok := existingResources[newKey(KindCheck, c.parserCheck.MetaName())]
			if ok && res.ID != c.ID() {
				hasChanges = true
				res.ID = c.existing.GetID()
			}
		}
		for _, d := range state.mDashboards {
			res, ok := existingResources[newKey(KindDashboard, d.parserDash.MetaName())]
			if ok && res.ID != d.ID() {
				hasChanges = true
				res.ID = d.existing.ID
			}
		}
		for _, e := range state.mEndpoints {
			res, ok := existingResources[newKey(KindNotificationEndpoint, e.parserEndpoint.MetaName())]
			if ok && res.ID != e.ID() {
				hasChanges = true
				res.ID = e.existing.GetID()
			}
		}
		for _, l := range state.mLabels {
			res, ok := existingResources[newKey(KindLabel, l.parserLabel.MetaName())]
			if ok && res.ID != l.ID() {
				hasChanges = true
				res.ID = l.existing.ID
			}
		}
		for _, r := range state.mRules {
			res, ok := existingResources[newKey(KindNotificationRule, r.parserRule.MetaName())]
			if !ok {
				continue
			}

			if res.ID != r.ID() {
				hasChanges = true
				res.ID = r.existing.GetID()
			}

			endpointAssociation := r.endpointAssociation()
			newAss := make([]StackResourceAssociation, 0, len(res.Associations))

			var endpointAssociationChanged bool
			for _, ass := range res.Associations {
				if ass.Kind.is(KindNotificationEndpoint) && ass != endpointAssociation {
					endpointAssociationChanged = true
					ass = endpointAssociation
				}
				newAss = append(newAss, ass)
			}
			if endpointAssociationChanged {
				hasChanges = true
				res.Associations = newAss
			}
		}
		for _, t := range state.mTasks {
			res, ok := existingResources[newKey(KindTask, t.parserTask.MetaName())]
			if ok && res.ID != t.ID() {
				hasChanges = true
				res.ID = t.existing.ID
			}
		}
		for _, t := range state.mTelegrafs {
			res, ok := existingResources[newKey(KindTelegraf, t.parserTelegraf.MetaName())]
			if ok && res.ID != t.ID() {
				hasChanges = true
				res.ID = t.existing.ID
			}
		}
		for _, v := range state.mVariables {
			res, ok := existingResources[newKey(KindVariable, v.parserVar.MetaName())]
			if ok && res.ID != v.ID() {
				hasChanges = true
				res.ID = v.existing.ID
			}
		}
	}
	if !hasChanges {
		return nil
	}

	latestEvent.EventType = StackEventUpdate
	latestEvent.Sources = sources
	latestEvent.UpdatedAt = s.timeGen.Now()
	stack.Events = append(stack.Events, latestEvent)
	return s.store.UpdateStack(ctx, stack)
}

func (s *Service) findLabel(ctx context.Context, orgID platform.ID, l *stateLabel) (*influxdb.Label, error) {
	if l.ID() != 0 {
		return s.labelSVC.FindLabelByID(ctx, l.ID())
	}

	existingLabels, err := s.labelSVC.FindLabels(ctx, influxdb.LabelFilter{
		Name:  l.parserLabel.Name(),
		OrgID: &orgID,
	}, influxdb.FindOptions{Limit: 1})
	if err != nil {
		return nil, err
	}
	if len(existingLabels) == 0 {
		return nil, errors.New("no labels found for name: " + l.parserLabel.Name())
	}
	return existingLabels[0], nil
}

func (s *Service) getAllPlatformVariables(ctx context.Context, orgID platform.ID) ([]*influxdb.Variable, error) {
	const limit = 100

	var (
		existingVars []*influxdb.Variable
		offset       int
	)
	for {
		vars, err := s.varSVC.FindVariables(ctx, influxdb.VariableFilter{
			OrganizationID: &orgID,
			// TODO: would be ideal to extend find variables to allow for a name matcher
			//  since names are unique for vars within an org. In the meanwhile, make large
			//  limit returned vars, should be more than enough for the time being.
		}, influxdb.FindOptions{Limit: limit, Offset: offset})
		if err != nil {
			return nil, err
		}
		existingVars = append(existingVars, vars...)

		if len(vars) < limit {
			break
		}
		offset += len(vars)
	}
	return existingVars, nil
}

func (s *Service) getAllChecks(ctx context.Context, orgID platform.ID) ([]influxdb.Check, error) {
	filter := influxdb.CheckFilter{OrgID: &orgID}
	const limit = 100

	var (
		out    []influxdb.Check
		offset int
	)
	for {
		checks, _, err := s.checkSVC.FindChecks(ctx, filter, influxdb.FindOptions{
			Limit:  limit,
			Offset: offset,
		})
		if err != nil {
			return nil, err
		}
		out = append(out, checks...)
		if len(checks) < limit {
			break
		}
		offset += limit
	}
	return out, nil
}

func (s *Service) getNotificationRules(ctx context.Context, orgID platform.ID) ([]influxdb.NotificationRule, error) {
	filter := influxdb.NotificationRuleFilter{OrgID: &orgID}
	const limit = 100

	var (
		out    []influxdb.NotificationRule
		offset int
	)
	for {
		rules, _, err := s.ruleSVC.FindNotificationRules(ctx, filter)
		if err != nil {
			return nil, err
		}
		out = append(out, rules...)
		if len(rules) < limit {
			break
		}
		offset += limit
	}
	return out, nil

}

func (s *Service) getAllTasks(ctx context.Context, orgID platform.ID) ([]*taskmodel.Task, error) {
	var (
		out     []*taskmodel.Task
		afterID *platform.ID
	)
	for {
		f := taskmodel.TaskFilter{
			OrganizationID: &orgID,
			Limit:          taskmodel.TaskMaxPageSize,
		}
		if afterID != nil {
			f.After = afterID
		}
		tasks, _, err := s.taskSVC.FindTasks(ctx, f)
		if err != nil {
			return nil, err
		}
		if len(tasks) == 0 {
			break
		}
		out = append(out, tasks...)
		afterID = &tasks[len(tasks)-1].ID
	}
	return out, nil
}

func newSummaryFromStateTemplate(state *stateCoordinator, template *Template) Summary {
	stateSum := state.summary()
	stateSum.MissingEnvs = template.missingEnvRefs()
	stateSum.MissingSecrets = template.missingSecrets()
	return stateSum
}

func stateLabelsToStackAssociations(stateLabels []*stateLabel) []StackResourceAssociation {
	var out []StackResourceAssociation
	for _, l := range stateLabels {
		out = append(out, StackResourceAssociation{
			Kind:     KindLabel,
			MetaName: l.parserLabel.MetaName(),
		})
	}
	return out
}

func applyFailErr(method string, ident stateIdentity, err error) error {
	v := ident.id.String()
	if v == "" {
		v = ident.metaName
	}
	msg := fmt.Sprintf("failed to %s %s[%q]", method, ident.resourceType, v)
	return ierrors.Wrap(err, msg)
}

func getLabelIDMap(ctx context.Context, labelSVC influxdb.LabelService, labelNames []string) (map[platform.ID]bool, error) {
	mLabelIDs := make(map[platform.ID]bool)
	for _, labelName := range labelNames {
		iLabels, err := labelSVC.FindLabels(ctx, influxdb.LabelFilter{
			Name: labelName,
		})
		if err != nil {
			return nil, err
		}
		if len(iLabels) == 1 {
			mLabelIDs[iLabels[0].ID] = true
		}
	}
	return mLabelIDs, nil
}

func sortObjects(objects []Object) []Object {
	sort.Slice(objects, func(i, j int) bool {
		iName, jName := objects[i].Name(), objects[j].Name()
		iKind, jKind := objects[i].Kind, objects[j].Kind

		if iKind.is(jKind) {
			return iName < jName
		}
		return kindPriorities[iKind] < kindPriorities[jKind]
	})
	return objects
}

type doMutex struct {
	sync.Mutex
}

func (m *doMutex) Do(fn func()) {
	m.Lock()
	defer m.Unlock()
	fn()
}

type (
	applier struct {
		creater    creater
		rollbacker rollbacker
	}

	rollbacker struct {
		resource string
		fn       func(orgID platform.ID) error
	}

	creater struct {
		entries int
		fn      func(ctx context.Context, i int, orgID, userID platform.ID) *applyErrBody
	}
)

type rollbackCoordinator struct {
	logger    *zap.Logger
	rollbacks []rollbacker

	sem chan struct{}
}

func newRollbackCoordinator(logger *zap.Logger, reqLimit int) *rollbackCoordinator {
	return &rollbackCoordinator{
		logger: logger,
		sem:    make(chan struct{}, reqLimit),
	}
}

func (r *rollbackCoordinator) runTilEnd(ctx context.Context, orgID, userID platform.ID, appliers ...applier) error {
	errStr := newErrStream(ctx)

	wg := new(sync.WaitGroup)
	for i := range appliers {
		// cannot reuse the shared variable from for loop since we're using concurrency b/c
		// that temp var gets recycled between iterations
		app := appliers[i]
		r.rollbacks = append(r.rollbacks, app.rollbacker)
		for idx := range make([]struct{}, app.creater.entries) {
			r.sem <- struct{}{}
			wg.Add(1)

			go func(i int, resource string) {
				defer func() {
					wg.Done()
					<-r.sem
				}()

				ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()

				defer func() {
					if err := recover(); err != nil {
						r.logger.Error(
							"panic applying "+resource,
							zap.String("stack_trace", fmt.Sprintf("%+v", stack.Trace())),
							zap.Reflect("panic", err),
						)
						errStr.add(errMsg{
							resource: resource,
							err: applyErrBody{
								msg: fmt.Sprintf("panic: %s paniced", resource),
							},
						})
					}
				}()

				if err := app.creater.fn(ctx, i, orgID, userID); err != nil {
					errStr.add(errMsg{resource: resource, err: *err})
				}
			}(idx, app.rollbacker.resource)
		}
	}
	wg.Wait()

	return errStr.close()
}

func (r *rollbackCoordinator) rollback(l *zap.Logger, err *error, orgID platform.ID) {
	if *err == nil {
		return
	}

	for _, r := range r.rollbacks {
		if err := r.fn(orgID); err != nil {
			l.Error("failed to delete "+r.resource, zap.Error(err))
		}
	}
}

type errMsg struct {
	resource string
	err      applyErrBody
}

type errStream struct {
	msgStream chan errMsg
	err       chan error
	done      <-chan struct{}
}

func newErrStream(ctx context.Context) *errStream {
	e := &errStream{
		msgStream: make(chan errMsg),
		err:       make(chan error),
		done:      ctx.Done(),
	}
	e.do()
	return e
}

func (e *errStream) do() {
	go func() {
		mErrs := func() map[string]applyErrs {
			mErrs := make(map[string]applyErrs)
			for {
				select {
				case <-e.done:
					return nil
				case msg, ok := <-e.msgStream:
					if !ok {
						return mErrs
					}
					mErrs[msg.resource] = append(mErrs[msg.resource], &msg.err)
				}
			}
		}()

		if len(mErrs) == 0 {
			e.err <- nil
			return
		}

		var errs []string
		for resource, err := range mErrs {
			errs = append(errs, err.toError(resource, "failed to apply resource").Error())
		}
		e.err <- errors.New(strings.Join(errs, "\n"))
	}()
}

func (e *errStream) close() error {
	close(e.msgStream)
	return <-e.err
}

func (e *errStream) add(msg errMsg) {
	select {
	case <-e.done:
	case e.msgStream <- msg:
	}
}

// TODO: clean up apply errors to inform the user in an actionable way
type applyErrBody struct {
	name string
	msg  string
}

type applyErrs []*applyErrBody

func (a applyErrs) toError(resType, msg string) error {
	if len(a) == 0 {
		return nil
	}
	errMsg := fmt.Sprintf(`resource_type=%q err=%q`, resType, msg)
	for _, e := range a {
		errMsg += fmt.Sprintf("\n\tmetadata_name=%q err_msg=%q", e.name, e.msg)
	}
	return errors.New(errMsg)
}

func validURLs(urls []string) error {
	for _, u := range urls {
		if _, err := url.Parse(u); err != nil {
			msg := fmt.Sprintf("url invalid for entry %q", u)
			return influxErr(errors2.EInvalid, msg)
		}
	}
	return nil
}

func isRestrictedTask(t *taskmodel.Task) bool {
	return t != nil && t.Type != taskmodel.TaskSystemType
}

func isSystemBucket(b *influxdb.Bucket) bool {
	return b != nil && b.Type == influxdb.BucketTypeSystem
}

func labelSlcToMap(labels []*stateLabel) map[string]*stateLabel {
	m := make(map[string]*stateLabel)
	for i := range labels {
		m[labels[i].Name()] = labels[i]
	}
	return m
}

func failedValidationErr(err error) error {
	if err == nil {
		return nil
	}
	return &errors2.Error{Code: errors2.EUnprocessableEntity, Err: err}
}

func internalErr(err error) error {
	if err == nil {
		return nil
	}
	return influxErr(errors2.EInternal, err)
}

func influxErr(code string, errArg interface{}, rest ...interface{}) *errors2.Error {
	err := &errors2.Error{
		Code: code,
	}
	for _, a := range append(rest, errArg) {
		switch v := a.(type) {
		case string:
			err.Msg = v
		case error:
			err.Err = v
		case nil:
		case interface{ String() string }:
			err.Msg = v.String()
		}
	}
	return err
}
