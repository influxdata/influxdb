package endpoints

import (
	"context"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/snowflake"
)

const serviceName = "notification endpoint"

type (
	// Service provides all the notification endpoint service behavior.
	Service struct {
		idGen   influxdb.IDGenerator
		timeGen influxdb.TimeGenerator

		store     Store
		orgSVC    influxdb.OrganizationService
		secretSVC influxdb.SecretService
		urmSVC    influxdb.UserResourceMappingService
	}

	Store interface {
		Init(ctx context.Context) error
		Create(ctx context.Context, edp influxdb.NotificationEndpoint) error
		Delete(ctx context.Context, id influxdb.ID) error
		Find(ctx context.Context, f FindFilter) ([]influxdb.NotificationEndpoint, error)
		FindByID(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error)
		Update(ctx context.Context, edp influxdb.NotificationEndpoint) error
	}

	ServiceMW func(svc influxdb.NotificationEndpointService) influxdb.NotificationEndpointService

	ServiceSetterFn func(opt *serviceOpt)

	serviceOpt struct {
		idGen   influxdb.IDGenerator
		timeGen influxdb.TimeGenerator

		store     Store
		orgSVC    influxdb.OrganizationService
		secretSVC influxdb.SecretService
		urmSVC    influxdb.UserResourceMappingService
	}
)

func WithIDGenerator(idGen influxdb.IDGenerator) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.idGen = idGen
	}
}

func WithTimeGenerator(timeGen influxdb.TimeGenerator) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.timeGen = timeGen
	}
}

func WithStore(store Store) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.store = store
	}
}

func WithOrgSVC(orgSVC influxdb.OrganizationService) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.orgSVC = orgSVC
	}
}

func WithSecretSVC(secretSVC influxdb.SecretService) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.secretSVC = secretSVC
	}
}

func WithUserResourceMappingSVC(urmSVC influxdb.UserResourceMappingService) ServiceSetterFn {
	return func(opt *serviceOpt) {
		opt.urmSVC = urmSVC
	}
}

// NewService constructs a new Service.
func NewService(opts ...ServiceSetterFn) *Service {
	opt := serviceOpt{
		idGen:   snowflake.NewIDGenerator(),
		timeGen: influxdb.RealTimeGenerator{},
	}
	for _, o := range opts {
		o(&opt)
	}

	return &Service{
		idGen:   opt.idGen,
		timeGen: opt.timeGen,
		store:   opt.store,

		orgSVC:    opt.orgSVC,
		secretSVC: opt.secretSVC,
		urmSVC:    opt.urmSVC,
	}
}

var _ influxdb.NotificationEndpointService = (*Service)(nil)

func (s *Service) Delete(ctx context.Context, id influxdb.ID) error {
	// TODO: extend mapping service to be able to delete
	// 	by ResID and ResType (ideally just ResID is all that is needed)

	mappings, _, err := s.urmSVC.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
		ResourceID:   id,
		ResourceType: influxdb.NotificationEndpointResourceType,
	})
	if err != nil {
		return err
	}

	if err := s.store.Delete(ctx, id); err != nil {
		return err
	}

	for _, mapping := range mappings {
		if err := s.urmSVC.DeleteUserResourceMapping(ctx, id, mapping.UserID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) FindByID(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
	if endpoint := getEndpoint(ctx); endpoint != nil {
		return endpoint, nil
	}
	return s.store.FindByID(ctx, id)
}

func (s *Service) Find(ctx context.Context, f influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, error) {
	mappings, _, err := s.urmSVC.FindUserResourceMappings(ctx, f.UserResourceMappingFilter)
	if err != nil {
		return nil, &influxdb.Error{Code: influxdb.EInternal, Err: err}
	}
	if len(mappings) == 0 {
		return []influxdb.NotificationEndpoint{}, nil
	}

	filter, err := s.newStoreFindFilter(ctx, mappings, f, opt...)
	if err != nil {
		return nil, err
	}

	return s.store.Find(ctx, filter)
}

func (s *Service) newStoreFindFilter(ctx context.Context, mappings []*influxdb.UserResourceMapping, f influxdb.NotificationEndpointFilter, opts ...influxdb.FindOptions) (FindFilter, error) {
	mUserMappings := make(map[influxdb.ID]bool)
	for _, item := range mappings {
		mUserMappings[item.ResourceID] = true
	}

	filter := FindFilter{UserMappings: mUserMappings}
	if f.OrgID != nil || f.Org != nil {
		org, err := s.orgSVC.FindOrganization(ctx, influxdb.OrganizationFilter{
			Name: f.Org,
			ID:   f.OrgID,
		})
		if err != nil {
			return FindFilter{}, err
		}
		filter.OrgID = org.ID
	}

	if len(opts) > 0 {
		filter.Descending = opts[0].Descending
		filter.Offset = opts[0].Offset
		filter.Limit = opts[0].Limit
	}

	return filter, nil
}

func (s *Service) Create(ctx context.Context, userID influxdb.ID, endpoint influxdb.NotificationEndpoint) error {
	base := endpoint.Base()
	if _, err := s.orgSVC.FindOrganizationByID(ctx, base.OrgID); err != nil {
		return &influxdb.Error{
			Code: influxdb.EConflict,
			Msg:  "organization is not valid for orgID: " + base.OrgID.String(),
			Err:  err,
		}
	}

	base.ID = s.idGen.ID()
	now := s.timeGen.Now()
	base.CreatedAt = now
	base.UpdatedAt = now
	endpoint.BackfillSecretKeys()
	if err := endpoint.Valid(); err != nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	if err := s.store.Create(ctx, endpoint); err != nil {
		return err
	}

	err := s.urmSVC.CreateUserResourceMapping(ctx, &influxdb.UserResourceMapping{
		ResourceID:   base.ID,
		UserID:       userID,
		UserType:     influxdb.Owner,
		ResourceType: influxdb.NotificationEndpointResourceType,
	})
	if err != nil {
		return err
	}
	return s.putSecrets(ctx, endpoint)
}

func UpdateEndpoint(endpoint influxdb.NotificationEndpoint) influxdb.EndpointUpdate {
	fn := func(now time.Time, existing influxdb.NotificationEndpoint) (influxdb.NotificationEndpoint, error) {
		endpoint.BackfillSecretKeys() // :sadpanda:
		base := endpoint.Base()
		base.CreatedAt = existing.Base().CreatedAt
		base.UpdatedAt = now
		return endpoint, endpoint.Valid()
	}
	return influxdb.EndpointUpdate{
		UpdateType: "endpoint",
		ID:         endpoint.Base().ID,
		Fn:         fn,
	}
}

func UpdateChangeSet(id influxdb.ID, update influxdb.NotificationEndpointUpdate) influxdb.EndpointUpdate {
	fn := func(now time.Time, existing influxdb.NotificationEndpoint) (influxdb.NotificationEndpoint, error) {
		if err := update.Valid(); err != nil {
			return nil, err
		}

		base := existing.Base()
		if update.Name != nil {
			base.Name = *update.Name
		}
		if update.Description != nil {
			base.Description = *update.Description
		}
		if update.Status != nil {
			base.Status = *update.Status
		}
		return UpdateEndpoint(existing).Fn(now, existing)
	}
	return influxdb.EndpointUpdate{
		UpdateType: "change_set",
		ID:         id,
		Fn:         fn,
	}
}

func (s *Service) Update(ctx context.Context, update influxdb.EndpointUpdate) (influxdb.NotificationEndpoint, error) {
	current, err := s.FindByID(ctx, update.ID)
	if err != nil {
		return nil, err
	}

	updateEndpoint, err := update.Fn(s.timeGen.Now(), current)
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	fns := []func(context.Context, influxdb.NotificationEndpoint) error{
		s.store.Update,
		s.putSecrets,
	}
	for _, fn := range fns {
		if err := fn(ctx, updateEndpoint); err != nil {
			return nil, err
		}
	}

	return updateEndpoint, nil
}

func (s *Service) putSecrets(ctx context.Context, endpoint influxdb.NotificationEndpoint) error {
	secrets := make(map[string]string)
	for _, fld := range endpoint.SecretFields() {
		if fld.Value != nil {
			secrets[fld.Key] = *fld.Value
		}
	}
	if len(secrets) == 0 {
		return nil
	}

	if err := s.secretSVC.PutSecrets(ctx, endpoint.Base().OrgID, secrets); err != nil {
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  "failed to put secrets",
			Err:  err,
		}
	}
	return nil
}
