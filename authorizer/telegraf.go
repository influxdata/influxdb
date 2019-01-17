package authorizer

import (
	"context"

	"github.com/influxdata/influxdb"
)

var _ influxdb.TelegrafConfigStore = (*TelegrafConfigService)(nil)

// TelegrafConfigService wraps a influxdb.TelegrafConfigStore and authorizes actions
// against it appropriately.
type TelegrafConfigService struct {
	s influxdb.TelegrafConfigStore
	influxdb.UserResourceMappingService
}

// NewTelegrafConfigService constructs an instance of an authorizing telegraf serivce.
func NewTelegrafConfigService(s influxdb.TelegrafConfigStore, urm influxdb.UserResourceMappingService) *TelegrafConfigService {
	return &TelegrafConfigService{
		s:                          s,
		UserResourceMappingService: urm,
	}
}

func newTelegrafPermission(a influxdb.Action, orgID, id influxdb.ID) (*influxdb.Permission, error) {
	return influxdb.NewPermissionAtID(id, a, influxdb.TelegrafsResourceType, orgID)
}

func authorizeReadTelegraf(ctx context.Context, orgID, id influxdb.ID) error {
	p, err := newTelegrafPermission(influxdb.ReadAction, orgID, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

func authorizeWriteTelegraf(ctx context.Context, orgID, id influxdb.ID) error {
	p, err := newTelegrafPermission(influxdb.WriteAction, orgID, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

// FindTelegrafConfigByID checks to see if the authorizer on context has read access to the id provided.
func (s *TelegrafConfigService) FindTelegrafConfigByID(ctx context.Context, id influxdb.ID) (*influxdb.TelegrafConfig, error) {
	tc, err := s.s.FindTelegrafConfigByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := authorizeReadTelegraf(ctx, tc.OrganizationID, id); err != nil {
		return nil, err
	}

	return tc, nil
}

// FindTelegrafConfig retrieves the telegraf config and checks to see if the authorizer on context has read access to the telegraf config.
func (s *TelegrafConfigService) FindTelegrafConfig(ctx context.Context, filter influxdb.TelegrafConfigFilter) (*influxdb.TelegrafConfig, error) {
	tc, err := s.s.FindTelegrafConfig(ctx, filter)
	if err != nil {
		return nil, err
	}

	if err := authorizeReadTelegraf(ctx, tc.OrganizationID, tc.ID); err != nil {
		return nil, err
	}

	return tc, nil
}

// FindTelegrafConfigs retrieves all telegraf configs that match the provided filter and then filters the list down to only the resources that are authorized.
func (s *TelegrafConfigService) FindTelegrafConfigs(ctx context.Context, filter influxdb.TelegrafConfigFilter, opt ...influxdb.FindOptions) ([]*influxdb.TelegrafConfig, int, error) {
	// TODO: we'll likely want to push this operation into the database eventually since fetching the whole list of data
	// will likely be expensive.
	ts, _, err := s.s.FindTelegrafConfigs(ctx, filter, opt...)
	if err != nil {
		return nil, 0, err
	}

	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	telegrafs := ts[:0]
	for _, tc := range ts {
		err := authorizeReadTelegraf(ctx, tc.OrganizationID, tc.ID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}

		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}

		telegrafs = append(telegrafs, tc)
	}

	return telegrafs, len(telegrafs), nil
}

// CreateTelegrafConfig checks to see if the authorizer on context has write access to the global telegraf config resource.
func (s *TelegrafConfigService) CreateTelegrafConfig(ctx context.Context, tc *influxdb.TelegrafConfig, userID influxdb.ID) error {
	p, err := influxdb.NewPermission(influxdb.WriteAction, influxdb.TelegrafsResourceType, tc.OrganizationID)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return s.s.CreateTelegrafConfig(ctx, tc, userID)
}

// UpdateTelegrafConfig checks to see if the authorizer on context has write access to the telegraf config provided.
func (s *TelegrafConfigService) UpdateTelegrafConfig(ctx context.Context, id influxdb.ID, upd *influxdb.TelegrafConfig, userID influxdb.ID) (*influxdb.TelegrafConfig, error) {
	tc, err := s.FindTelegrafConfigByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := authorizeWriteTelegraf(ctx, tc.OrganizationID, id); err != nil {
		return nil, err
	}

	return s.s.UpdateTelegrafConfig(ctx, id, upd, userID)
}

// DeleteTelegrafConfig checks to see if the authorizer on context has write access to the telegraf config provided.
func (s *TelegrafConfigService) DeleteTelegrafConfig(ctx context.Context, id influxdb.ID) error {
	tc, err := s.FindTelegrafConfigByID(ctx, id)
	if err != nil {
		return err
	}

	if err := authorizeWriteTelegraf(ctx, tc.OrganizationID, id); err != nil {
		return err
	}

	return s.s.DeleteTelegrafConfig(ctx, id)
}
