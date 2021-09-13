package authorizer

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

var _ influxdb.TelegrafConfigStore = (*TelegrafConfigService)(nil)

// TelegrafConfigService wraps a influxdb.TelegrafConfigStore and authorizes actions
// against it appropriately.
type TelegrafConfigService struct {
	s influxdb.TelegrafConfigStore
	influxdb.UserResourceMappingService
}

// NewTelegrafConfigService constructs an instance of an authorizing telegraf service.
func NewTelegrafConfigService(s influxdb.TelegrafConfigStore, urm influxdb.UserResourceMappingService) *TelegrafConfigService {
	return &TelegrafConfigService{
		s:                          s,
		UserResourceMappingService: urm,
	}
}

// FindTelegrafConfigByID checks to see if the authorizer on context has read access to the id provided.
func (s *TelegrafConfigService) FindTelegrafConfigByID(ctx context.Context, id platform.ID) (*influxdb.TelegrafConfig, error) {
	tc, err := s.s.FindTelegrafConfigByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeRead(ctx, influxdb.TelegrafsResourceType, tc.ID, tc.OrgID); err != nil {
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
	return AuthorizeFindTelegrafs(ctx, ts)
}

// CreateTelegrafConfig checks to see if the authorizer on context has write access to the global telegraf config resource.
func (s *TelegrafConfigService) CreateTelegrafConfig(ctx context.Context, tc *influxdb.TelegrafConfig, userID platform.ID) error {
	if _, _, err := AuthorizeCreate(ctx, influxdb.TelegrafsResourceType, tc.OrgID); err != nil {
		return err
	}
	return s.s.CreateTelegrafConfig(ctx, tc, userID)
}

// UpdateTelegrafConfig checks to see if the authorizer on context has write access to the telegraf config provided.
func (s *TelegrafConfigService) UpdateTelegrafConfig(ctx context.Context, id platform.ID, upd *influxdb.TelegrafConfig, userID platform.ID) (*influxdb.TelegrafConfig, error) {
	tc, err := s.FindTelegrafConfigByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.TelegrafsResourceType, tc.ID, tc.OrgID); err != nil {
		return nil, err
	}
	return s.s.UpdateTelegrafConfig(ctx, id, upd, userID)
}

// DeleteTelegrafConfig checks to see if the authorizer on context has write access to the telegraf config provided.
func (s *TelegrafConfigService) DeleteTelegrafConfig(ctx context.Context, id platform.ID) error {
	tc, err := s.FindTelegrafConfigByID(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.TelegrafsResourceType, tc.ID, tc.OrgID); err != nil {
		return err
	}
	return s.s.DeleteTelegrafConfig(ctx, id)
}
