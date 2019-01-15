package inmem

import (
	"context"

	platform "github.com/influxdata/influxdb"
)

var _ platform.TelegrafConfigStore = new(Service)

// FindTelegrafConfigByID returns a single telegraf config by ID.
func (s *Service) FindTelegrafConfigByID(ctx context.Context, id platform.ID) (tc *platform.TelegrafConfig, err error) {
	op := OpPrefix + platform.OpFindTelegrafConfigByID
	var pErr *platform.Error
	tc, pErr = s.findTelegrafConfigByID(ctx, id)
	if pErr != nil {
		pErr.Op = op
		err = pErr
	}
	return tc, err
}

func (s *Service) findTelegrafConfigByID(ctx context.Context, id platform.ID) (*platform.TelegrafConfig, *platform.Error) {
	if !id.Valid() {
		return nil, &platform.Error{
			Code: platform.EEmptyValue,
			Err:  platform.ErrInvalidID,
		}
	}
	result, found := s.telegrafConfigKV.Load(id)
	if !found {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  platform.ErrTelegrafConfigNotFound,
		}
	}
	tc := new(platform.TelegrafConfig)
	*tc = result.(platform.TelegrafConfig)
	return tc, nil
}

// FindTelegrafConfig returns the first telegraf config that matches filter.
func (s *Service) FindTelegrafConfig(ctx context.Context, filter platform.TelegrafConfigFilter) (*platform.TelegrafConfig, error) {
	op := OpPrefix + platform.OpFindTelegrafConfig
	tcs, n, err := s.FindTelegrafConfigs(ctx, filter, platform.FindOptions{Limit: 1})
	if err != nil {
		return nil, err
	}
	if n > 0 {
		return tcs[0], nil
	}
	return nil, &platform.Error{
		Code: platform.ENotFound,
		Op:   op,
	}
}

func (s *Service) findTelegrafConfigs(ctx context.Context, filter platform.TelegrafConfigFilter, opt ...platform.FindOptions) ([]*platform.TelegrafConfig, int, *platform.Error) {
	tcs := make([]*platform.TelegrafConfig, 0)
	m, _, err := s.FindUserResourceMappings(ctx, filter.UserResourceMappingFilter)
	if err != nil {
		return nil, 0, &platform.Error{
			Err: err,
		}
	}
	if len(m) == 0 {
		return tcs, 0, nil
	}
	for _, item := range m {
		tc, err := s.findTelegrafConfigByID(ctx, item.ResourceID)
		if err != nil && platform.ErrorCode(err) != platform.ENotFound {
			return nil, 0, &platform.Error{
				// return internal error, for any mapping issue
				Err: err,
			}
		}
		if tc != nil {
			// Restrict results by organization ID, if it has been provided
			if filter.OrganizationID != nil && filter.OrganizationID.Valid() && tc.OrganizationID != *filter.OrganizationID {
				continue
			}
			tcs = append(tcs, tc)
		}
	}

	return tcs, len(tcs), nil
}

// FindTelegrafConfigs returns a list of telegraf configs that match filter and the total count of matching telegraf configs.
// Additional options provide pagination & sorting.
func (s *Service) FindTelegrafConfigs(ctx context.Context, filter platform.TelegrafConfigFilter, opt ...platform.FindOptions) (tcs []*platform.TelegrafConfig, n int, err error) {
	op := OpPrefix + platform.OpFindTelegrafConfigs
	var pErr *platform.Error
	tcs, n, pErr = s.findTelegrafConfigs(ctx, filter)
	if pErr != nil {
		pErr.Op = op
		err = pErr
	}
	return tcs, n, err
}

func (s *Service) putTelegrafConfig(ctx context.Context, tc *platform.TelegrafConfig) *platform.Error {
	if !tc.ID.Valid() {
		return &platform.Error{
			Code: platform.EEmptyValue,
			Err:  platform.ErrInvalidID,
		}
	}
	if !tc.OrganizationID.Valid() {
		return &platform.Error{
			Code: platform.EEmptyValue,
			Msg:  platform.ErrTelegrafConfigInvalidOrganizationID,
		}
	}
	s.telegrafConfigKV.Store(tc.ID, *tc)
	return nil
}

// CreateTelegrafConfig creates a new telegraf config and sets b.ID with the new identifier.
func (s *Service) CreateTelegrafConfig(ctx context.Context, tc *platform.TelegrafConfig, userID platform.ID) error {
	op := OpPrefix + platform.OpCreateTelegrafConfig
	tc.ID = s.IDGenerator.ID()

	pErr := s.putTelegrafConfig(ctx, tc)
	if pErr != nil {
		pErr.Op = op
		return pErr
	}

	urm := &platform.UserResourceMapping{
		ResourceID:   tc.ID,
		UserID:       userID,
		UserType:     platform.Owner,
		ResourceType: platform.TelegrafsResourceType,
	}
	if err := s.CreateUserResourceMapping(ctx, urm); err != nil {
		return err
	}

	return nil
}

// UpdateTelegrafConfig updates a single telegraf config.
// Returns the new telegraf config after update.
func (s *Service) UpdateTelegrafConfig(ctx context.Context, id platform.ID, tc *platform.TelegrafConfig, userID platform.ID) (*platform.TelegrafConfig, error) {
	var err error
	op := OpPrefix + platform.OpUpdateTelegrafConfig
	current, pErr := s.findTelegrafConfigByID(ctx, id)
	if pErr != nil {
		pErr.Op = op
		err = pErr
		return nil, err
	}
	tc.ID = id
	// OrganizationID can not be updated
	tc.OrganizationID = current.OrganizationID
	pErr = s.putTelegrafConfig(ctx, tc)
	if pErr != nil {
		pErr.Op = op
		err = pErr
	}

	return tc, err
}

// DeleteTelegrafConfig removes a telegraf config by ID.
func (s *Service) DeleteTelegrafConfig(ctx context.Context, id platform.ID) error {
	op := OpPrefix + platform.OpDeleteTelegrafConfig
	var err error
	if !id.Valid() {
		return &platform.Error{
			Op:   op,
			Code: platform.EEmptyValue,
			Err:  platform.ErrInvalidID,
		}
	}
	if _, pErr := s.findTelegrafConfigByID(ctx, id); pErr != nil {
		return pErr
	}
	s.telegrafConfigKV.Delete(id)

	err = s.deleteUserResourceMapping(ctx, platform.UserResourceMappingFilter{
		ResourceID:   id,
		ResourceType: platform.TelegrafsResourceType,
	})

	if err != nil {
		return &platform.Error{
			Code: platform.ErrorCode(err),
			Op:   op,
			Err:  err,
		}
	}
	return nil
}
