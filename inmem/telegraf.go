package inmem

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/platform"
)

var _ platform.TelegrafConfigStore = new(Service)

// FindTelegrafConfigByID returns a single telegraf config by ID.
func (s *Service) FindTelegrafConfigByID(ctx context.Context, id platform.ID) (tc *platform.TelegrafConfig, err error) {
	op := "inmem/find telegraf config by id"
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
			Msg:  fmt.Sprintf("telegraf config with ID %v not found", id),
		}
	}
	tc := new(platform.TelegrafConfig)
	*tc = result.(platform.TelegrafConfig)
	return tc, nil
}

// FindTelegrafConfig returns the first telegraf config that matches filter.
func (s *Service) FindTelegrafConfig(ctx context.Context, filter platform.UserResourceMappingFilter) (*platform.TelegrafConfig, error) {
	op := "inmem/find telegraf config"
	tcs, n, err := s.FindTelegrafConfigs(ctx, filter)
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

func (s *Service) findTelegrafConfigs(ctx context.Context, filter platform.UserResourceMappingFilter, opt ...platform.FindOptions) ([]*platform.TelegrafConfig, int, *platform.Error) {
	tcs := make([]*platform.TelegrafConfig, 0)
	m, _, err := s.FindUserResourceMappings(ctx, filter)
	if err != nil {
		return nil, 0, &platform.Error{
			Err: err,
		}
	}
	if len(m) == 0 {
		return nil, 0, &platform.Error{
			Code: platform.ENotFound,
		}
	}
	for _, item := range m {
		tc, err := s.findTelegrafConfigByID(ctx, item.ResourceID)
		if err != nil {
			return nil, 0, &platform.Error{
				// return internal error, for any mapping issue
				Err: err,
			}
		}
		tcs = append(tcs, tc)
	}
	if len(tcs) == 0 {
		return nil, 0, &platform.Error{
			Msg: "inconsistent user resource mapping and telegraf config",
		}
	}
	return tcs, len(tcs), nil
}

// FindTelegrafConfigs returns a list of telegraf configs that match filter and the total count of matching telegraf configs.
// Additional options provide pagination & sorting.
func (s *Service) FindTelegrafConfigs(ctx context.Context, filter platform.UserResourceMappingFilter, opt ...platform.FindOptions) (tcs []*platform.TelegrafConfig, n int, err error) {
	op := "inmem/find telegraf configs"
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
	s.telegrafConfigKV.Store(tc.ID, *tc)
	return nil
}

// CreateTelegrafConfig creates a new telegraf config and sets b.ID with the new identifier.
func (s *Service) CreateTelegrafConfig(ctx context.Context, tc *platform.TelegrafConfig, userID platform.ID, now time.Time) error {
	op := "inmem/create telegraf config"
	tc.ID = s.IDGenerator.ID()
	err := s.CreateUserResourceMapping(ctx, &platform.UserResourceMapping{
		ResourceID:   tc.ID,
		UserID:       userID,
		UserType:     platform.Owner,
		ResourceType: platform.TelegrafResourceType,
	})
	if err != nil {
		return err
	}
	tc.Created = now
	tc.LastMod = now
	tc.LastModBy = userID
	pErr := s.putTelegrafConfig(ctx, tc)
	if pErr != nil {
		pErr.Op = op
		err = pErr
	}
	return err

}

// UpdateTelegrafConfig updates a single telegraf config.
// Returns the new telegraf config after update.
func (s *Service) UpdateTelegrafConfig(ctx context.Context, id platform.ID, tc *platform.TelegrafConfig, userID platform.ID, now time.Time) (*platform.TelegrafConfig, error) {
	var err error
	op := "inmem/update telegraf config"
	oldTc, pErr := s.findTelegrafConfigByID(ctx, id)
	if pErr != nil {
		pErr.Op = op
		err = pErr
		return nil, err
	}
	tc.Created = oldTc.Created
	tc.ID = id
	tc.LastMod = now
	tc.LastModBy = userID
	pErr = s.putTelegrafConfig(ctx, tc)
	if pErr != nil {
		pErr.Op = op
		err = pErr
	}

	return tc, err
}

// DeleteTelegrafConfig removes a telegraf config by ID.
func (s *Service) DeleteTelegrafConfig(ctx context.Context, id platform.ID) error {
	op := "inmem/delete telegraf config"
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
		ResourceType: platform.TelegrafResourceType,
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
