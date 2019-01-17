package authorizer

import (
	"context"

	"github.com/influxdata/influxdb"
)

var _ influxdb.MacroService = (*MacroService)(nil)

// MacroService wraps a influxdb.MacroService and authorizes actions
// against it appropriately.
type MacroService struct {
	s influxdb.MacroService
}

// NewMacroService constructs an instance of an authorizing macro service.
func NewMacroService(s influxdb.MacroService) *MacroService {
	return &MacroService{
		s: s,
	}
}

func newMacroPermission(a influxdb.Action, orgID, id influxdb.ID) (*influxdb.Permission, error) {
	return influxdb.NewPermissionAtID(id, a, influxdb.MacrosResourceType, orgID)
}

func authorizeReadMacro(ctx context.Context, orgID, id influxdb.ID) error {
	p, err := newMacroPermission(influxdb.ReadAction, orgID, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

func authorizeWriteMacro(ctx context.Context, orgID, id influxdb.ID) error {
	p, err := newMacroPermission(influxdb.WriteAction, orgID, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

// FindMacroByID checks to see if the authorizer on context has read access to the id provided.
func (s *MacroService) FindMacroByID(ctx context.Context, id influxdb.ID) (*influxdb.Macro, error) {
	m, err := s.s.FindMacroByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := authorizeReadMacro(ctx, m.OrganizationID, id); err != nil {
		return nil, err
	}

	return m, nil
}

// FindMacros retrieves all macros that match the provided filter and then filters the list down to only the resources that are authorized.
func (s *MacroService) FindMacros(ctx context.Context, filter influxdb.MacroFilter, opt ...influxdb.FindOptions) ([]*influxdb.Macro, error) {
	// TODO: we'll likely want to push this operation into the database since fetching the whole list of data will likely be expensive.
	ms, err := s.s.FindMacros(ctx, filter, opt...)
	if err != nil {
		return nil, err
	}

	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	macros := ms[:0]
	for _, m := range ms {
		err := authorizeReadMacro(ctx, m.OrganizationID, m.ID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, err
		}

		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}

		macros = append(macros, m)
	}

	return macros, nil
}

// CreateMacro checks to see if the authorizer on context has write access to the global macro resource.
func (s *MacroService) CreateMacro(ctx context.Context, m *influxdb.Macro) error {
	p, err := influxdb.NewPermission(influxdb.WriteAction, influxdb.MacrosResourceType, m.OrganizationID)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return s.s.CreateMacro(ctx, m)
}

// UpdateMacro checks to see if the authorizer on context has write access to the macro provided.
func (s *MacroService) UpdateMacro(ctx context.Context, id influxdb.ID, upd *influxdb.MacroUpdate) (*influxdb.Macro, error) {
	m, err := s.FindMacroByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := authorizeWriteMacro(ctx, m.OrganizationID, id); err != nil {
		return nil, err
	}

	return s.s.UpdateMacro(ctx, id, upd)
}

// ReplaceMacro checks to see if the authorizer on context has write access to the macro provided.
func (s *MacroService) ReplaceMacro(ctx context.Context, m *influxdb.Macro) error {
	m, err := s.FindMacroByID(ctx, m.ID)
	if err != nil {
		return err
	}

	if err := authorizeWriteMacro(ctx, m.OrganizationID, m.ID); err != nil {
		return err
	}

	return s.s.ReplaceMacro(ctx, m)
}

// DeleteMacro checks to see if the authorizer on context has write access to the macro provided.
func (s *MacroService) DeleteMacro(ctx context.Context, id influxdb.ID) error {
	m, err := s.FindMacroByID(ctx, id)
	if err != nil {
		return err
	}

	if err := authorizeWriteMacro(ctx, m.OrganizationID, id); err != nil {
		return err
	}

	return s.s.DeleteMacro(ctx, id)
}
