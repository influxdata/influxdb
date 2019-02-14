package authorizer

import (
	"context"

	"github.com/influxdata/influxdb"
)

var _ influxdb.VariableService = (*VariableService)(nil)

// VariableService wraps a influxdb.VariableService and authorizes actions
// against it appropriately.
type VariableService struct {
	s influxdb.VariableService
}

// NewVariableService constructs an instance of an authorizing variable service.
func NewVariableService(s influxdb.VariableService) *VariableService {
	return &VariableService{
		s: s,
	}
}

func newVariablePermission(a influxdb.Action, orgID, id influxdb.ID) (*influxdb.Permission, error) {
	return influxdb.NewPermissionAtID(id, a, influxdb.VariablesResourceType, orgID)
}

func authorizeReadVariable(ctx context.Context, orgID, id influxdb.ID) error {
	p, err := newVariablePermission(influxdb.ReadAction, orgID, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

func authorizeWriteVariable(ctx context.Context, orgID, id influxdb.ID) error {
	p, err := newVariablePermission(influxdb.WriteAction, orgID, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

// FindVariableByID checks to see if the authorizer on context has read access to the id provided.
func (s *VariableService) FindVariableByID(ctx context.Context, id influxdb.ID) (*influxdb.Variable, error) {
	m, err := s.s.FindVariableByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := authorizeReadVariable(ctx, m.OrganizationID, id); err != nil {
		return nil, err
	}

	return m, nil
}

// FindVariables retrieves all variables that match the provided filter and then filters the list down to only the resources that are authorized.
func (s *VariableService) FindVariables(ctx context.Context, filter influxdb.VariableFilter, opt ...influxdb.FindOptions) ([]*influxdb.Variable, error) {
	// TODO: we'll likely want to push this operation into the database since fetching the whole list of data will likely be expensive.
	ms, err := s.s.FindVariables(ctx, filter, opt...)
	if err != nil {
		return nil, err
	}

	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	variables := ms[:0]
	for _, m := range ms {
		err := authorizeReadVariable(ctx, m.OrganizationID, m.ID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, err
		}

		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}

		variables = append(variables, m)
	}

	return variables, nil
}

// CreateVariable checks to see if the authorizer on context has write access to the global variable resource.
func (s *VariableService) CreateVariable(ctx context.Context, m *influxdb.Variable) error {
	p, err := influxdb.NewPermission(influxdb.WriteAction, influxdb.VariablesResourceType, m.OrganizationID)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return s.s.CreateVariable(ctx, m)
}

// UpdateVariable checks to see if the authorizer on context has write access to the variable provided.
func (s *VariableService) UpdateVariable(ctx context.Context, id influxdb.ID, upd *influxdb.VariableUpdate) (*influxdb.Variable, error) {
	m, err := s.FindVariableByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := authorizeWriteVariable(ctx, m.OrganizationID, id); err != nil {
		return nil, err
	}

	return s.s.UpdateVariable(ctx, id, upd)
}

// ReplaceVariable checks to see if the authorizer on context has write access to the variable provided.
func (s *VariableService) ReplaceVariable(ctx context.Context, m *influxdb.Variable) error {
	m, err := s.FindVariableByID(ctx, m.ID)
	if err != nil {
		return err
	}

	if err := authorizeWriteVariable(ctx, m.OrganizationID, m.ID); err != nil {
		return err
	}

	return s.s.ReplaceVariable(ctx, m)
}

// DeleteVariable checks to see if the authorizer on context has write access to the variable provided.
func (s *VariableService) DeleteVariable(ctx context.Context, id influxdb.ID) error {
	m, err := s.FindVariableByID(ctx, id)
	if err != nil {
		return err
	}

	if err := authorizeWriteVariable(ctx, m.OrganizationID, id); err != nil {
		return err
	}

	return s.s.DeleteVariable(ctx, id)
}
