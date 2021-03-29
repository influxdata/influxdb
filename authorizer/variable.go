package authorizer

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
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

// FindVariableByID checks to see if the authorizer on context has read access to the id provided.
func (s *VariableService) FindVariableByID(ctx context.Context, id platform.ID) (*influxdb.Variable, error) {
	v, err := s.s.FindVariableByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeRead(ctx, influxdb.VariablesResourceType, v.ID, v.OrganizationID); err != nil {
		return nil, err
	}
	return v, nil
}

// FindVariables retrieves all variables that match the provided filter and then filters the list down to only the resources that are authorized.
func (s *VariableService) FindVariables(ctx context.Context, filter influxdb.VariableFilter, opt ...influxdb.FindOptions) ([]*influxdb.Variable, error) {
	// TODO: we'll likely want to push this operation into the database since fetching the whole list of data will likely be expensive.
	vs, err := s.s.FindVariables(ctx, filter, opt...)
	if err != nil {
		return nil, err
	}
	vs, _, err = AuthorizeFindVariables(ctx, vs)
	return vs, err
}

// CreateVariable checks to see if the authorizer on context has write access to the global variable resource.
func (s *VariableService) CreateVariable(ctx context.Context, v *influxdb.Variable) error {
	if _, _, err := AuthorizeCreate(ctx, influxdb.VariablesResourceType, v.OrganizationID); err != nil {
		return err
	}
	return s.s.CreateVariable(ctx, v)
}

// UpdateVariable checks to see if the authorizer on context has write access to the variable provided.
func (s *VariableService) UpdateVariable(ctx context.Context, id platform.ID, upd *influxdb.VariableUpdate) (*influxdb.Variable, error) {
	v, err := s.FindVariableByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.VariablesResourceType, v.ID, v.OrganizationID); err != nil {
		return nil, err
	}
	return s.s.UpdateVariable(ctx, id, upd)
}

// ReplaceVariable checks to see if the authorizer on context has write access to the variable provided.
func (s *VariableService) ReplaceVariable(ctx context.Context, m *influxdb.Variable) error {
	v, err := s.FindVariableByID(ctx, m.ID)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.VariablesResourceType, v.ID, v.OrganizationID); err != nil {
		return err
	}
	return s.s.ReplaceVariable(ctx, m)
}

// DeleteVariable checks to see if the authorizer on context has write access to the variable provided.
func (s *VariableService) DeleteVariable(ctx context.Context, id platform.ID) error {
	v, err := s.FindVariableByID(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.VariablesResourceType, v.ID, v.OrganizationID); err != nil {
		return err
	}
	return s.s.DeleteVariable(ctx, id)
}
