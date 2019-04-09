package inmem

import (
	"context"
	"fmt"

	platform "github.com/influxdata/influxdb"
)

func (s *Service) loadVariable(ctx context.Context, id platform.ID) (*platform.Variable, *platform.Error) {
	r, ok := s.variableKV.Load(id.String())
	if !ok {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  platform.ErrVariableNotFound,
		}
	}

	m, ok := r.(*platform.Variable)
	if !ok {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  fmt.Sprintf("type %T is not a variable", r),
		}
	}

	return m, nil
}

// FindVariableByID implements the platform.VariableService interface
func (s *Service) FindVariableByID(ctx context.Context, id platform.ID) (*platform.Variable, error) {
	m, pe := s.loadVariable(ctx, id)
	if pe != nil {
		return nil, &platform.Error{
			Err: pe,
			Op:  OpPrefix + platform.OpFindVariableByID,
		}
	}

	return m, nil
}

func filterVariablesFn(filter platform.VariableFilter) func(m *platform.Variable) bool {
	if filter.ID != nil {
		return func(m *platform.Variable) bool {
			return m.ID == *filter.ID
		}
	}

	if filter.OrganizationID != nil {
		return func(m *platform.Variable) bool {
			return m.OrganizationID == *filter.OrganizationID
		}
	}

	return func(m *platform.Variable) bool { return true }
}

// FindVariables implements the platform.VariableService interface
func (s *Service) FindVariables(ctx context.Context, filter platform.VariableFilter, opt ...platform.FindOptions) ([]*platform.Variable, error) {
	op := OpPrefix + platform.OpFindVariables
	var variables []*platform.Variable

	if filter.ID != nil {
		m, err := s.FindVariableByID(ctx, *filter.ID)
		if err != nil && platform.ErrorCode(err) != platform.ENotFound {
			return variables, &platform.Error{
				Err: err,
				Op:  op,
			}
		}
		if m == nil {
			return variables, nil
		}

		return []*platform.Variable{m}, nil
	}

	filterFn := filterVariablesFn(filter)
	s.variableKV.Range(func(k, v interface{}) bool {
		variable, ok := v.(*platform.Variable)
		if !ok {
			return false
		}
		if filterFn(variable) {
			variables = append(variables, variable)
		}

		return true
	})

	return variables, nil
}

// CreateVariable implements the platform.VariableService interface
func (s *Service) CreateVariable(ctx context.Context, m *platform.Variable) error {
	op := OpPrefix + platform.OpCreateVariable
	m.ID = s.IDGenerator.ID()
	err := s.ReplaceVariable(ctx, m)
	if err != nil {
		return &platform.Error{
			Op:  op,
			Err: err,
		}
	}

	return nil
}

// UpdateVariable implements the platform.VariableService interface
func (s *Service) UpdateVariable(ctx context.Context, id platform.ID, update *platform.VariableUpdate) (*platform.Variable, error) {
	op := OpPrefix + platform.OpUpdateVariable
	variable, err := s.FindVariableByID(ctx, id)
	if err != nil {
		return nil, &platform.Error{
			Op:  op,
			Err: err,
		}
	}

	if err := update.Apply(variable); err != nil {
		return nil, &platform.Error{
			Op:  op,
			Err: err,
		}
	}

	if err := s.ReplaceVariable(ctx, variable); err != nil {
		return nil, &platform.Error{
			Op:  op,
			Err: err,
		}
	}

	return variable, nil
}

// DeleteVariable implements the platform.VariableService interface
func (s *Service) DeleteVariable(ctx context.Context, id platform.ID) error {
	op := OpPrefix + platform.OpDeleteVariable
	_, err := s.FindVariableByID(ctx, id)
	if err != nil {
		return &platform.Error{
			Op:  op,
			Err: err,
		}
	}

	s.variableKV.Delete(id.String())

	return nil
}

// ReplaceVariable stores a Variable in the key value store
func (s *Service) ReplaceVariable(ctx context.Context, m *platform.Variable) error {
	s.variableKV.Store(m.ID.String(), m)
	return nil
}
