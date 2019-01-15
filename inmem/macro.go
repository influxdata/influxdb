package inmem

import (
	"context"
	"fmt"

	platform "github.com/influxdata/influxdb"
)

func (s *Service) loadMacro(ctx context.Context, id platform.ID) (*platform.Macro, *platform.Error) {
	r, ok := s.macroKV.Load(id.String())
	if !ok {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  platform.ErrMacroNotFound,
		}
	}

	m, ok := r.(*platform.Macro)
	if !ok {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  fmt.Sprintf("type %T is not a macro", r),
		}
	}

	return m, nil
}

// FindMacroByID implements the platform.MacroService interface
func (s *Service) FindMacroByID(ctx context.Context, id platform.ID) (*platform.Macro, error) {
	m, pe := s.loadMacro(ctx, id)
	if pe != nil {
		return nil, &platform.Error{
			Err: pe,
			Op:  OpPrefix + platform.OpFindMacroByID,
		}
	}

	return m, nil
}

func filterMacrosFn(filter platform.MacroFilter) func(m *platform.Macro) bool {
	if filter.ID != nil {
		return func(m *platform.Macro) bool {
			return m.ID == *filter.ID
		}
	}

	if filter.OrganizationID != nil {
		return func(m *platform.Macro) bool {
			return m.OrganizationID == *filter.OrganizationID
		}
	}

	return func(m *platform.Macro) bool { return true }
}

// FindMacros implements the platform.MacroService interface
func (s *Service) FindMacros(ctx context.Context, filter platform.MacroFilter, opt ...platform.FindOptions) ([]*platform.Macro, error) {
	op := OpPrefix + platform.OpFindMacros
	var macros []*platform.Macro

	if filter.ID != nil {
		m, err := s.FindMacroByID(ctx, *filter.ID)
		if err != nil && platform.ErrorCode(err) != platform.ENotFound {
			return macros, &platform.Error{
				Err: err,
				Op:  op,
			}
		}
		if m == nil {
			return macros, nil
		}

		return []*platform.Macro{m}, nil
	}

	filterFn := filterMacrosFn(filter)
	s.macroKV.Range(func(k, v interface{}) bool {
		macro, ok := v.(*platform.Macro)
		if !ok {
			return false
		}
		if filterFn(macro) {
			macros = append(macros, macro)
		}

		return true
	})

	return macros, nil
}

// CreateMacro implements the platform.MacroService interface
func (s *Service) CreateMacro(ctx context.Context, m *platform.Macro) error {
	op := OpPrefix + platform.OpCreateMacro
	m.ID = s.IDGenerator.ID()
	err := s.ReplaceMacro(ctx, m)
	if err != nil {
		return &platform.Error{
			Op:  op,
			Err: err,
		}
	}

	return nil
}

// UpdateMacro implements the platform.MacroService interface
func (s *Service) UpdateMacro(ctx context.Context, id platform.ID, update *platform.MacroUpdate) (*platform.Macro, error) {
	op := OpPrefix + platform.OpUpdateMacro
	macro, err := s.FindMacroByID(ctx, id)
	if err != nil {
		return nil, &platform.Error{
			Op:  op,
			Err: err,
		}
	}

	if err := update.Apply(macro); err != nil {
		return nil, &platform.Error{
			Op:  op,
			Err: err,
		}
	}

	if err := s.ReplaceMacro(ctx, macro); err != nil {
		return nil, &platform.Error{
			Op:  op,
			Err: err,
		}
	}

	return macro, nil
}

// DeleteMacro implements the platform.MacroService interface
func (s *Service) DeleteMacro(ctx context.Context, id platform.ID) error {
	op := OpPrefix + platform.OpDeleteMacro
	_, err := s.FindMacroByID(ctx, id)
	if err != nil {
		return &platform.Error{
			Op:  op,
			Err: err,
		}
	}

	s.macroKV.Delete(id.String())

	return nil
}

// ReplaceMacro stores a Macro in the key value store
func (s *Service) ReplaceMacro(ctx context.Context, m *platform.Macro) error {
	s.macroKV.Store(m.ID.String(), m)
	return nil
}
