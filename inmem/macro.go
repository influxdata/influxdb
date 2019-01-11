package inmem

import (
	"context"
	"fmt"

	platform "github.com/influxdata/influxdb"
)

// FindMacroByID implements the platform.MacroService interface
func (s *Service) FindMacroByID(ctx context.Context, id platform.ID) (*platform.Macro, error) {
	op := OpPrefix + platform.OpFindMacroByID
	i, ok := s.macroKV.Load(id.String())
	if !ok {
		return nil, &platform.Error{
			Op:   op,
			Code: platform.ENotFound,
			Msg:  "macro not found",
		}
	}

	macro, ok := i.(*platform.Macro)
	if !ok {
		return nil, &platform.Error{
			Op:  op,
			Msg: fmt.Sprintf("type %T is not a macro", i),
		}
	}

	return macro, nil
}

// FindMacros implements the platform.MacroService interface
func (s *Service) FindMacros(ctx context.Context) ([]*platform.Macro, error) {
	op := OpPrefix + platform.OpFindMacros
	var err error
	var macros []*platform.Macro
	s.macroKV.Range(func(k, v interface{}) bool {
		macro, ok := v.(*platform.Macro)
		if !ok {
			err = &platform.Error{
				Op:  op,
				Msg: fmt.Sprintf("type %T is not a macro", v),
			}
			return false
		}

		macros = append(macros, macro)
		return true
	})

	if err != nil {
		return nil, err
	}

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
			Op:   op,
			Code: platform.ENotFound,
			Msg:  "macro not found",
			Err:  err,
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
			Op:   op,
			Code: platform.ENotFound,
			Msg:  "macro not found",
			Err:  err,
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
