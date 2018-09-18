package inmem

import (
	"context"
	"fmt"

	"github.com/influxdata/platform"
	kerrors "github.com/influxdata/platform/kit/errors"
)

// FindMacroByID implements the platform.MacroService interface
func (s *Service) FindMacroByID(ctx context.Context, id platform.ID) (*platform.Macro, error) {
	i, ok := s.macroKV.Load(id.String())
	if !ok {
		return nil, kerrors.Errorf(kerrors.NotFound, "macro with ID %v not found", id)
	}

	macro, ok := i.(*platform.Macro)
	if !ok {
		return nil, fmt.Errorf("type %T is not a macro", i)
	}

	return macro, nil
}

// FindMacros implements the platform.MacroService interface
func (s *Service) FindMacros(ctx context.Context) ([]*platform.Macro, error) {
	var err error
	var macros []*platform.Macro
	s.macroKV.Range(func(k, v interface{}) bool {
		macro, ok := v.(*platform.Macro)
		if !ok {
			err = fmt.Errorf("type %T is not a macro", v)
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
	m.ID = s.IDGenerator.ID()
	return s.ReplaceMacro(ctx, m)
}

// UpdateMacro implements the platform.MacroService interface
func (s *Service) UpdateMacro(ctx context.Context, id platform.ID, update *platform.MacroUpdate) (*platform.Macro, error) {
	macro, err := s.FindMacroByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := update.Apply(macro); err != nil {
		return nil, err
	}

	if err := s.ReplaceMacro(ctx, macro); err != nil {
		return nil, err
	}

	return macro, nil
}

// DeleteMacro implements the platform.MacroService interface
func (s *Service) DeleteMacro(ctx context.Context, id platform.ID) error {
	_, err := s.FindMacroByID(ctx, id)
	if err != nil {
		return err
	}

	s.macroKV.Delete(id.String())

	return nil
}

// ReplaceMacro stores a Macro in the key value store
func (s *Service) ReplaceMacro(ctx context.Context, m *platform.Macro) error {
	s.macroKV.Store(m.ID.String(), m)
	return nil
}
