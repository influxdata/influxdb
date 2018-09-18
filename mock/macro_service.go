package mock

import (
	"context"

	"github.com/influxdata/platform"
)

var _ platform.MacroService = &MacroService{}

type MacroService struct {
	FindMacrosF    func(context.Context) ([]*platform.Macro, error)
	FindMacroByIDF func(context.Context, platform.ID) (*platform.Macro, error)
	CreateMacroF   func(context.Context, *platform.Macro) error
	UpdateMacroF   func(ctx context.Context, id platform.ID, update *platform.MacroUpdate) (*platform.Macro, error)
	ReplaceMacroF  func(context.Context, *platform.Macro) error
	DeleteMacroF   func(context.Context, platform.ID) error
}

func (s *MacroService) CreateMacro(ctx context.Context, macro *platform.Macro) error {
	return s.CreateMacroF(ctx, macro)
}

func (s *MacroService) ReplaceMacro(ctx context.Context, macro *platform.Macro) error {
	return s.ReplaceMacroF(ctx, macro)
}

func (s *MacroService) FindMacros(ctx context.Context) ([]*platform.Macro, error) {
	return s.FindMacrosF(ctx)
}

func (s *MacroService) FindMacroByID(ctx context.Context, id platform.ID) (*platform.Macro, error) {
	return s.FindMacroByIDF(ctx, id)
}

func (s *MacroService) DeleteMacro(ctx context.Context, id platform.ID) error {
	return s.DeleteMacroF(ctx, id)
}

func (s *MacroService) UpdateMacro(ctx context.Context, id platform.ID, update *platform.MacroUpdate) (*platform.Macro, error) {
	return s.UpdateMacroF(ctx, id, update)
}
