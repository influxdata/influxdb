package mocks

import (
	"context"

	"github.com/influxdata/platform/chronograf/v2"
)

var _ platform.CellService = &CellService{}

type CellService struct {
	CreateCellF   func(context.Context, *platform.Cell) error
	FindCellByIDF func(context.Context, platform.ID) (*platform.Cell, error)
	FindCellsF    func(context.Context, platform.CellFilter) ([]*platform.Cell, int, error)
	UpdateCellF   func(context.Context, platform.ID, platform.CellUpdate) (*platform.Cell, error)
	DeleteCellF   func(context.Context, platform.ID) error
}

func (s *CellService) FindCellByID(ctx context.Context, id platform.ID) (*platform.Cell, error) {
	return s.FindCellByIDF(ctx, id)
}

func (s *CellService) FindCells(ctx context.Context, filter platform.CellFilter) ([]*platform.Cell, int, error) {
	return s.FindCellsF(ctx, filter)
}

func (s *CellService) CreateCell(ctx context.Context, b *platform.Cell) error {
	return s.CreateCellF(ctx, b)
}

func (s *CellService) UpdateCell(ctx context.Context, id platform.ID, upd platform.CellUpdate) (*platform.Cell, error) {
	return s.UpdateCellF(ctx, id, upd)
}

func (s *CellService) DeleteCell(ctx context.Context, id platform.ID) error {
	return s.DeleteCellF(ctx, id)
}
