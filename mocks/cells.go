package mocks

import (
	"context"

	chronograf "github.com/influxdata/chronograf/v2"
)

var _ chronograf.CellService = &CellService{}

type CellService struct {
	CreateCellF   func(context.Context, *chronograf.Cell) error
	FindCellByIDF func(context.Context, chronograf.ID) (*chronograf.Cell, error)
	FindCellsF    func(context.Context, chronograf.CellFilter) ([]*chronograf.Cell, int, error)
	UpdateCellF   func(context.Context, chronograf.ID, chronograf.CellUpdate) (*chronograf.Cell, error)
	DeleteCellF   func(context.Context, chronograf.ID) error
}

func (s *CellService) FindCellByID(ctx context.Context, id chronograf.ID) (*chronograf.Cell, error) {
	return s.FindCellByIDF(ctx, id)
}

func (s *CellService) FindCells(ctx context.Context, filter chronograf.CellFilter) ([]*chronograf.Cell, int, error) {
	return s.FindCellsF(ctx, filter)
}

func (s *CellService) CreateCell(ctx context.Context, b *chronograf.Cell) error {
	return s.CreateCellF(ctx, b)
}

func (s *CellService) UpdateCell(ctx context.Context, id chronograf.ID, upd chronograf.CellUpdate) (*chronograf.Cell, error) {
	return s.UpdateCellF(ctx, id, upd)
}

func (s *CellService) DeleteCell(ctx context.Context, id chronograf.ID) error {
	return s.DeleteCellF(ctx, id)
}
