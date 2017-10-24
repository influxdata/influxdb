package mocks

import (
	"context"

	"github.com/influxdata/chronograf"
)

var _ chronograf.LayoutsStore = &LayoutsStore{}

type LayoutsStore struct {
	AddF    func(ctx context.Context, layout chronograf.Layout) (chronograf.Layout, error)
	AllF    func(ctx context.Context) ([]chronograf.Layout, error)
	DeleteF func(ctx context.Context, layout chronograf.Layout) error
	GetF    func(ctx context.Context, id string) (chronograf.Layout, error)
	UpdateF func(ctx context.Context, layout chronograf.Layout) error
}

func (s *LayoutsStore) Add(ctx context.Context, layout chronograf.Layout) (chronograf.Layout, error) {
	return s.AddF(ctx, layout)
}

func (s *LayoutsStore) All(ctx context.Context) ([]chronograf.Layout, error) {
	return s.AllF(ctx)
}

func (s *LayoutsStore) Delete(ctx context.Context, layout chronograf.Layout) error {
	return s.DeleteF(ctx, layout)
}

func (s *LayoutsStore) Get(ctx context.Context, id string) (chronograf.Layout, error) {
	return s.GetF(ctx, id)
}

func (s *LayoutsStore) Update(ctx context.Context, layout chronograf.Layout) error {
	return s.UpdateF(ctx, layout)
}
