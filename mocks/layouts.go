package mocks

import (
	"context"

	"github.com/influxdata/chronograf"
)

var _ chronograf.LayoutStore = &LayoutStore{}

type LayoutStore struct {
	AddF    func(ctx context.Context, layout chronograf.Layout) (chronograf.Layout, error)
	AllF    func(ctx context.Context) ([]chronograf.Layout, error)
	DeleteF func(ctx context.Context, layout chronograf.Layout) error
	GetF    func(ctx context.Context, id string) (chronograf.Layout, error)
	UpdateF func(ctx context.Context, layout chronograf.Layout) error
}

func (s *LayoutStore) Add(ctx context.Context, layout chronograf.Layout) (chronograf.Layout, error) {
	return s.AddF(ctx, layout)
}

func (s *LayoutStore) All(ctx context.Context) ([]chronograf.Layout, error) {
	return s.AllF(ctx)
}

func (s *LayoutStore) Delete(ctx context.Context, layout chronograf.Layout) error {
	return s.DeleteF(ctx, layout)
}

func (s *LayoutStore) Get(ctx context.Context, id string) (chronograf.Layout, error) {
	return s.GetF(ctx, id)
}

func (s *LayoutStore) Update(ctx context.Context, layout chronograf.Layout) error {
	return s.UpdateF(ctx, layout)
}
