package mocks

import (
	"context"

	"github.com/influxdata/platform/chronograf"
)

type MappingsStore struct {
	AddF    func(context.Context, *chronograf.Mapping) (*chronograf.Mapping, error)
	AllF    func(context.Context) ([]chronograf.Mapping, error)
	DeleteF func(context.Context, *chronograf.Mapping) error
	UpdateF func(context.Context, *chronograf.Mapping) error
	GetF    func(context.Context, string) (*chronograf.Mapping, error)
}

func (s *MappingsStore) Add(ctx context.Context, m *chronograf.Mapping) (*chronograf.Mapping, error) {
	return s.AddF(ctx, m)
}

func (s *MappingsStore) All(ctx context.Context) ([]chronograf.Mapping, error) {
	return s.AllF(ctx)
}

func (s *MappingsStore) Delete(ctx context.Context, m *chronograf.Mapping) error {
	return s.DeleteF(ctx, m)
}

func (s *MappingsStore) Get(ctx context.Context, id string) (*chronograf.Mapping, error) {
	return s.GetF(ctx, id)
}

func (s *MappingsStore) Update(ctx context.Context, m *chronograf.Mapping) error {
	return s.UpdateF(ctx, m)
}
