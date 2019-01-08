package mocks

import (
	"context"

	"github.com/influxdata/influxdb/chronograf"
)

var _ chronograf.ServersStore = &ServersStore{}

// ServersStore mock allows all functions to be set for testing
type ServersStore struct {
	AllF    func(context.Context) ([]chronograf.Server, error)
	AddF    func(context.Context, chronograf.Server) (chronograf.Server, error)
	DeleteF func(context.Context, chronograf.Server) error
	GetF    func(ctx context.Context, ID int) (chronograf.Server, error)
	UpdateF func(context.Context, chronograf.Server) error
}

func (s *ServersStore) All(ctx context.Context) ([]chronograf.Server, error) {
	return s.AllF(ctx)
}

func (s *ServersStore) Add(ctx context.Context, srv chronograf.Server) (chronograf.Server, error) {
	return s.AddF(ctx, srv)
}

func (s *ServersStore) Delete(ctx context.Context, srv chronograf.Server) error {
	return s.DeleteF(ctx, srv)
}

func (s *ServersStore) Get(ctx context.Context, id int) (chronograf.Server, error) {
	return s.GetF(ctx, id)
}

func (s *ServersStore) Update(ctx context.Context, srv chronograf.Server) error {
	return s.UpdateF(ctx, srv)
}
