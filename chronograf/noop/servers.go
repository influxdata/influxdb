package noop

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/chronograf"
)

// ensure ServersStore implements chronograf.ServersStore
var _ chronograf.ServersStore = &ServersStore{}

type ServersStore struct{}

func (s *ServersStore) All(context.Context) ([]chronograf.Server, error) {
	return nil, fmt.Errorf("no servers found")
}

func (s *ServersStore) Add(context.Context, chronograf.Server) (chronograf.Server, error) {
	return chronograf.Server{}, fmt.Errorf("failed to add server")
}

func (s *ServersStore) Delete(context.Context, chronograf.Server) error {
	return fmt.Errorf("failed to delete server")
}

func (s *ServersStore) Get(ctx context.Context, ID int) (chronograf.Server, error) {
	return chronograf.Server{}, chronograf.ErrServerNotFound
}

func (s *ServersStore) Update(context.Context, chronograf.Server) error {
	return fmt.Errorf("failed to update server")
}
