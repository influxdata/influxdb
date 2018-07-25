package organizations

import (
	"context"

	"github.com/influxdata/platform/chronograf"
)

// ensure that ServersStore implements chronograf.ServerStore
var _ chronograf.ServersStore = &ServersStore{}

// ServersStore facade on a ServerStore that filters servers
// by organization.
type ServersStore struct {
	store        chronograf.ServersStore
	organization string
}

// NewServersStore creates a new ServersStore from an existing
// chronograf.ServerStore and an organization string
func NewServersStore(s chronograf.ServersStore, org string) *ServersStore {
	return &ServersStore{
		store:        s,
		organization: org,
	}
}

// All retrieves all servers from the underlying ServerStore and filters them
// by organization.
func (s *ServersStore) All(ctx context.Context) ([]chronograf.Server, error) {
	err := validOrganization(ctx)
	if err != nil {
		return nil, err
	}
	ds, err := s.store.All(ctx)
	if err != nil {
		return nil, err
	}

	// This filters servers without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	servers := ds[:0]
	for _, d := range ds {
		if d.Organization == s.organization {
			servers = append(servers, d)
		}
	}

	return servers, nil
}

// Add creates a new Server in the ServersStore with server.Organization set to be the
// organization from the server store.
func (s *ServersStore) Add(ctx context.Context, d chronograf.Server) (chronograf.Server, error) {
	err := validOrganization(ctx)
	if err != nil {
		return chronograf.Server{}, err
	}

	d.Organization = s.organization
	return s.store.Add(ctx, d)
}

// Delete the server from ServersStore
func (s *ServersStore) Delete(ctx context.Context, d chronograf.Server) error {
	err := validOrganization(ctx)
	if err != nil {
		return err
	}

	d, err = s.store.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.store.Delete(ctx, d)
}

// Get returns a Server if the id exists and belongs to the organization that is set.
func (s *ServersStore) Get(ctx context.Context, id int) (chronograf.Server, error) {
	err := validOrganization(ctx)
	if err != nil {
		return chronograf.Server{}, err
	}

	d, err := s.store.Get(ctx, id)
	if err != nil {
		return chronograf.Server{}, err
	}

	if d.Organization != s.organization {
		return chronograf.Server{}, chronograf.ErrServerNotFound
	}

	return d, nil
}

// Update the server in ServersStore.
func (s *ServersStore) Update(ctx context.Context, d chronograf.Server) error {
	err := validOrganization(ctx)
	if err != nil {
		return err
	}

	_, err = s.store.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.store.Update(ctx, d)
}
