package bolt

import (
	"context"

	"github.com/influxdata/chronograf"
)

type OrganizationServersStore struct {
	client *Client
}

func (s *OrganizationServersStore) All(ctx context.Context) ([]chronograf.Server, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return nil, err
	}
	ds, err := s.client.ServersStore.All(ctx)
	if err != nil {
		return nil, err
	}

	dashboards := ds[:0]
	for _, d := range ds {
		if d.Organization == org {
			dashboards = append(dashboards, d)
		}
	}

	return dashboards, nil
}

func (s *OrganizationServersStore) Add(ctx context.Context, d chronograf.Server) (chronograf.Server, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return chronograf.Server{}, err
	}

	d.Organization = org
	return s.client.ServersStore.Add(ctx, d)
}

func (s *OrganizationServersStore) Delete(ctx context.Context, d chronograf.Server) error {
	d, err := s.client.ServersStore.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.client.ServersStore.Delete(ctx, d)
}

func (s *OrganizationServersStore) Get(ctx context.Context, id int) (chronograf.Server, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return chronograf.Server{}, err
	}

	d, err := s.client.ServersStore.Get(ctx, id)
	if err != nil {
		return chronograf.Server{}, err
	}

	if d.Organization != org {
		return chronograf.Server{}, chronograf.ErrServerNotFound
	}

	return d, nil
}

func (s *OrganizationServersStore) Update(ctx context.Context, d chronograf.Server) error {
	_, err := s.client.ServersStore.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.client.ServersStore.Update(ctx, d)
}
