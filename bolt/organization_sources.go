package bolt

import (
	"context"

	"github.com/influxdata/chronograf"
)

type OrganizationSourcesStore struct {
	client *Client
}

func (s *OrganizationSourcesStore) All(ctx context.Context) ([]chronograf.Source, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return nil, err
	}
	ds, err := s.client.SourcesStore.All(ctx)
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

func (s *OrganizationSourcesStore) Add(ctx context.Context, d chronograf.Source) (chronograf.Source, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return chronograf.Source{}, err
	}

	d.Organization = org
	return s.client.SourcesStore.Add(ctx, d)
}

func (s *OrganizationSourcesStore) Delete(ctx context.Context, d chronograf.Source) error {
	d, err := s.client.SourcesStore.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.client.SourcesStore.Delete(ctx, d)
}

func (s *OrganizationSourcesStore) Get(ctx context.Context, id int) (chronograf.Source, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return chronograf.Source{}, err
	}

	d, err := s.client.SourcesStore.Get(ctx, id)
	if err != nil {
		return chronograf.Source{}, err
	}

	if d.Organization != org {
		return chronograf.Source{}, chronograf.ErrSourceNotFound
	}

	return d, nil
}

func (s *OrganizationSourcesStore) Update(ctx context.Context, d chronograf.Source) error {
	_, err := s.client.SourcesStore.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.client.SourcesStore.Update(ctx, d)
}
