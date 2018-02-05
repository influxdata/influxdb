package mocks

import (
	"context"

	"github.com/influxdata/chronograf"
)

// Store is a server.DataStore
type Store struct {
	SourcesStore       chronograf.SourcesStore
	MappingsStore      chronograf.MappingsStore
	ServersStore       chronograf.ServersStore
	LayoutsStore       chronograf.LayoutsStore
	UsersStore         chronograf.UsersStore
	DashboardsStore    chronograf.DashboardsStore
	OrganizationsStore chronograf.OrganizationsStore
	ConfigStore        chronograf.ConfigStore
}

func (s *Store) Sources(ctx context.Context) chronograf.SourcesStore {
	return s.SourcesStore
}

func (s *Store) Servers(ctx context.Context) chronograf.ServersStore {
	return s.ServersStore
}

func (s *Store) Layouts(ctx context.Context) chronograf.LayoutsStore {
	return s.LayoutsStore
}

func (s *Store) Users(ctx context.Context) chronograf.UsersStore {
	return s.UsersStore
}

func (s *Store) Organizations(ctx context.Context) chronograf.OrganizationsStore {
	return s.OrganizationsStore
}
func (s *Store) Mappings(ctx context.Context) chronograf.MappingsStore {
	return s.MappingsStore
}

func (s *Store) Dashboards(ctx context.Context) chronograf.DashboardsStore {
	return s.DashboardsStore
}

func (s *Store) Config(ctx context.Context) chronograf.ConfigStore {
	return s.ConfigStore
}
