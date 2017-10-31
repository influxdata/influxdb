package server

import (
	"context"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/organizations"
)

// TODO: Comment
// DataSource is ...
// Having this as an interface is useful for testing
type DataStore interface {
	Sources(ctx context.Context) chronograf.SourcesStore
	Servers(ctx context.Context) chronograf.ServersStore
	Layouts(ctx context.Context) chronograf.LayoutsStore
	Users(ctx context.Context) chronograf.UsersStore
	// TODO: remove
	RawUsers(ctx context.Context) chronograf.UsersStore
	Organizations(ctx context.Context) chronograf.OrganizationsStore
	Dashboards(ctx context.Context) chronograf.DashboardsStore
}

// ensure that Store implements a DataStore
var _ DataStore = &Store{}

// Store is a DataStore
type Store struct {
	SourcesStore       chronograf.SourcesStore
	ServersStore       chronograf.ServersStore
	LayoutsStore       chronograf.LayoutsStore
	UsersStore         chronograf.UsersStore
	DashboardsStore    chronograf.DashboardsStore
	OrganizationsStore chronograf.OrganizationsStore
}

func (s *Store) Sources(ctx context.Context) chronograf.SourcesStore {
	return organizations.NewSourcesStore(s.SourcesStore)
}

func (s *Store) Servers(ctx context.Context) chronograf.ServersStore {
	return organizations.NewServersStore(s.ServersStore)
}

func (s *Store) Layouts(ctx context.Context) chronograf.LayoutsStore {
	return organizations.NewLayoutsStore(s.LayoutsStore)
}

func (s *Store) Users(ctx context.Context) chronograf.UsersStore {
	return organizations.NewUsersStore(s.UsersStore)
}

// TODO: remove me and put logic into Users Call
func (s *Store) RawUsers(ctx context.Context) chronograf.UsersStore {
	return s.UsersStore
}

func (s *Store) Organizations(ctx context.Context) chronograf.OrganizationsStore {
	return s.OrganizationsStore
}

func (s *Store) Dashboards(ctx context.Context) chronograf.DashboardsStore {
	return organizations.NewDashboardsStore(s.DashboardsStore)
}
