package server

import (
	"context"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/organizations"
)

const organizationKey = "organizationID"

func hasOrganizationContext(ctx context.Context) (string, bool) {
	// prevents panic in case of nil context
	if ctx == nil {
		return "", false
	}
	orgID, ok := ctx.Value(organizationKey).(string)
	// should never happen
	if !ok {
		return "", false
	}
	if orgID == "" {
		return "", false
	}
	return orgID, true
}

const superAdminKey = "superadmin"

func hasSuperAdminContext(ctx context.Context) (bool, bool) {
	// prevents panic in case of nil context
	if ctx == nil {
		return false, false
	}
	sa, ok := ctx.Value(superAdminKey).(bool)
	// should never happen
	if !ok {
		return false, false
	}
	return sa, true
}

// TODO: Comment
// DataSource is ...
// Having this as an interface is useful for testing
type DataStore interface {
	Sources(ctx context.Context) chronograf.SourcesStore
	Servers(ctx context.Context) chronograf.ServersStore
	Layouts(ctx context.Context) chronograf.LayoutsStore
	Users(ctx context.Context) chronograf.UsersStore
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
	if org, ok := hasOrganizationContext(ctx); ok {
		return organizations.NewSourcesStore(s.SourcesStore, org)
	}

	return s.SourcesStore
}

func (s *Store) Servers(ctx context.Context) chronograf.ServersStore {
	if org, ok := hasOrganizationContext(ctx); ok {
		return organizations.NewServersStore(s.ServersStore, org)
	}

	return s.ServersStore
}

func (s *Store) Layouts(ctx context.Context) chronograf.LayoutsStore {
	if org, ok := hasOrganizationContext(ctx); ok {
		return organizations.NewLayoutsStore(s.LayoutsStore, org)
	}

	return s.LayoutsStore
}

func (s *Store) Users(ctx context.Context) chronograf.UsersStore {
	if superAdmin, ok := hasSuperAdminContext(ctx); ok && superAdmin {
		return s.UsersStore
	}
	if org, ok := hasOrganizationContext(ctx); ok {
		return organizations.NewUsersStore(s.UsersStore, org)
	}

	// TODO: eventually have NoOpUserstore
	return organizations.NewUsersStore(s.UsersStore, "")
}

func (s *Store) Dashboards(ctx context.Context) chronograf.DashboardsStore {
	if org, ok := hasOrganizationContext(ctx); ok {
		return organizations.NewDashboardsStore(s.DashboardsStore, org)
	}

	return s.DashboardsStore
}

func (s *Store) Organizations(ctx context.Context) chronograf.OrganizationsStore {
	return s.OrganizationsStore
}
