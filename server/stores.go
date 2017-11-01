package server

import (
	"context"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/noop"
	"github.com/influxdata/chronograf/organizations"
)

// hasOrganizationContext retrieves organization specified on context
// under the organizations.ContextKey
func hasOrganizationContext(ctx context.Context) (string, bool) {
	// prevents panic in case of nil context
	if ctx == nil {
		return "", false
	}
	orgID, ok := ctx.Value(organizations.ContextKey).(string)
	// should never happen
	if !ok {
		return "", false
	}
	if orgID == "" {
		return "", false
	}
	return orgID, true
}

type superAdminKey string

// SuperAdminKey is the context key for retrieving is the context
// is for a super admin
const SuperAdminKey = superAdminKey("superadmin")

// hasSuperAdminContext speficies if the context contains
// the SuperAdminKey and that the value stored there is true
func hasSuperAdminContext(ctx context.Context) bool {
	// prevents panic in case of nil context
	if ctx == nil {
		return false
	}
	sa, ok := ctx.Value(SuperAdminKey).(bool)
	// should never happen
	if !ok {
		return false
	}
	return sa
}

// DataStore is collection of resources that are used by the Service
// Abstracting this into an interface was useful for isolated testing
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

// Store implements the DataStore interface
type Store struct {
	SourcesStore       chronograf.SourcesStore
	ServersStore       chronograf.ServersStore
	LayoutsStore       chronograf.LayoutsStore
	UsersStore         chronograf.UsersStore
	DashboardsStore    chronograf.DashboardsStore
	OrganizationsStore chronograf.OrganizationsStore
}

// Sources returns a noop.SourcesStore if the context has no organization specified
// and a organization.SourcesStore otherwise.
func (s *Store) Sources(ctx context.Context) chronograf.SourcesStore {
	if org, ok := hasOrganizationContext(ctx); ok {
		return organizations.NewSourcesStore(s.SourcesStore, org)
	}

	return &noop.SourcesStore{}
}

// Servers returns a noop.ServersStore if the context has no organization specified
// and a organization.ServersStore otherwise.
func (s *Store) Servers(ctx context.Context) chronograf.ServersStore {
	if org, ok := hasOrganizationContext(ctx); ok {
		return organizations.NewServersStore(s.ServersStore, org)
	}

	return &noop.ServersStore{}
}

// Layouts returns a noop.LayoutsStore if the context has no organization specified
// and a organization.LayoutsStore otherwise.
func (s *Store) Layouts(ctx context.Context) chronograf.LayoutsStore {
	if org, ok := hasOrganizationContext(ctx); ok {
		return organizations.NewLayoutsStore(s.LayoutsStore, org)
	}

	return &noop.LayoutsStore{}
}

// Users returns a chronograf.UsersStore.
// If the context is a super admin context, then the underlying chronograf.UsersStore
// is returned.
// If there is an organization specified on context, then an organizations.UsersStore
// is returned.
// If niether are specified, a noop.UsersStore is returned.
func (s *Store) Users(ctx context.Context) chronograf.UsersStore {
	if superAdmin := hasSuperAdminContext(ctx); superAdmin {
		return s.UsersStore
	}
	if org, ok := hasOrganizationContext(ctx); ok {
		return organizations.NewUsersStore(s.UsersStore, org)
	}

	return &noop.UsersStore{}
}

// Dashboards returns a noop.DashboardsStore if the context has no organization specified
// and a organization.DashboardsStore otherwise.
func (s *Store) Dashboards(ctx context.Context) chronograf.DashboardsStore {
	if org, ok := hasOrganizationContext(ctx); ok {
		return organizations.NewDashboardsStore(s.DashboardsStore, org)
	}

	return &noop.DashboardsStore{}
}

// Organizations returns the underlying OrganizationsStore.
func (s *Store) Organizations(ctx context.Context) chronograf.OrganizationsStore {
	return s.OrganizationsStore
}
