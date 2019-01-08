package server

import (
	"github.com/influxdata/influxdb/chronograf"
	"github.com/influxdata/influxdb/chronograf/canned"
	"github.com/influxdata/influxdb/chronograf/filestore"
	"github.com/influxdata/influxdb/chronograf/memdb"
	"github.com/influxdata/influxdb/chronograf/multistore"
)

// LayoutBuilder is responsible for building Layouts
type LayoutBuilder interface {
	Build(chronograf.LayoutsStore) (*multistore.Layouts, error)
}

// MultiLayoutBuilder implements LayoutBuilder and will return a Layouts
type MultiLayoutBuilder struct {
	Logger     chronograf.Logger
	UUID       chronograf.ID
	CannedPath string
}

// Build will construct a Layouts of canned and db-backed personalized
// layouts
func (builder *MultiLayoutBuilder) Build(db chronograf.LayoutsStore) (*multistore.Layouts, error) {
	// These apps are those handled from a directory
	apps := filestore.NewApps(builder.CannedPath, builder.UUID, builder.Logger)
	// These apps are statically compiled into chronograf
	binApps := &canned.BinLayoutsStore{
		Logger: builder.Logger,
	}
	// Acts as a front-end to both the bolt layouts, filesystem layouts and binary statically compiled layouts.
	// The idea here is that these stores form a hierarchy in which each is tried sequentially until
	// the operation has success.  So, the database is preferred over filesystem over binary data.
	layouts := &multistore.Layouts{
		Stores: []chronograf.LayoutsStore{
			db,
			apps,
			binApps,
		},
	}

	return layouts, nil
}

// DashboardBuilder is responsible for building dashboards
type DashboardBuilder interface {
	Build(chronograf.DashboardsStore) (*multistore.DashboardsStore, error)
}

// MultiDashboardBuilder builds a DashboardsStore backed by bolt and the filesystem
type MultiDashboardBuilder struct {
	Logger chronograf.Logger
	ID     chronograf.ID
	Path   string
}

// Build will construct a Dashboard store of filesystem and db-backed dashboards
func (builder *MultiDashboardBuilder) Build(db chronograf.DashboardsStore) (*multistore.DashboardsStore, error) {
	// These dashboards are those handled from a directory
	files := filestore.NewDashboards(builder.Path, builder.ID, builder.Logger)
	// Acts as a front-end to both the bolt dashboard and filesystem dashboards.
	// The idea here is that these stores form a hierarchy in which each is tried sequentially until
	// the operation has success.  So, the database is preferred over filesystem
	dashboards := &multistore.DashboardsStore{
		Stores: []chronograf.DashboardsStore{
			db,
			files,
		},
	}

	return dashboards, nil
}

// SourcesBuilder builds a MultiSourceStore
type SourcesBuilder interface {
	Build(chronograf.SourcesStore) (*multistore.SourcesStore, error)
}

// MultiSourceBuilder implements SourcesBuilder
type MultiSourceBuilder struct {
	InfluxDBURL      string
	InfluxDBUsername string
	InfluxDBPassword string

	Logger chronograf.Logger
	ID     chronograf.ID
	Path   string
}

// Build will return a MultiSourceStore
func (fs *MultiSourceBuilder) Build(db chronograf.SourcesStore) (*multistore.SourcesStore, error) {
	// These dashboards are those handled from a directory
	files := filestore.NewSources(fs.Path, fs.ID, fs.Logger)

	stores := []chronograf.SourcesStore{db, files}

	if fs.InfluxDBURL != "" {
		influxStore := &memdb.SourcesStore{
			Source: &chronograf.Source{
				ID:       0,
				Name:     fs.InfluxDBURL,
				Type:     chronograf.InfluxDB,
				Username: fs.InfluxDBUsername,
				Password: fs.InfluxDBPassword,
				URL:      fs.InfluxDBURL,
				Default:  true,
			}}
		stores = append([]chronograf.SourcesStore{influxStore}, stores...)
	}
	sources := &multistore.SourcesStore{
		Stores: stores,
	}

	return sources, nil
}

// KapacitorBuilder builds a KapacitorStore
type KapacitorBuilder interface {
	Build(chronograf.ServersStore) (*multistore.KapacitorStore, error)
}

// MultiKapacitorBuilder implements KapacitorBuilder
type MultiKapacitorBuilder struct {
	KapacitorURL      string
	KapacitorUsername string
	KapacitorPassword string

	Logger chronograf.Logger
	ID     chronograf.ID
	Path   string
}

// Build will return a multistore facade KapacitorStore over memdb and bolt
func (builder *MultiKapacitorBuilder) Build(db chronograf.ServersStore) (*multistore.KapacitorStore, error) {
	// These dashboards are those handled from a directory
	files := filestore.NewKapacitors(builder.Path, builder.ID, builder.Logger)

	stores := []chronograf.ServersStore{db, files}

	if builder.KapacitorURL != "" {
		memStore := &memdb.KapacitorStore{
			Kapacitor: &chronograf.Server{
				ID:       0,
				SrcID:    0,
				Name:     builder.KapacitorURL,
				URL:      builder.KapacitorURL,
				Username: builder.KapacitorUsername,
				Password: builder.KapacitorPassword,
			},
		}
		stores = append([]chronograf.ServersStore{memStore}, stores...)
	}
	kapacitors := &multistore.KapacitorStore{
		Stores: stores,
	}
	return kapacitors, nil
}

// OrganizationBuilder is responsible for building dashboards
type OrganizationBuilder interface {
	Build(chronograf.OrganizationsStore) (*multistore.OrganizationsStore, error)
}

// MultiOrganizationBuilder builds a OrganizationsStore backed by bolt and the filesystem
type MultiOrganizationBuilder struct {
	Logger chronograf.Logger
	Path   string
}

// Build will construct a Organization store of filesystem and db-backed dashboards
func (builder *MultiOrganizationBuilder) Build(db chronograf.OrganizationsStore) (*multistore.OrganizationsStore, error) {
	// These organization are those handled from a directory
	files := filestore.NewOrganizations(builder.Path, builder.Logger)
	// Acts as a front-end to both the bolt org and filesystem orgs.
	// The idea here is that these stores form a hierarchy in which each is tried sequentially until
	// the operation has success.  So, the database is preferred over filesystem
	orgs := &multistore.OrganizationsStore{
		Stores: []chronograf.OrganizationsStore{
			db,
			files,
		},
	}

	return orgs, nil
}
