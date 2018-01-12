package mocks

import (
	"context"

	"github.com/influxdata/chronograf"
)

var _ chronograf.TimeSeries = &TimeSeries{}

// TimeSeries is a mockable chronograf time series by overriding the functions.
type TimeSeries struct {
	// Connect will connect to the time series using the information in `Source`.
	ConnectF func(context.Context, *chronograf.Source) error
	// Query retrieves time series data from the database.
	QueryF func(context.Context, chronograf.Query) (chronograf.Response, error)
	// Write records points into the TimeSeries
	WriteF func(context.Context, *chronograf.Point) error
	// UsersStore represents the user accounts within the TimeSeries database
	UsersF func(context.Context) chronograf.UsersStore
	// Permissions returns all valid names permissions in this database
	PermissionsF func(context.Context) chronograf.Permissions
	// RolesF represents the roles. Roles group permissions and Users
	RolesF func(context.Context) (chronograf.RolesStore, error)
}

// New implements TimeSeriesClient
func (t *TimeSeries) New(chronograf.Source, chronograf.Logger) (chronograf.TimeSeries, error) {
	return t, nil
}

// Connect will connect to the time series using the information in `Source`.
func (t *TimeSeries) Connect(ctx context.Context, src *chronograf.Source) error {
	return t.ConnectF(ctx, src)
}

// Query retrieves time series data from the database.
func (t *TimeSeries) Query(ctx context.Context, query chronograf.Query) (chronograf.Response, error) {
	return t.QueryF(ctx, query)
}

// Write records a point into the time series
func (t *TimeSeries) Write(ctx context.Context, point *chronograf.Point) error {
	return t.WriteF(ctx, point)
}

// Users represents the user accounts within the TimeSeries database
func (t *TimeSeries) Users(ctx context.Context) chronograf.UsersStore {
	return t.UsersF(ctx)
}

// Roles represents the roles. Roles group permissions and Users
func (t *TimeSeries) Roles(ctx context.Context) (chronograf.RolesStore, error) {
	return t.RolesF(ctx)
}

// Permissions returns all valid names permissions in this database
func (t *TimeSeries) Permissions(ctx context.Context) chronograf.Permissions {
	return t.PermissionsF(ctx)
}
