package mocks

import (
	"context"

	"github.com/influxdata/chronograf"
)

var _ chronograf.TimeSeries = &TimeSeries{}

// TimeSeries is a mockable chronograf time series by overriding the functions.
type TimeSeries struct {
	// Query retrieves time series data from the database.
	QueryF func(context.Context, chronograf.Query) (chronograf.Response, error)
	// Connect will connect to the time series using the information in `Source`.
	ConnectF func(context.Context, *chronograf.Source) error
	// UsersStore represents the user accounts within the TimeSeries database
	UsersF func(context.Context) chronograf.UsersStore
	// Allowances returns all valid names permissions in this database
	AllowancesF func(context.Context) chronograf.Allowances
}

// Query retrieves time series data from the database.
func (t *TimeSeries) Query(ctx context.Context, query chronograf.Query) (chronograf.Response, error) {
	return t.QueryF(ctx, query)
}

// Connect will connect to the time series using the information in `Source`.
func (t *TimeSeries) Connect(ctx context.Context, src *chronograf.Source) error {
	return t.ConnectF(ctx, src)
}

// Users represents the user accounts within the TimeSeries database
func (t *TimeSeries) Users(ctx context.Context) chronograf.UsersStore {
	return t.UsersF(ctx)
}

// Allowances returns all valid names permissions in this database
func (t *TimeSeries) Allowances(ctx context.Context) chronograf.Allowances {
	return t.AllowancesF(ctx)
}
