package internal

import (
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
)

// AuthorizerMock is a mockable implementation of a query.Authorizer.
type AuthorizerMock struct {
	AuthorizeDatabaseFn    func(influxql.Privilege, string) bool
	AuthorizeQueryFn       func(database string, query *influxql.Query) error
	AuthorizeSeriesReadFn  func(database string, measurement []byte, tags models.Tags) bool
	AuthorizeSeriesWriteFn func(database string, measurement []byte, tags models.Tags) bool
}

// AuthorizeDatabase determines if the provided privilege is sufficient to
// authorise access to the database.
func (a *AuthorizerMock) AuthorizeDatabase(p influxql.Privilege, name string) bool {
	return a.AuthorizeDatabaseFn(p, name)
}

// AuthorizeQuery determins if the query can be executed against the provided
// database.
func (a *AuthorizerMock) AuthorizeQuery(database string, query *influxql.Query) error {
	return a.AuthorizeQueryFn(database, query)
}

// AuthorizeSeriesRead determines if the series comprising measurement and tags
// can be read on the provided database.
func (a *AuthorizerMock) AuthorizeSeriesRead(database string, measurement []byte, tags models.Tags) bool {
	return a.AuthorizeSeriesReadFn(database, measurement, tags)
}

// AuthorizeSeriesWrite determines if the series comprising measurement and tags
// can be written to, on the provided database.
func (a *AuthorizerMock) AuthorizeSeriesWrite(database string, measurement []byte, tags models.Tags) bool {
	return a.AuthorizeSeriesWriteFn(database, measurement, tags)
}
