package mrfusion

import "golang.org/x/net/context"

// Used to retrieve a Response from a TimeSeries.
type Query string

// Row represents a single row returned from the execution of a single statement in a `Query`.
type Row interface {
	Name() string
	Tags() map[string]string
	Columns() []string
	Values() [][]interface{}
}

// Result represents a resultset returned from a single statement in a `Query`.
type Result interface {
	Series() ([]Row, error)
}

// Response is the result of a query against a TimeSeries
type Response interface {
	Results() ([]Result, error)
}

// MonitoredService is a service sending monitoring data to a `TimeSeries`
type MonitoredService struct {
	TagKey   string `json:"tagKey"`
	TagValue string `json:"tagValue"`
	Type     string `json:"type"`
}

// Represents a queryable time series database.
type TimeSeries interface {
	// Query retrieves time series data from the database.
	Query(context.Context, Query) (Response, error)
	// MonitoredServices retrieves all services sending monitoring data to this `TimeSeries`
	MonitoredServices(context.Context) ([]MonitoredService, error)
}
