package mrfusion

import (
	"encoding/json"

	"golang.org/x/net/context"
)

// Used to retrieve a Response from a TimeSeries.
type Query string

// Row represents a single row returned from the execution of a single statement in a `Query`.
type Row interface {
	Name() string
	Tags() map[string]string
	Columns() []string
	Values() [][]interface{}
}

// Message represents a user message.
type Message interface {
	Level() string
	Text() string
}

// Result represents a resultset returned from a single statement in a `Query`.
type Result interface {
	Series() ([]Row, error)
	Messages() ([]Message, error)
}

// Response is the result of a query against a TimeSeries
type Response interface {
	Results() ([]Result, error)
	json.Marshaler
}

// Represents a queryable time series database.
type TimeSeries interface {
	// Query retrieves time series data from the database.
	Query(context.Context, Query) (Response, error)
}
