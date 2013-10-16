package datastore

import (
	"parser"
	"protocol"
)

type Datastore interface {
	ExecuteQuery(database string, query *parser.Query, yield func(*protocol.Series) error) error
	WriteSeriesData(database string, series *protocol.Series) error
	Close()
}
