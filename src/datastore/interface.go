package datastore

import (
	"parser"
	"protocol"
	"regexp"
)

type Datastore interface {
	ExecuteQuery(database string, query *parser.Query, yield func(*protocol.Series) error) error
	WriteSeriesData(database string, series *protocol.Series) error
	DeleteRangeOfSeries(database, series string, startTime, endTime int64) error
	DeleteRangeOfRegex(database string, regex *regexp.Regexp, startTime, endTime int64) error
	Close()
}
