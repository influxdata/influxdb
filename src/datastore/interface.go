package datastore

import (
	"parser"
	"protocol"
	"regexp"
	"time"
)

type Datastore interface {
	ExecuteQuery(database string, query *parser.Query, yield func(*protocol.Series) error) error
	WriteSeriesData(database string, series *protocol.Series) error
	DeleteRangeOfSeries(database, series string, startTime, endTime time.Time) error
	DeleteRangeOfRegex(database string, regex regexp.Regexp, startTime, endTime time.Time) error
	Close()
}
