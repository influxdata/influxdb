package datastore

import (
	"common"
	"parser"
	"protocol"
	"regexp"
	"time"
)

type Datastore interface {
	ExecuteQuery(user common.User, database string,
		query *parser.Query, yield func(*protocol.Series) error,
		ringFilter func(database, series *string, time *int64) bool) error
	LogRequestAndAssignId(request *protocol.Request) error
	// Increment the named integer by the given amount and return the new value
	AtomicIncrement(name string, val int) (uint64, error)
	WriteSeriesData(database string, series *protocol.Series) error
	DropDatabase(database string) error
	DeleteRangeOfSeries(database, series string, startTime, endTime time.Time) error
	DeleteRangeOfRegex(user common.User, database string, regex *regexp.Regexp, startTime, endTime time.Time) error
	Close()
}
