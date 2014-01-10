package datastore

import (
	"common"
	"parser"
	"protocol"
)

type Datastore interface {
	ExecuteQuery(user common.User, database string,
		query *parser.SelectQuery, yield func(*protocol.Series) error,
		ringFilter func(database, series *string, time *int64) bool) error
	// Logs the request to a local store and assigns a sequence number that is unique per server id per day
	LogRequestAndAssignSequenceNumber(request *protocol.Request, replicationFactor *uint8, ownerServerId *uint32) error
	CurrentSequenceNumber(clusterVersion *uint32, replicationFactor *uint8, ownerServerId, originatingServerId *uint32) (uint64, error)
	// will replay all requests from a given number. If the number hasn't occured yet today, it replays from yesterday.
	// So this log replay is only meant to work for outages that last less than maybe 12 hours.
	ReplayRequestsFromSequenceNumber(*uint32, *uint32, *uint32, *uint8, *uint64, func(*[]byte) error) error
	// Increment the named integer by the given amount and return the new value
	AtomicIncrement(name string, val int) (uint64, error)
	WriteSeriesData(database string, series *protocol.Series) error
	DeleteSeriesData(database string, query *parser.DeleteQuery) error
	GetSeriesForDatabase(database string, yield func(string) error) error
	DropDatabase(database string) error
	DropSeries(database, series string) error
	Close()
}
