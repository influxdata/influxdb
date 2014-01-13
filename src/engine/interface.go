package engine

import (
	"common"
	"protocol"
)

type EngineI interface {
	RunQuery(user common.User, database string, query string, localOnly bool, yield func(*protocol.Series) error) error
}

// APIs (like HTTP or the binary) will send an object that conforms to this interface
// to coordinator.RunQuery. If the API returns an error when Yield is called, the query
// will be halted.
type QueryResultStream interface {
	Yield(series *protocol.Series) error
	// tells the stream object to flush any buffered series and close out.
	Close()
}

// The protobuf request handler will create a map result stream object that when
// yielded to will marshal the map result and send it over the wire as a response.
// If yield returns an error, the query will be halted.
type MapResultStream interface {
	Yield(mapResult *protocol.MapResult) error
	// tells the stream object to flush any remaining things and close out.
	Close()
}

// Interface for a map job. This will get sent to the datastore, which
// will take a query and a map job and call out to YieldPoint. The map job will yield
// to a MapResultStream object that it has as MapResults are ready.
type MapJob interface {
	// returns true if the query has hit its limit and should be stopped
	YieldPoint(series *string, point *protocol.Point) (stopQuery *bool, err error)
	// tells the map job that no more points will be yielded. It should flush remaining
	// MapResults to the MapResultStream
	Close()
}

// The reduce job knows how to shuffle map results together and will call the reducers.
// Will combine reducer outputs and yield series results to a QueryResultsStream
type ReduceJob interface {
	ShuffleAndReduce(mapResults []*protocol.MapResult) error
	// tell the reduce job that no more map results will be sent
	Close()
	WaitForCompletion() error
}

type Reducer interface {
	// the field values could be nil if there's nothing to return
	Reduce(mapResults *[]protocol.MapResult) ([]*protocol.FieldValue, error)
	// when the stream of MapResults are done, this gets called and remaining results are returned.
	Close() ([]*protocol.FieldValue, error)
	// the field value arrays returned by reduce and close will correspond to this array of column
	// names. The Reduce job should only have to call this once to know which columns a given
	// reducer is returning. That means the reducer should always return field values in the same order.
	ColumnNames() []string
}

type Mapper interface {
	Map(points []*protocol.Point) (*protocol.MapResult, error)
	Close() (*protocol.MapResult, error)
}
