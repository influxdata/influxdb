package coordinator

import (
	"parser"
	"protocol"
)

type Coordinator interface {
	// Assumption about the returned data:
	//   1. For any given time series, the points returned are in order
	//   2. If the query involves more than one time series, there is no
	//      guarantee on the order in whic they are returned
	//   3. Data is filtered, i.e. where clause should be assumed to hold true
	//      for all the data points that are returned
	//   4. The end of a time series is signaled by returning a series with no data points
	//   5. TODO: Aggregation on the nodes
	DistributeQuery(db string, query *parser.Query, yield func(*protocol.Series) error) error
	WriteSeriesData(db string, series *protocol.Series) error
	CreateDatabase(db, initialApiKey, requestingApiKey string) error
}
