package coordinator

import (
	"protocol"
	"query"
)

type Coordinator interface {
	DistributeQuery(query *query.Query, yield func(*protocol.Series) error) error
	WriteSeriesData(series *protocol.Series) error
}
