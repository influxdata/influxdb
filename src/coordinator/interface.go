package coordinator

import (
	"parser"
	"protocol"
)

type Coordinator interface {
	DistributeQuery(query *parser.Query, yield func(*protocol.Series) error) error
	WriteSeriesData(series *protocol.Series) error
}
