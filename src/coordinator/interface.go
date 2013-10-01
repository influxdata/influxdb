package coordinator

import (
	"protocol"
)

type Coordinator interface {
	DistributeQuery(query *protocol.Query, yield func(*protocol.Series) error) error
}
