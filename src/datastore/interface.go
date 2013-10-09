package datastore

import (
	"parser"
	"protocol"
)

type Datastore interface {
	ExecuteQuery(ringLocation int64, query *parser.Query, yield func(*protocol.Series) error) error
	WriteSeriesData(ringLocation int64, series *protocol.Series) error
}
