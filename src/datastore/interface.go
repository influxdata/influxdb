package datastore

import (
	"protocol"
	"query"
)

type Datastore interface {
	ExecuteQuery(ringLocation int64, query *query.Query, yield func(*protocol.Series) error) error
	WriteSeriesData(ringLocation int64, series *protocol.Series) error
}
