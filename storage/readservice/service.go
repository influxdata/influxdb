package readservice

import (
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/control"
)

// NewProxyQueryService returns a proxy query service based on the given queryController
// suitable for the storage read service.
func NewProxyQueryService(queryController *control.Controller) query.ProxyQueryService {
	return query.ProxyQueryServiceAsyncBridge{
		AsyncQueryService: queryController,
	}
}
