package source

import (
	"fmt"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/http/influxdb"
	"github.com/influxdata/influxdb/query"
)

// NewQueryService creates a bucket service from a source.
func NewQueryService(s *platform.Source) (query.ProxyQueryService, error) {
	switch s.Type {
	case platform.SelfSourceType:
		// TODO(fntlnz): this is supposed to call a query service directly locally,
		// we are letting it err for now since we have some refactoring to do on
		// how services are instantiated
		return nil, fmt.Errorf("self source type not implemented")
	case platform.V2SourceType:
		// This is an influxd that calls another influxd, the query path is /v1/query - in future /v2/query
		// it basically is the same as Self but on an external influxd.
		return &http.SourceProxyQueryService{
			InsecureSkipVerify: s.InsecureSkipVerify,
			Addr:               s.URL,
			SourceFields:       s.SourceFields,
		}, nil
	case platform.V1SourceType:
		// This is an InfluxDB 1.7 source, which supports both InfluxQL and Flux queries
		return &influxdb.SourceProxyQueryService{
			InsecureSkipVerify: s.InsecureSkipVerify,
			URL:                s.URL,
			SourceFields:       s.SourceFields,
			V1SourceFields:     s.V1SourceFields,
			OrganizationID:     s.OrganizationID,
		}, nil
	}
	return nil, fmt.Errorf("unsupported source type %s", s.Type)
}
