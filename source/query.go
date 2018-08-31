package source

import (
	"fmt"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/http"
	"github.com/influxdata/platform/http/influxdb"
	"github.com/influxdata/platform/query"
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
			URL:                s.URL,
			SourceFields:       s.SourceFields,
		}, nil
	case platform.V1SourceType:
		// This can be an influxdb or an influxdb + fluxd.
		// If it is an influxdb alone it is just a source configured with the url of an influxdb
		// and will only accept "influxql" compilers, if it has a fluxd it will also accept "flux" compilers.
		// If the compiler used is "influxql" the proxy request will be made on the platform.URL directly.
		// If the compiler used is "flux" the proxy request will be made on the platform.V1SourceFields.FluxURL
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
