package influxql

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb/query"
)

// Endpoint contains the necessary information to connect to a specific cluster.
type Endpoint struct {
	URL      string `json:"url"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// Service is a client for the influxdb 1.x endpoint that implements the QueryService
// for the influxql compiler type.
type Service struct {
	// Endpoints maps a cluster name to the influxdb 1.x endpoint.
	Endpoints map[string]Endpoint
}

// Query will execute a query for the influxql.Compiler type against an influxdb 1.x endpoint,
// and return results using the default decoder.
func (s *Service) Query(ctx context.Context, req *query.Request) (flux.ResultIterator, error) {
	resp, err := s.query(ctx, req)
	if err != nil {
		return nil, err
	}

	// Decode the response into the JSON structure.
	var results Response
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, err
	}

	// Return a result iterator using the response.
	return NewResponseIterator(&results), nil
}

// QueryRawJSON will execute a query for the influxql.Compiler type against an influxdb 1.x endpoint,
// and return the body of the response as a byte array.
func (s *Service) QueryRawJSON(ctx context.Context, req *query.Request) ([]byte, error) {
	resp, err := s.query(ctx, req)
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(resp.Body)
}

func (s *Service) query(ctx context.Context, req *query.Request) (*http.Response, error) {
	// Verify that this is an influxql query in the compiler.
	compiler, ok := req.Compiler.(*Compiler)
	if !ok {
		return nil, fmt.Errorf("influxql query service does not support the '%s' compiler type", req.Compiler.CompilerType())
	}

	// Lookup the endpoint information for the cluster.
	endpoint, ok := s.Endpoints[compiler.Cluster]
	if !ok {
		return nil, fmt.Errorf("no endpoint found for cluster %s", compiler.Cluster)
	}

	// Prepare the HTTP request.
	u, err := url.Parse(endpoint.URL)
	if err != nil {
		return nil, err
	}
	u.Path += "/query"

	params := url.Values{}
	params.Set("q", compiler.Query)
	if compiler.DB != "" {
		params.Set("db", compiler.DB)
	}
	if compiler.RP != "" {
		params.Set("rp", compiler.RP)
	}
	u.RawQuery = params.Encode()

	hreq, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return nil, err
	}
	hreq = hreq.WithContext(ctx)
	hreq.SetBasicAuth(endpoint.Username, endpoint.Password)

	// Perform the request and look at the status code.
	resp, err := http.DefaultClient.Do(hreq)
	if err != nil {
		return nil, err
	} else if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("unexpected http status: %s", resp.Status)
	}

	return resp, nil
}
