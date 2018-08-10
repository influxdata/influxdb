package influxdb

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	nethttp "net/http"
	"net/url"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/chronograf"
	"github.com/influxdata/platform/http"
)

// SourceQuerier connects to Influx via HTTP using tokens to manage buckets
type SourceQuerier struct {
	Source *platform.Source
}

func (s *SourceQuerier) Query(ctx context.Context, q *platform.SourceQuery) (*platform.SourceQueryResult, error) {
	switch q.Type {
	case "influxql":
		return s.influxQuery(ctx, q)
	case "flux":
		return s.fluxQuery(ctx, q)
	}

	return nil, fmt.Errorf("unsupported language %v", q.Type)
}

func (s *SourceQuerier) influxQuery(ctx context.Context, q *platform.SourceQuery) (*platform.SourceQueryResult, error) {
	c, err := newClient(s.Source)
	if err != nil {
		return nil, err
	}

	query := chronograf.Query{
		Command: q.Query,
		// TODO(specify database)
	}

	res, err := c.Query(ctx, query)
	if err != nil {
		return nil, err
	}

	b, err := res.MarshalJSON()
	if err != nil {
		return nil, err
	}

	return &platform.SourceQueryResult{
		Reader: bytes.NewReader(b),
	}, nil
}

func (s *SourceQuerier) fluxQuery(ctx context.Context, q *platform.SourceQuery) (*platform.SourceQueryResult, error) {
	req, err := s.newFluxHTTPRequest(ctx, q)
	if err != nil {
		return nil, err
	}

	hc := newHTTPClient(req.URL.Scheme, s.Source.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}

	if err := http.CheckError(resp); err != nil {
		return nil, err
	}

	return &platform.SourceQueryResult{
		Reader: resp.Body,
	}, nil

}

func newURL(addr, path string) (*url.URL, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	u.Path = path
	return u, nil
}

func (s *SourceQuerier) newFluxHTTPRequest(ctx context.Context, q *platform.SourceQuery) (*nethttp.Request, error) {
	u, err := newURL(s.Source.FluxURL, "/v1/query")
	if err != nil {
		return nil, err
	}
	values := url.Values{}
	values.Set("q", q.Query)
	// TODO(desa): replace this with real value.
	values.Set("orgName", "an org has no name")
	u.RawQuery = values.Encode()

	req, err := nethttp.NewRequest("POST", u.String(), nil)
	if err != nil {
		return nil, err
	}
	// TODO(desa): replace with real token
	req.Header.Set("Authorization", "Token 123")
	req.Header.Set("Accept", "text/csv")

	return req, nil
}

func newHTTPClient(scheme string, insecure bool) *nethttp.Client {
	hc := &nethttp.Client{
		Transport: defaultTransport,
	}
	if scheme == "https" && insecure {
		hc.Transport = skipVerifyTransport
	}

	return hc
}

// Shared transports for all clients to prevent leaking connections
var (
	skipVerifyTransport = &nethttp.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	defaultTransport = &nethttp.Transport{}
)
