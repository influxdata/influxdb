package client

import (
	"crypto/tls"
	"net/http"
	"net/url"
)

const (
	fluxPath = "/api/v2/query"
)

// Shared transports for all clients to prevent leaking connections
var (
	skipVerifyTransport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	defaultTransport = &http.Transport{}
)

// HTTP implements a Flux query client that makes requests to the /api/v2/query
// API endpoint.
type HTTP struct {
	Addr               string
	Username           string
	Password           string
	InsecureSkipVerify bool
	url                *url.URL
}

// NewHTTP creates a HTTP client
func NewHTTP(u url.URL) (*HTTP, error) {
	return &HTTP{url: &u}, nil
}

// Query runs a flux query against a influx server and decodes the result
func (s *HTTP) Do(hreq *http.Request) (*http.Response, error) {
	if s.Username != "" {
		hreq.SetBasicAuth(s.Username, s.Password)
	}

	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "text/csv")

	hc := newClient(s.url.Scheme, s.InsecureSkipVerify)
	return hc.Do(hreq)
}

func newClient(scheme string, insecure bool) *http.Client {
	hc := &http.Client{
		Transport: defaultTransport,
	}
	if scheme == "https" && insecure {
		hc.Transport = skipVerifyTransport
	}
	return hc
}
