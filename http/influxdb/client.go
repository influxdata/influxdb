package influxdb

import (
	"crypto/tls"
	"net/http"
	"net/url"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/chronograf"
	"github.com/influxdata/influxdb/chronograf/influx"
	platformhttp "github.com/influxdata/influxdb/http"
)

// Shared transports for all clients to prevent leaking connections
var (
	skipVerifyTransport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	defaultTransport = &http.Transport{}
)

func newClient(s *platform.Source) (*influx.Client, error) {
	c := &influx.Client{}
	url, err := url.Parse(s.URL)
	if err != nil {
		return nil, err
	}
	c.URL = url
	c.Authorizer = DefaultAuthorization(s)
	c.InsecureSkipVerify = s.InsecureSkipVerify
	c.Logger = &chronograf.NoopLogger{}
	return c, nil
}

// DefaultAuthorization creates either a shared JWT builder, basic auth or Noop
// This is copy of the method from chronograf/influx adapted for platform sources.
func DefaultAuthorization(src *platform.Source) influx.Authorizer {
	// Optionally, add the shared secret JWT token creation
	if src.Username != "" && src.SharedSecret != "" {
		return &influx.BearerJWT{
			Username:     src.Username,
			SharedSecret: src.SharedSecret,
		}
	} else if src.Username != "" && src.Password != "" {
		return &influx.BasicAuth{
			Username: src.Username,
			Password: src.Password,
		}
	}
	return &influx.NoAuthorization{}
}

func newURL(addr, path string) (*url.URL, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	u.Path = path
	return u, nil
}

func newTraceClient(scheme string, insecure bool) *traceClient {
	hc := &traceClient{
		Client: http.Client{
			Transport: defaultTransport,
		},
	}
	if scheme == "https" && insecure {
		hc.Transport = skipVerifyTransport
	}

	return hc
}

// traceClient always injects any opentracing trace into the client requests.
type traceClient struct {
	http.Client
}

// Do injects the trace and then performs the request.
func (c *traceClient) Do(r *http.Request) (*http.Response, error) {
	platformhttp.InjectTrace(r)
	return c.Client.Do(r)
}
