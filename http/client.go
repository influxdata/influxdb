package http

import (
	"net/http"
	"net/url"

	"github.com/influxdata/influxdb/kit/tracing"
)

// Service connects to an InfluxDB via HTTP.
type Service struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool

	*AuthorizationService
	*OrganizationService
	*UserService
	*BucketService
	*VariableService
	*DashboardService
}

// NewService returns a service that is an HTTP
// client to a remote
func NewService(addr, token string) *Service {
	return &Service{
		Addr:  addr,
		Token: token,
		AuthorizationService: &AuthorizationService{
			Addr:  addr,
			Token: token,
		},
		OrganizationService: &OrganizationService{
			Addr:  addr,
			Token: token,
		},
		UserService: &UserService{
			Addr:  addr,
			Token: token,
		},
		BucketService: &BucketService{
			Addr:  addr,
			Token: token,
		},
		DashboardService: &DashboardService{
			Addr:  addr,
			Token: token,
		},
		VariableService: &VariableService{
			Addr:  addr,
			Token: token,
		},
	}
}

func newURL(addr, path string) (*url.URL, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	u.Path = path
	return u, nil
}

func newClient(scheme string, insecure bool) *traceClient {
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
	span, _ := tracing.StartSpanFromContext(r.Context())
	tracing.InjectToHTTPRequest(span, r)
	return c.Client.Do(r)
}
