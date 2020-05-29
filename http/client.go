package http

import (
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/influxdata/influxdb/v2/dbrp"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
)

// NewHTTPClient creates a new httpc.Client type. This call sets all
// the options that are important to the http pkg on the httpc client.
// The default status fn and so forth will all be set for the caller.
// In addition, some options can be specified. Those will be added to the defaults.
func NewHTTPClient(addr, token string, insecureSkipVerify bool, opts ...httpc.ClientOptFn) (*httpc.Client, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}

	defaultOpts := []httpc.ClientOptFn{
		httpc.WithAddr(addr),
		httpc.WithContentType("application/json"),
		httpc.WithHTTPClient(NewClient(u.Scheme, insecureSkipVerify)),
		httpc.WithInsecureSkipVerify(insecureSkipVerify),
		httpc.WithStatusFn(CheckError),
	}
	if token != "" {
		defaultOpts = append(defaultOpts, httpc.WithAuthToken(token))
	}
	opts = append(defaultOpts, opts...)
	return httpc.New(opts...)
}

// Service connects to an InfluxDB via HTTP.
type Service struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool

	*AuthorizationService
	*BackupService
	*BucketService
	*TaskService
	*DashboardService
	*OrganizationService
	*NotificationRuleService
	*UserService
	*VariableService
	*WriteService
	DocumentService
	*CheckService
	*NotificationEndpointService
	*UserResourceMappingService
	*TelegrafService
	*LabelService
	*SecretService
	DBRPMappingServiceV2 *dbrp.Client
}

// NewService returns a service that is an HTTP client to a remote.
// Address and token are needed for those services that do not use httpc.Client,
// but use those for configuring.
// Usually one would do:
//
// ```
// c := NewHTTPClient(addr, token, insecureSkipVerify)
// s := NewService(c, addr token)
// ```
//
// So one should provide the same `addr` and `token` to both calls to ensure consistency
// in the behavior of the returned service.
func NewService(httpClient *httpc.Client, addr, token string) (*Service, error) {
	return &Service{
		Addr:                 addr,
		Token:                token,
		AuthorizationService: &AuthorizationService{Client: httpClient},
		BackupService: &BackupService{
			Addr:  addr,
			Token: token,
		},
		BucketService:           &BucketService{Client: httpClient},
		TaskService:             &TaskService{Client: httpClient},
		DashboardService:        &DashboardService{Client: httpClient},
		OrganizationService:     &OrganizationService{Client: httpClient},
		NotificationRuleService: &NotificationRuleService{Client: httpClient},
		UserService:             &UserService{Client: httpClient},
		VariableService:         &VariableService{Client: httpClient},
		WriteService: &WriteService{
			Addr:  addr,
			Token: token,
		},
		DocumentService:             NewDocumentService(httpClient),
		CheckService:                &CheckService{Client: httpClient},
		NotificationEndpointService: &NotificationEndpointService{Client: httpClient},
		UserResourceMappingService:  &UserResourceMappingService{Client: httpClient},
		TelegrafService:             NewTelegrafService(httpClient),
		LabelService:                &LabelService{Client: httpClient},
		SecretService:               &SecretService{Client: httpClient},
		DBRPMappingServiceV2:        dbrp.NewClient(httpClient),
	}, nil
}

// NewURL concats addr and path.
func NewURL(addr, path string) (*url.URL, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	u.Path = path
	return u, nil
}

// NewClient returns an http.Client that pools connections and injects a span.
func NewClient(scheme string, insecure bool) *http.Client {
	return httpClient(scheme, insecure)
}

// SpanTransport injects the http.RoundTripper.RoundTrip() request
// with a span.
type SpanTransport struct {
	base http.RoundTripper
}

// RoundTrip implements the http.RoundTripper, intercepting the base
// round trippers call and injecting a span.
func (s *SpanTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	span, _ := tracing.StartSpanFromContext(r.Context())
	defer span.Finish()
	tracing.InjectToHTTPRequest(span, r)
	return s.base.RoundTrip(r)
}

func httpClient(scheme string, insecure bool) *http.Client {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if scheme == "https" && insecure {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return &http.Client{
		Transport: &SpanTransport{
			base: tr,
		},
	}
}
