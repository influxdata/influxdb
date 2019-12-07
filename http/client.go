package http

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"

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
func NewClient(scheme string, insecure bool) *traceClient {
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
	defer span.Finish()
	tracing.InjectToHTTPRequest(span, r)
	return c.Client.Do(r)
}

// HTTPClient is a basic http client that can make cReqs with out having to juggle
// the token and so forth. It provides sane defaults for checking response
// statuses, sets auth token when provided, and sets the content type to
// application/json for each request. The token, response checker, and
// content type can be overidden on the cReq as well.
type HTTPClient struct {
	addr   url.URL
	token  string
	client *traceClient
}

// NewHTTPClient creates a new HTTPClient(client).
func NewHTTPClient(addr, token string, insecureSkipVerify bool) (*HTTPClient, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}

	return &HTTPClient{
		addr:   *u,
		token:  token,
		client: NewClient(u.Scheme, insecureSkipVerify),
	}, nil
}

func (c *HTTPClient) delete(urlPath string) *cReq {
	return c.newClientReq(http.MethodDelete, urlPath, nil)
}

func (c *HTTPClient) get(urlPath string) *cReq {
	return c.newClientReq(http.MethodGet, urlPath, nil)
}

func (c *HTTPClient) post(urlPath string, body io.Reader) *cReq {
	return c.newClientReq(http.MethodPost, urlPath, body)
}

func (c *HTTPClient) newClientReq(method, urlPath string, body io.Reader) *cReq {
	u := c.addr
	u.Path = path.Join(u.Path, urlPath)
	req, err := http.NewRequest(method, u.String(), body)
	if err != nil {
		return &cReq{err: err}
	}
	if c.token != "" {
		SetToken(c.token, req)
	}

	cr := &cReq{
		client: c.client,
		req:    req,
		respFn: CheckError,
	}
	return cr.ContentType("application/json")
}

type cReq struct {
	client interface {
		Do(*http.Request) (*http.Response, error)
	}
	req    *http.Request
	respFn func(*http.Response) error

	err error
}

func (r *cReq) Header(k, v string) *cReq {
	if r.err != nil {
		return r
	}
	r.req.Header.Add(k, v)
	return r
}

type queryPair struct {
	k, v string
}

func (r *cReq) Queries(pairs ...queryPair) *cReq {
	if r.err != nil || len(pairs) == 0 {
		return r
	}
	params := r.req.URL.Query()
	for _, p := range pairs {
		params.Add(p.k, p.v)
	}
	r.req.URL.RawQuery = params.Encode()
	return r
}

func (r *cReq) ContentType(ct string) *cReq {
	return r.Header("Content-Type", ct)
}

func (r *cReq) DecodeJSON(v interface{}) *cReq {
	return r.RespFn(func(resp *http.Response) error {
		return json.NewDecoder(resp.Body).Decode(v)
	})
}

func (r *cReq) RespFn(fn func(*http.Response) error) *cReq {
	r.respFn = fn
	return r
}

func (r *cReq) Do(ctx context.Context) error {
	if r.err != nil {
		return r.err
	}
	r.req = r.req.WithContext(ctx)

	resp, err := r.client.Do(r.req)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body) // drain body completely
		resp.Body.Close()
	}()

	return r.respFn(resp)
}
