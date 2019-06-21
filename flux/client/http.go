package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/repl"
	iclient "github.com/influxdata/influxdb/client"
	"github.com/pkg/errors"
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
func NewHTTP(host string, port int, ssl bool) (*HTTP, error) {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	u, e := iclient.ParseConnectionString(addr, ssl)
	if e != nil {
		return nil, e
	}
	u.Path = fluxPath
	return &HTTP{url: &u}, nil
}

// Query runs a flux query against a influx server and decodes the result
func (s *HTTP) Query(ctx context.Context, r *ProxyRequest) (flux.ResultIterator, error) {
	qreq, err := QueryRequestFromProxyRequest(r)
	if err != nil {
		return nil, err
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(qreq); err != nil {
		return nil, err
	}

	hreq, err := http.NewRequest("POST", s.url.String(), &body)
	if err != nil {
		return nil, err
	}
	if s.Username != "" {
		hreq.SetBasicAuth(s.Username, s.Password)
	}

	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "text/csv")
	hreq = hreq.WithContext(ctx)

	hc := newClient(s.url.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return nil, err
	}

	if err := checkError(resp); err != nil {
		return nil, err
	}

	decoder := csv.NewMultiResultDecoder(csv.ResultDecoderConfig{})
	return decoder.Decode(resp.Body)
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

// CheckError reads the http.Response and returns an error if one exists.
// It will automatically recognize the errors returned by Influx services
// and decode the error into an internal error type. If the error cannot
// be determined in that way, it will create a generic error message.
//
// If there is no error, then this returns nil.
func checkError(resp *http.Response) error {
	switch resp.StatusCode / 100 {
	case 4:
		// We will attempt to parse this error outside of this block.
		msg := "client error"
		data, _ := ioutil.ReadAll(resp.Body)
		mt, _, err := mime.ParseMediaType(resp.Header.Get("content-type"))
		if err == nil && mt == "text/plain" && len(msg) > 0 {
			msg = string(data)
		}
		return errors.Wrap(errors.New(resp.Status), msg)
	case 1, 2:
		return nil

	default:
		msg := "unknown server error"
		return errors.Wrap(errors.New(resp.Status), msg)
	}
}

func QueryRequestFromProxyRequest(req *ProxyRequest) (*QueryRequest, error) {
	qr := new(QueryRequest)
	switch c := req.Compiler.(type) {
	case lang.FluxCompiler:
		qr.Type = "flux"
		qr.Query = c.Query
	case repl.Compiler:
		qr.Type = "flux"
		qr.Spec = c.Spec
	default:
		return nil, fmt.Errorf("unsupported compiler %T", c)
	}
	switch d := req.Dialect.(type) {
	case *csv.Dialect:
		var header = !d.ResultEncoderConfig.NoHeader
		qr.Dialect.Header = &header
		qr.Dialect.Delimiter = string(d.ResultEncoderConfig.Delimiter)
		qr.Dialect.CommentPrefix = "#"
		qr.Dialect.DateTimeFormat = "RFC3339"
		qr.Dialect.Annotations = d.ResultEncoderConfig.Annotations
	default:
		return nil, fmt.Errorf("unsupported dialect %T", d)
	}

	return qr, nil
}
