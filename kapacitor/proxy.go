package kapacitor

import (
	"bytes"
	"net/http"
	"net/url"

	"github.com/influxdata/chronograf"

	"golang.org/x/net/context"
)

// Client is a device for retrieving time series data from an InfluxDB instance
type Proxy struct {
	URL *url.URL
}

func (p *Proxy) Do(ctx context.Context, req *chronograf.Request) (*http.Response, error) {
	// TODO: Locking?
	p.URL.Path = req.Path

	httpReq, err := http.NewRequest(req.Method, p.URL.String(), bytes.NewBuffer(req.Body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{}
	return httpClient.Do(httpReq)
}

func (p *Proxy) Connect(ctx context.Context, srv *chronograf.Server) error {
	u, err := url.Parse(srv.URL)
	if err != nil {
		return err
	}
	u.User = url.UserPassword(srv.Username, srv.Password)
	p.URL = u
	return nil
}
