package subscriber

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/influxdata/influxdb/coordinator"
)

// HTTP supports writing points over HTTP using the line protocol.
type HTTP struct {
	url         *url.URL
	accessToken string
	httpClient  *http.Client
}

// NewHTTP returns a new HTTP points writer with default options.
func NewHTTP(addr string, timeout time.Duration) (*HTTP, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	} else if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("Unsupported protocol scheme: %s, your address must start with http:// or https://", u.Scheme)
	}
	// Grab username from URL as token.
	var token string
	if u.User != nil {
		token = u.User.Username()
		u.User = nil
	}
	return &HTTP{
		url:         u,
		accessToken: token,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}, nil
}

// WritePoints writes points over HTTP transport.
func (h *HTTP) WritePoints(p *coordinator.WritePointsRequest) error {
	var b bytes.Buffer
	for _, p := range p.Points {
		if _, err := b.WriteString(p.String()); err != nil {
			return err
		}
		if err := b.WriteByte('\n'); err != nil {
			return err
		}
	}

	u := *h.url
	u.Path = "write"
	params := url.Values{}
	params.Set("db", p.Database)
	params.Set("rp", p.RetentionPolicy)
	params.Set("precision", "ns")
	u.RawQuery = params.Encode()

	req, err := http.NewRequest("POST", u.String(), &b)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "")
	req.Header.Set("User-Agent", "InfluxDBSubscription")
	if h.accessToken != "" {
		req.Header.Set("InfluxDB-Access-Token", h.accessToken)
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf(string(body))
	}
	return nil
}
