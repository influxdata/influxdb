package flux

import (
	"context"
	"crypto/tls"
	errors2 "errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Shared transports for all clients to prevent leaking connections.
var (
	skipVerifyTransport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	defaultTransport = &http.Transport{}
)

// Client is how we interact with Flux.
type Client struct {
	URL                *url.URL
	InsecureSkipVerify bool
	Timeout            time.Duration
}

// Ping checks the connection of a Flux.
func (c *Client) Ping(ctx context.Context) error {
	t := 2 * time.Second
	if c.Timeout > 0 {
		t = c.Timeout
	}
	ctx, cancel := context.WithTimeout(ctx, t)
	defer cancel()
	err := c.pingTimeout(ctx)
	return err
}

func (c *Client) pingTimeout(ctx context.Context) error {
	resps := make(chan (error))
	go func() {
		resps <- c.ping(c.URL)
	}()

	select {
	case resp := <-resps:
		return resp
	case <-ctx.Done():
		return fmt.Errorf("request to backend timed out")
	}
}

func (c *Client) ping(u *url.URL) error {
	u.Path = "ping"

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return err
	}

	hc := &http.Client{}
	if c.InsecureSkipVerify {
		hc.Transport = skipVerifyTransport
	} else {
		hc.Transport = defaultTransport
	}

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusNoContent {
		var err = errors2.New(string(body))
		return err
	}

	return nil
}
