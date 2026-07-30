package subscriber

import (
	"bytes"
	"context"
	"net"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

// HTTP supports writing points over HTTP using the line protocol.
type HTTP struct {
	c    client.HTTPClient
	addr string
}

// NewHTTP returns a new HTTP points writer with default options.
func NewHTTP(addr string, timeout time.Duration) (*HTTP, error) {
	return NewHTTPS(addr, timeout, nil)
}

// NewHTTPS returns a new HTTPS points writer with default options and HTTPS
// configured.
//
// dialTLSContext dials the writer's TLS connections and is the only source of
// its TLS configuration. It is normally tlsconfig.TLSConfigManager.DialContext,
// which resolves the configuration on each connection. That is what allows a
// reloaded configuration to reach a writer that already exists: the writer keeps
// dialing through the manager rather than through a configuration captured when
// it was built. It may be nil to use Go's defaults, as NewHTTP does.
//
// timeout bounds the whole request, including the connection and TLS handshake,
// so no separate handshake timeout is needed.
func NewHTTPS(addr string, timeout time.Duration, dialTLSContext func(ctx context.Context, network, addr string) (net.Conn, error)) (*HTTP, error) {
	conf := client.HTTPConfig{
		Addr:           addr,
		Timeout:        timeout,
		DialTLSContext: dialTLSContext,
	}

	c, err := client.NewHTTPClient(conf)
	if err != nil {
		return nil, err
	}
	return &HTTP{c: c, addr: addr}, nil
}

// WritePoints writes points over HTTP transport.
func (h *HTTP) WritePointsContext(ctx context.Context, request WriteRequest) (destination string, err error) {
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:        request.Database,
		RetentionPolicy: request.RetentionPolicy,
	})
	return h.addr, h.c.WriteRawCtx(ctx, bp, bytes.NewReader(request.lineProtocol))
}
