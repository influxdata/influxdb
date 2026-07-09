package subscriber

import (
	"bytes"
	"context"
	"crypto/tls"
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
// configured. tlsConfig is the fully-resolved client TLS configuration (root
// CAs, any client certificate, and InsecureSkipVerify) built by the service via
// tlsconfig.TLSConfigManager; it may be nil to use Go's defaults. When it
// carries a manager-backed GetClientCertificate, rotated client certificates
// are picked up automatically on new connections.
func NewHTTPS(addr string, timeout time.Duration, tlsConfig *tls.Config) (*HTTP, error) {
	conf := client.HTTPConfig{
		Addr:      addr,
		Timeout:   timeout,
		TLSConfig: tlsConfig,
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
