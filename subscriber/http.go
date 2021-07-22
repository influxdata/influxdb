package subscriber

import (
	"context"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"net/http"

	apihttp "github.com/influxdata/influxdb-client-go/v2/api/http"
)

// HTTP supports writing points over HTTP using the line protocol.
type HTTP struct {
	writer api.WriteAPIBlocking
	errBuffer chan error
}

var _ PointsWriter = &HTTP{}

// NewHTTP returns a new HTTP points writer with default options.
func NewHTTP(addr string, token string, org string, bucket string) (*HTTP, error) {
	// TODO: deal with TLS options / timeouts / other config
	// TODO: connection pooling?
	service := apihttp.NewService(addr, "Token " + token, apihttp.DefaultOptions().SetHTTPClient(http.DefaultClient))
	writer := api.NewWriteAPIBlocking(org, bucket, service, write.DefaultOptions())
	return &HTTP{
		writer:writer,
	}, nil
}

// WritePoints writes points over HTTP transport.
// Should not be called concurrently.
func (h *HTTP) WritePointsContext(ctx context.Context, lineProtocol []byte) (err error) {
	// It would be nice if there was a real API for this instead of abusing the existing one.
	return h.writer.WriteRecord(ctx, string(lineProtocol))
}