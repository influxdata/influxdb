package influxdb

import (
	"context"
	"io"
)

// WriteService writes data read from the reader.
type WriteService interface {
	WriteTo(ctx context.Context, filter BucketFilter, r io.Reader) error
}
