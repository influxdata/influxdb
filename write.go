package influxdb

import (
	"context"
	"io"
)

// WriteService writes data read from the reader.
type WriteService interface {
	Write(ctx context.Context, org, bucket ID, r io.Reader) error
}
