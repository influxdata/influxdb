package influxdb

import (
	"context"
	"io"
)

// WriteService writes data read from the reader.
type WriteService interface {
	Write(ctx context.Context, org, bucket ID, r io.Reader) error
	WriteTo(ctx context.Context, filter BucketFilter, r io.Reader) error
}
