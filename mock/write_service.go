package mock

import (
	"context"
	"io"

	platform "github.com/influxdata/influxdb/v2"
)

// WriteService writes data read from the reader.
type WriteService struct {
	WriteToF func(context.Context, platform.BucketFilter, io.Reader) error
}

// WriteTo calls the mocked WriteToF function with arguments.
func (s *WriteService) WriteTo(ctx context.Context, filter platform.BucketFilter, r io.Reader) error {
	return s.WriteToF(ctx, filter, r)
}
