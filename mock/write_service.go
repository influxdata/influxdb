package mock

import (
	"context"
	"io"

	platform "github.com/influxdata/influxdb"
)

// WriteService writes data read from the reader.
type WriteService struct {
	WriteF func(context.Context, platform.ID, platform.ID, io.Reader) error
}

// Write calls the mocked WriteF function with arguments.
func (s *WriteService) Write(ctx context.Context, org, bucket platform.ID, r io.Reader) error {
	return s.WriteF(ctx, org, bucket, r)
}
