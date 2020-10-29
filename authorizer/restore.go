package authorizer

import (
	"context"
	"io"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
)

var _ influxdb.RestoreService = (*RestoreService)(nil)

// RestoreService wraps a influxdb.RestoreService and authorizes actions
// against it appropriately.
type RestoreService struct {
	s influxdb.RestoreService
}

// NewRestoreService constructs an instance of an authorizing restore service.
func NewRestoreService(s influxdb.RestoreService) *RestoreService {
	return &RestoreService{
		s: s,
	}
}

func (b RestoreService) RestoreBucket(ctx context.Context, id influxdb.ID, dbi []byte) (shardIDMap map[uint64]uint64, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// TODO(bbj): Correct permissions.
	if err := IsAllowedAll(ctx, influxdb.ReadAllPermissions()); err != nil {
		return nil, err
	}
	return b.s.RestoreBucket(ctx, id, dbi)
}

func (b RestoreService) RestoreShard(ctx context.Context, shardID uint64, r io.Reader) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// TODO(bbj): Correct permissions.
	if err := IsAllowedAll(ctx, influxdb.ReadAllPermissions()); err != nil {
		return err
	}
	return b.s.RestoreShard(ctx, shardID, r)
}
