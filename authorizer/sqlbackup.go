package authorizer

import (
	"context"
	"io"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
)

var _ influxdb.SqlBackupService = (*SqlBackupService)(nil)

// SqlBackupService wraps a influxdb.SqlBackupService and authorizes actions
// against it appropriately.
type SqlBackupService struct {
	s influxdb.SqlBackupService
}

// NewSqlBackupService constructs an instance of an authorizing backup service.
func NewSqlBackupService(s influxdb.SqlBackupService) *SqlBackupService {
	return &SqlBackupService{
		s: s,
	}
}

func (b SqlBackupService) BackupSqlStore(ctx context.Context, w io.Writer) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := IsAllowedAll(ctx, influxdb.OperPermissions()); err != nil {
		return err
	}
	return b.s.BackupSqlStore(ctx, w)
}
