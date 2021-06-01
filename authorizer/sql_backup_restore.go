package authorizer

import (
	"context"
	"io"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
)

var _ influxdb.SqlBackupRestoreService = (*SqlBackupRestoreService)(nil)

// SqlBackupRestoreService wraps a influxdb.SqlBackupRestoreService and authorizes actions
// against it appropriately.
type SqlBackupRestoreService struct {
	s influxdb.SqlBackupRestoreService
}

// NewSqlBackupRestoreService constructs an instance of an authorizing backup service.
func NewSqlBackupRestoreService(s influxdb.SqlBackupRestoreService) *SqlBackupRestoreService {
	return &SqlBackupRestoreService{
		s: s,
	}
}

func (s SqlBackupRestoreService) BackupSqlStore(ctx context.Context, w io.Writer) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := IsAllowedAll(ctx, influxdb.OperPermissions()); err != nil {
		return err
	}
	return s.s.BackupSqlStore(ctx, w)
}

func (s SqlBackupRestoreService) RestoreSqlStore(ctx context.Context, r io.Reader) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := IsAllowedAll(ctx, influxdb.OperPermissions()); err != nil {
		return err
	}
	return s.s.RestoreSqlStore(ctx, r)
}
