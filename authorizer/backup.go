package authorizer

import (
	"context"
	"io"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
)

var _ influxdb.BackupService = (*BackupService)(nil)

// BackupService wraps a influxdb.BackupService and authorizes actions
// against it appropriately.
type BackupService struct {
	s influxdb.BackupService
}

// NewBackupService constructs an instance of an authorizing backup service.
func NewBackupService(s influxdb.BackupService) *BackupService {
	return &BackupService{
		s: s,
	}
}

func (b BackupService) CreateBackup(ctx context.Context) (int, []string, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := IsAllowedAll(ctx, influxdb.ReadAllPermissions()); err != nil {
		return 0, nil, err
	}
	return b.s.CreateBackup(ctx)
}

func (b BackupService) FetchBackupFile(ctx context.Context, backupID int, backupFile string, w io.Writer) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := IsAllowedAll(ctx, influxdb.ReadAllPermissions()); err != nil {
		return err
	}
	return b.s.FetchBackupFile(ctx, backupID, backupFile, w)
}

func (b BackupService) InternalBackupPath(backupID int) string {
	return b.s.InternalBackupPath(backupID)
}
