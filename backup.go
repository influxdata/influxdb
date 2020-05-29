package influxdb

import (
	"context"
	"io"
)

// BackupService represents the data backup functions of InfluxDB.
type BackupService interface {
	// CreateBackup creates a local copy (hard links) of the TSM data for all orgs and buckets.
	// The return values are used to download each backup file.
	CreateBackup(context.Context) (backupID int, backupFiles []string, err error)
	// FetchBackupFile downloads one backup file, data or metadata.
	FetchBackupFile(ctx context.Context, backupID int, backupFile string, w io.Writer) error
	// InternalBackupPath is a utility to determine the on-disk location of a backup fileset.
	InternalBackupPath(backupID int) string
}

// KVBackupService represents the meta data backup functions of InfluxDB.
type KVBackupService interface {
	// Backup creates a live backup copy of the metadata database.
	Backup(ctx context.Context, w io.Writer) error
}
