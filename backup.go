package influxdb

import (
	"context"
	"io"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
)

const (
	BackupFilenamePattern = "20060102T150405Z"
)

// BackupService represents the data backup functions of InfluxDB.
type BackupService interface {
	// BackupKVStore creates a live backup copy of the metadata database.
	BackupKVStore(ctx context.Context, w io.Writer) error

	// BackupShard downloads a backup file for a single shard.
	BackupShard(ctx context.Context, w io.Writer, shardID uint64, since time.Time) error
}

// RestoreService represents the data restore functions of InfluxDB.
type RestoreService interface {
	// RestoreKVStore restores & replaces metadata database.
	RestoreKVStore(ctx context.Context, r io.Reader) error

	// RestoreKVStore restores the metadata database.
	RestoreBucket(ctx context.Context, id platform.ID, rpiData []byte) (shardIDMap map[uint64]uint64, err error)

	// RestoreShard uploads a backup file for a single shard.
	RestoreShard(ctx context.Context, shardID uint64, r io.Reader) error
}

// Manifest lists the KV and shard file information contained in the backup.
type Manifest struct {
	KV    ManifestKVEntry `json:"kv"`
	Files []ManifestEntry `json:"files"`
}

// ManifestEntry contains the data information for a backed up shard.
type ManifestEntry struct {
	OrganizationID   string    `json:"organizationID"`
	OrganizationName string    `json:"organizationName"`
	BucketID         string    `json:"bucketID"`
	BucketName       string    `json:"bucketName"`
	ShardID          uint64    `json:"shardID"`
	FileName         string    `json:"fileName"`
	Size             int64     `json:"size"`
	LastModified     time.Time `json:"lastModified"`
}

// ManifestKVEntry contains the KV store information for a backup.
type ManifestKVEntry struct {
	FileName string `json:"fileName"`
	Size     int64  `json:"size"`
}

// Size returns the size of the manifest.
func (m *Manifest) Size() int64 {
	n := m.KV.Size
	for _, f := range m.Files {
		n += f.Size
	}
	return n
}
