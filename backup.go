package influxdb

import (
	"context"
	"io"
	"time"
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
	// RestoreKVStore restores the metadata database.
	RestoreBucket(ctx context.Context, id ID, rpiData []byte) (shardIDMap map[uint64]uint64, err error)

	// RestoreShard uploads a backup file for a single shard.
	RestoreShard(ctx context.Context, shardID uint64, r io.Reader) error
}

// Manifest lists the KV and shard file information contained in the backup.
// If Limited is false, the manifest contains a full backup, otherwise
// it is a partial backup.
type Manifest struct {
	KV    ManifestKVEntry `json:"kv"`
	Files []ManifestEntry `json:"files"`

	// If limited is true, then one (or all) of the following fields will be set

	OrganizationID string `json:"organizationID,omitempty"`
	BucketID       string `json:"bucketID,omitempty"`
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
