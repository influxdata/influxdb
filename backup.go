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

	// RLockKVStore locks the database.
	RLockKVStore()

	// RUnlockKVStore unlocks the database.
	RUnlockKVStore()
}

// SqlBackupRestoreService represents the backup and restore functions for the sqlite database.
type SqlBackupRestoreService interface {
	// BackupSqlStore creates a live backup copy of the sqlite database.
	BackupSqlStore(ctx context.Context, w io.Writer) error

	// RestoreSqlStore restores & replaces the sqlite database.
	RestoreSqlStore(ctx context.Context, r io.Reader) error

	// RLockSqlStore takes a read lock on the database
	RLockSqlStore()

	// RUnlockSqlStore releases a previously-taken read lock on the database.
	RUnlockSqlStore()
}

type BucketManifestWriter interface {
	WriteManifest(ctx context.Context, w io.Writer) error
}

// RestoreService represents the data restore functions of InfluxDB.
type RestoreService interface {
	// RestoreKVStore restores & replaces metadata database.
	RestoreKVStore(ctx context.Context, r io.Reader) error

	// RestoreBucket restores storage metadata for a bucket.
	// TODO(danmoran): As far as I can tell, dbInfo is typed as a []byte because typing it as
	//  a meta.DatabaseInfo introduces a circular dependency between the root package and `meta`.
	//  We should refactor to make this signature easier to use. It might be easier to wait
	//  until we're ready to delete the 2.0.x restore APIs before refactoring.
	RestoreBucket(ctx context.Context, id platform.ID, dbInfo []byte) (shardIDMap map[uint64]uint64, err error)

	// RestoreShard uploads a backup file for a single shard.
	RestoreShard(ctx context.Context, shardID uint64, r io.Reader) error
}

// BucketMetadataManifest contains the information about a bucket for backup purposes.
// It is composed of various nested structs below.
type BucketMetadataManifest struct {
	OrganizationID         platform.ID               `json:"organizationID"`
	OrganizationName       string                    `json:"organizationName"`
	BucketID               platform.ID               `json:"bucketID"`
	BucketName             string                    `json:"bucketName"`
	Description            *string                   `json:"description,omitempty"`
	DefaultRetentionPolicy string                    `json:"defaultRetentionPolicy"`
	RetentionPolicies      []RetentionPolicyManifest `json:"retentionPolicies"`
}

type RetentionPolicyManifest struct {
	Name               string                 `json:"name"`
	ReplicaN           int                    `json:"replicaN"`
	Duration           time.Duration          `json:"duration"`
	ShardGroupDuration time.Duration          `json:"shardGroupDuration"`
	ShardGroups        []ShardGroupManifest   `json:"shardGroups"`
	Subscriptions      []SubscriptionManifest `json:"subscriptions"`
}

type ShardGroupManifest struct {
	ID          uint64          `json:"id"`
	StartTime   time.Time       `json:"startTime"`
	EndTime     time.Time       `json:"endTime"`
	DeletedAt   *time.Time      `json:"deletedAt,omitempty"`   // use pointer to time.Time so that omitempty works
	TruncatedAt *time.Time      `json:"truncatedAt,omitempty"` // use pointer to time.Time so that omitempty works
	Shards      []ShardManifest `json:"shards"`
}

type ShardManifest struct {
	ID          uint64       `json:"id"`
	ShardOwners []ShardOwner `json:"shardOwners"`
}

type ShardOwner struct {
	NodeID uint64 `json:"nodeID"`
}

type SubscriptionManifest struct {
	Name         string   `json:"name"`
	Mode         string   `json:"mode"`
	Destinations []string `json:"destinations"`
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

type RestoredBucketMappings struct {
	ID            platform.ID            `json:"id"`
	Name          string                 `json:"name"`
	ShardMappings []RestoredShardMapping `json:"shardMappings"`
}

type RestoredShardMapping struct {
	OldId uint64 `json:"oldId"`
	NewId uint64 `json:"newId"`
}

// Size returns the size of the manifest.
func (m *Manifest) Size() int64 {
	n := m.KV.Size
	for _, f := range m.Files {
		n += f.Size
	}
	return n
}
