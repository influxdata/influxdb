package backup

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"go.uber.org/zap"
)

type Request struct {
	// Organization to backup.
	// If not set, all orgs will be included.
	OrgID influxdb.ID
	Org   string

	// Bucket to backup.
	// If not set, all buckets within the org filter will be included.
	BucketID influxdb.ID
	Bucket   string

	// Path to the directory where backup files should be written.
	Path string
}

type backupRunner struct {
	baseName      string
	backupSvc     influxdb.BackupService
	tenantService influxdb.TenantService
	metaClient    *meta.Client
	log           *zap.Logger
}

func RunBackup(ctx context.Context, req Request, svc influxdb.BackupService, log *zap.Logger) error {
	if err := os.MkdirAll(req.Path, 0777); err != nil {
		return err
	}

	var manifest influxdb.Manifest
	runner := backupRunner{
		baseName:  time.Now().UTC().Format(influxdb.BackupFilenamePattern),
		backupSvc: svc,
		log:       log,
	}

	manifest.KV.FileName = fmt.Sprintf("%s.bolt", runner.baseName)
	kvPath := filepath.Join(req.Path, manifest.KV.FileName)
	if err := runner.backupKV(ctx, kvPath); err != nil {
		return err
	}

	fi, err := os.Stat(kvPath)
	if err != nil {
		return fmt.Errorf("failed to inspect local KV backup at %q: %w", kvPath, err)
	}
	manifest.KV.Size = fi.Size()

	// Inspect the backed-up KV data so we can iterate through orgs & buckets.
	kvStore := bolt.NewKVStore(runner.log, kvPath)
	if err := kvStore.Open(ctx); err != nil {
		return err
	}
	defer kvStore.Close()

	runner.tenantService = tenant.NewService(tenant.NewStore(kvStore))
	runner.metaClient = meta.NewClient(meta.NewConfig(), kvStore)
	if err := runner.metaClient.Open(); err != nil {
		return err
	}
	defer runner.metaClient.Close()

	manifest.Files, err = runner.findShards(ctx, req)
	if err != nil {
		return err
	}

	for i := range manifest.Files {
		if err := runner.backupShard(ctx, &manifest.Files[i], req); err != nil {
			return err
		}
	}

	manifestPath := filepath.Join(req.Path, fmt.Sprintf("%s.manifest", runner.baseName))
	if err := runner.writeManifest(manifest, manifestPath); err != nil {
		return fmt.Errorf("failed to write backup manfiest to %q: %w", manifestPath, err)
	}

	log.Info("Backup complete", zap.String("path", req.Path))
	return nil
}

func (r *backupRunner) backupKV(ctx context.Context, path string) error {
	r.log.Info("Backing up KV store", zap.String("path", path))
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to open local KV backup file at %q: %w", path, err)
	}

	// Stream bolt file from server, sync, and ensure file closes correctly.
	if err := r.backupSvc.BackupKVStore(ctx, f); err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to download KV backup: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to flush KV backup to local disk: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close local KV backup at %q: %w", path, err)
	}

	return nil
}

func (r *backupRunner) findShards(ctx context.Context, req Request) ([]influxdb.ManifestEntry, error) {
	filter := influxdb.OrganizationFilter{}
	if req.OrgID.Valid() {
		filter.ID = &req.OrgID
	}
	if req.Org != "" {
		filter.Name = &req.Org
	}

	orgs, _, err := r.tenantService.FindOrganizations(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to find matching organizations: %w", err)
	}

	var entries []influxdb.ManifestEntry
	for _, org := range orgs {
		r.log.Info("Backing up organization", zap.String("id", org.ID.String()), zap.String("name", org.Name))
		oentries, err := r.findOrgShards(ctx, org, req)
		if err != nil {
			return nil, err
		}
		entries = append(entries, oentries...)
	}

	return entries, nil
}

func (r *backupRunner) findOrgShards(ctx context.Context, org *influxdb.Organization, req Request) ([]influxdb.ManifestEntry, error) {
	filter := influxdb.BucketFilter{OrganizationID: &org.ID}
	if req.BucketID.Valid() {
		filter.ID = &req.BucketID
	}
	if req.Bucket != "" {
		filter.Name = &req.Bucket
	}
	buckets, _, err := r.tenantService.FindBuckets(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to find matching buckets: %w", err)
	}

	var entries []influxdb.ManifestEntry
	for _, bucket := range buckets {
		r.log.Info("Backing up bucket", zap.String("id", bucket.ID.String()), zap.String("name", bucket.Name))

		// Lookup matching database from the bucket.
		dbi := r.metaClient.Database(bucket.ID.String())
		if dbi == nil {
			return nil, fmt.Errorf("bucket database not found: %s", bucket.ID.String())
		}

		// Collect info for each shard in the DB.
		for _, rpi := range dbi.RetentionPolicies {
			for _, sg := range rpi.ShardGroups {
				if sg.Deleted() {
					continue
				}

				for _, sh := range sg.Shards {
					entries = append(entries, influxdb.ManifestEntry{
						OrganizationID:   org.ID.String(),
						OrganizationName: org.Name,
						BucketID:         bucket.ID.String(),
						BucketName:       bucket.Name,
						ShardID:          sh.ID,
					})
				}
			}
		}
	}

	return entries, nil
}

func (r *backupRunner) backupShard(ctx context.Context, shardInfo *influxdb.ManifestEntry, req Request) error {
	shardInfo.FileName = fmt.Sprintf("%s.s%d.tar.gz", r.baseName, shardInfo.ShardID)
	path := filepath.Join(req.Path, shardInfo.FileName)
	r.log.Info("Backing up shard", zap.Uint64("id", shardInfo.ShardID), zap.String("path", path))

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to open local shard backup at %q: %w", path, err)
	}
	gw := gzip.NewWriter(f)

	// Stream file from server, sync, and ensure file closes correctly.
	if err := r.backupSvc.BackupShard(ctx, gw, shardInfo.ShardID, time.Time{}); err != nil {
		_ = gw.Close()
		_ = f.Close()

		if influxdb.ErrorCode(err) == influxdb.ENotFound {
			r.log.Warn("Shard removed during backup", zap.Uint64("id", shardInfo.ShardID))
			return nil
		}
		return fmt.Errorf("failed to download shard backup: %w", err)
	}
	if err := gw.Close(); err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to flush GZIP footer to local shard backup: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to flush shard backup to local disk: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close local shard backup at %q: %w", path, err)
	}

	// Use downloaded file's info to fill in remaining pieces of manifest.
	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to inspect local shard backup at %q: %w", path, err)
	}
	shardInfo.Size = fi.Size()
	shardInfo.LastModified = fi.ModTime().UTC()

	return nil
}

func (r *backupRunner) writeManifest(manifest influxdb.Manifest, path string) error {
	r.log.Info("Writing manifest", zap.String("path", path))

	buf, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	buf = append(buf, '\n')
	return ioutil.WriteFile(path, buf, 0600)
}
