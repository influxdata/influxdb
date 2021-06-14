package backup

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"go.uber.org/zap"
)

type BucketManifestWriter struct {
	ts *tenant.Service
	mc *meta.Client
}

func NewBucketManifestWriter(ts *tenant.Service, mc *meta.Client) BucketManifestWriter {
	return BucketManifestWriter{
		ts: ts,
		mc: mc,
	}
}

// WriteManifest writes a bucket manifest describing all of the buckets that exist in the database.
// It is intended to be used to write to an HTTP response after appropriate measures have been taken
// to ensure that the request is authorized.
func (b BucketManifestWriter) WriteManifest(ctx context.Context, w io.Writer) error {
	bkts, _, err := b.ts.FindBuckets(ctx, influxdb.BucketFilter{})
	if err != nil {
		return err
	}

	l := make([]influxdb.BucketMetadataManifest, 0, len(bkts))

	for _, bkt := range bkts {
		org, err := b.ts.OrganizationService.FindOrganizationByID(ctx, bkt.OrgID)
		if err != nil {
			return err
		}

		dbInfo := b.mc.Database(bkt.ID.String())

		var description *string
		if bkt.Description != "" {
			description = &bkt.Description
		}

		l = append(l, influxdb.BucketMetadataManifest{
			OrganizationID:         bkt.OrgID,
			OrganizationName:       org.Name,
			BucketID:               bkt.ID,
			BucketName:             bkt.Name,
			Description:            description,
			DefaultRetentionPolicy: dbInfo.DefaultRetentionPolicy,
			RetentionPolicies:      retentionPolicyToManifest(dbInfo.RetentionPolicies),
		})
	}

	return json.NewEncoder(w).Encode(&l)
}

// retentionPolicyToManifest and the various similar functions that follow are for converting
// from the structs in the meta package to the manifest structs
func retentionPolicyToManifest(meta []meta.RetentionPolicyInfo) []influxdb.RetentionPolicyManifest {
	r := make([]influxdb.RetentionPolicyManifest, 0, len(meta))

	for _, m := range meta {
		r = append(r, influxdb.RetentionPolicyManifest{
			Name:               m.Name,
			ReplicaN:           m.ReplicaN,
			Duration:           m.Duration,
			ShardGroupDuration: m.ShardGroupDuration,
			ShardGroups:        shardGroupToManifest(m.ShardGroups),
			Subscriptions:      subscriptionInfosToManifest(m.Subscriptions),
		})
	}

	return r
}

func subscriptionInfosToManifest(subInfos []meta.SubscriptionInfo) []influxdb.SubscriptionManifest {
	r := make([]influxdb.SubscriptionManifest, 0, len(subInfos))

	for _, s := range subInfos {
		r = append(r, influxdb.SubscriptionManifest(s))
	}

	return r
}

func shardGroupToManifest(shardGroups []meta.ShardGroupInfo) []influxdb.ShardGroupManifest {
	r := make([]influxdb.ShardGroupManifest, 0, len(shardGroups))

	for _, s := range shardGroups {
		deletedAt := &s.DeletedAt
		truncatedAt := &s.TruncatedAt

		// set deletedAt and truncatedAt to nil rather than their zero values so that the fields
		// can be properly omitted from the JSON response if they are empty
		if deletedAt.IsZero() {
			deletedAt = nil
		}

		if truncatedAt.IsZero() {
			truncatedAt = nil
		}

		r = append(r, influxdb.ShardGroupManifest{
			ID:          s.ID,
			StartTime:   s.StartTime,
			EndTime:     s.EndTime,
			DeletedAt:   deletedAt,
			TruncatedAt: truncatedAt,
			Shards:      shardInfosToManifest(s.Shards),
		})
	}

	return r
}

func shardInfosToManifest(shards []meta.ShardInfo) []influxdb.ShardManifest {
	r := make([]influxdb.ShardManifest, 0, len(shards))

	for _, s := range shards {
		r = append(r, influxdb.ShardManifest{
			ID:          s.ID,
			ShardOwners: shardOwnersToManifest(s.Owners),
		})
	}

	return r
}

func shardOwnersToManifest(shardOwners []meta.ShardOwner) []influxdb.ShardOwner {
	r := make([]influxdb.ShardOwner, 0, len(shardOwners))

	for _, s := range shardOwners {
		r = append(r, influxdb.ShardOwner(s))
	}

	return r
}

type Request struct {
	// Organization to backup.
	// If not set, all orgs will be included.
	OrgID platform.ID
	Org   string

	// Bucket to backup.
	// If not set, all buckets within the org filter will be included.
	BucketID platform.ID
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

		if errors.ErrorCode(err) == errors.ENotFound {
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
