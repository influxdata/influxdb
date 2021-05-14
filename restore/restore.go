package restore

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"go.uber.org/zap"
)

type Request struct {
	// Path to local backup data created using `influx backup`
	Path string

	// Original ID/name of the organization to restore.
	// If not set, all orgs will be restored.
	OrgID platform.ID
	Org   string

	// New name to use for the restored organization.
	// If not set, the org will be restored using its backed-up name.
	NewOrgName string

	// Original ID/name of the bucket to restore.
	// If not set, all buckets within the org filter will be restored.
	BucketID platform.ID
	Bucket   string

	// New name to use for the restored bucket.
	// If not set, the bucket will be restored using its backed-up name.
	NewBucketName string

	// If true, replace all data on the server with the local backup.
	// Otherwise only restore the requested org/bucket, leaving other data untouched.
	Full bool
}

type Services struct {
	RestoreService influxdb.RestoreService
	OrgService     influxdb.OrganizationService
	BucketService  influxdb.BucketService
}

type restoreRunner struct {
	Services

	kvManifest     *influxdb.ManifestKVEntry
	shardManifests map[uint64]*influxdb.ManifestEntry

	tenantService influxdb.TenantService
	metaClient    *meta.Client
	log           *zap.Logger
}

func RunRestore(ctx context.Context, req Request, svcs Services, log *zap.Logger) error {
	runner := restoreRunner{
		Services: svcs,
		log:      log,
	}

	if err := runner.loadManifests(req.Path); err != nil {
		return err
	}
	// Bail out early if no manifests were found.
	// TODO: Should this be an error?
	if runner.kvManifest == nil {
		return nil
	}

	if req.Full {
		return runner.fullRestore(ctx, req)
	}
	return runner.partialRestore(ctx, req)
}

func (r *restoreRunner) loadManifests(path string) error {
	// Read all manifest files from path, sort in descending time.
	manifests, err := filepath.Glob(filepath.Join(path, "*.manifest"))
	if err != nil {
		return fmt.Errorf("failed to find backup manifests at %q: %w", path, err)
	} else if len(manifests) == 0 {
		return nil
	}
	sort.Sort(sort.Reverse(sort.StringSlice(manifests)))

	r.shardManifests = make(map[uint64]*influxdb.ManifestEntry)
	for _, filename := range manifests {
		// Skip file if it is a directory.
		if fi, err := os.Stat(filename); err != nil {
			return fmt.Errorf("failed to inspect local manifest at %q: %w", filename, err)
		} else if fi.IsDir() {
			continue
		}

		// Read manifest file for backup.
		var manifest influxdb.Manifest
		if buf, err := ioutil.ReadFile(filename); err != nil {
			return fmt.Errorf("failed to read local manifest at %q: %w", filename, err)
		} else if err := json.Unmarshal(buf, &manifest); err != nil {
			return fmt.Errorf("read manifest: %v", err)
		}

		// Save latest KV entry (first in the sorted slice).
		if r.kvManifest == nil {
			r.kvManifest = &manifest.KV
		}

		// Load most recent backup per shard.
		for i := range manifest.Files {
			sh := manifest.Files[i]
			if _, err := os.Stat(filepath.Join(path, sh.FileName)); err != nil {
				continue
			}

			entry := r.shardManifests[sh.ShardID]
			if entry == nil || sh.LastModified.After(entry.LastModified) {
				r.shardManifests[sh.ShardID] = &sh
			}
		}
	}

	return nil
}

func (r *restoreRunner) fullRestore(ctx context.Context, req Request) error {
	if err := r.restoreKV(ctx, req.Path); err != nil {
		return err
	}

	for _, m := range r.shardManifests {
		if err := r.restoreShard(ctx, req.Path, m); err != nil {
			return err
		}
	}

	r.log.Info("Full restore complete", zap.String("path", req.Path))
	return nil
}

func (r *restoreRunner) partialRestore(ctx context.Context, req Request) error {
	// Open meta store so we can iterate over metadata.
	kvStore := bolt.NewKVStore(r.log, filepath.Join(req.Path, r.kvManifest.FileName))
	if err := kvStore.Open(ctx); err != nil {
		return err
	}
	defer kvStore.Close()

	r.tenantService = tenant.NewService(tenant.NewStore(kvStore))
	r.metaClient = meta.NewClient(meta.NewConfig(), kvStore)
	if err := r.metaClient.Open(); err != nil {
		return err
	}
	defer r.metaClient.Close()

	if err := r.restoreOrganizations(ctx, req); err != nil {
		return err
	}

	r.log.Info("Partial restore complete", zap.String("path", req.Path))
	return nil
}

func (r *restoreRunner) restoreKV(ctx context.Context, path string) error {
	kvPath := filepath.Join(path, r.kvManifest.FileName)
	r.log.Info("Restoring full metadata from local backup", zap.String("path", kvPath))

	f, err := os.Open(kvPath)
	if err != nil {
		return fmt.Errorf("failed to open local KV backup at %q: %w", kvPath, err)
	}
	defer f.Close()

	if err := r.RestoreService.RestoreKVStore(ctx, f); err != nil {
		return fmt.Errorf("failed to upload local KV backup at %q: %w", kvPath, err)
	}

	r.log.Info("Full metadata restored", zap.String("path", kvPath))
	return nil
}

func (r *restoreRunner) restoreShard(ctx context.Context, path string, manifest *influxdb.ManifestEntry) error {
	shardPath := filepath.Join(path, manifest.FileName)
	r.log.Info("Restoring shard from local backup", zap.Uint64("id", manifest.ShardID), zap.String("path", shardPath))

	f, err := os.Open(shardPath)
	if err != nil {
		return fmt.Errorf("failed to open local shard backup at %q: %w", shardPath, err)
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("failed to open gzip reader for local shard backup: %w", err)
	}
	defer gr.Close()

	if err := r.RestoreService.RestoreShard(ctx, manifest.ShardID, gr); err != nil {
		return fmt.Errorf("failed to upload local shard backup at %q: %w", shardPath, err)
	}
	return nil
}

func (r *restoreRunner) restoreOrganizations(ctx context.Context, req Request) error {
	var filter influxdb.OrganizationFilter
	if req.OrgID.Valid() {
		filter.ID = &req.OrgID
	}
	if req.Org != "" {
		filter.Name = &req.Org
	}

	orgs, _, err := r.tenantService.FindOrganizations(ctx, filter)
	if err != nil {
		return err
	}

	for _, org := range orgs {
		if err := r.restoreOrganization(ctx, org, req); err != nil {
			return err
		}
	}

	return nil
}

func (r *restoreRunner) restoreOrganization(ctx context.Context, org *influxdb.Organization, req Request) error {
	newOrg := *org
	if req.NewOrgName != "" {
		newOrg.Name = req.NewOrgName
	}
	r.log.Info(
		"Restoring organization",
		zap.String("backup_id", org.ID.String()),
		zap.String("backup_name", org.Name),
		zap.String("restored_name", newOrg.Name),
	)

	// Create organization on server if it doesn't already exist.
	if o, err := r.OrgService.FindOrganization(ctx, influxdb.OrganizationFilter{Name: &newOrg.Name}); errors.ErrorCode(err) == errors.ENotFound {
		if err := r.OrgService.CreateOrganization(ctx, &newOrg); err != nil {
			return fmt.Errorf("failed to create organization %q: %w", newOrg.Name, err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to check existence of organization %q: %w", newOrg.Name, err)
	} else {
		newOrg.ID = o.ID
	}

	filter := influxdb.BucketFilter{OrganizationID: &org.ID}
	if req.BucketID.Valid() {
		filter.ID = &req.BucketID
	}
	if req.Bucket != "" {
		filter.Name = &req.Bucket
	}

	buckets, _, err := r.tenantService.FindBuckets(ctx, filter)
	if err != nil {
		return err
	}

	for _, bkt := range buckets {
		// Skip internal buckets.
		if strings.HasPrefix(bkt.Name, "_") {
			continue
		}

		bkt = bkt.Clone()
		bkt.OrgID = newOrg.ID

		if err := r.restoreBucket(ctx, bkt, req); err != nil {
			return err
		}
	}

	return nil
}

func (r *restoreRunner) restoreBucket(ctx context.Context, bkt *influxdb.Bucket, req Request) error {
	newBucket := *bkt
	if req.NewBucketName != "" {
		newBucket.Name = req.NewBucketName
	}
	r.log.Info(
		"Restoring bucket",
		zap.String("backup_id", bkt.ID.String()),
		zap.String("backup_name", bkt.Name),
		zap.String("restored_name", newBucket.Name),
	)

	// Lookup matching database from the meta store.
	// Search using bucket ID from backup.
	dbi := r.metaClient.Database(bkt.ID.String())
	if dbi == nil {
		return fmt.Errorf("database for bucket %q not found in local backup", bkt.ID.String())
	}

	// Serialize to protobufs.
	buf, err := dbi.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal database info for bucket %q: %w", bkt.ID.String(), err)
	}

	// TODO: Only create if it doesn't already exist.
	if err := r.BucketService.CreateBucket(ctx, &newBucket); err != nil {
		return fmt.Errorf("failed to create bucket %q: %w", newBucket.Name, err)
	}

	shardIDMap, err := r.RestoreService.RestoreBucket(ctx, newBucket.ID, buf)
	if err != nil {
		return fmt.Errorf("failed to restore bucket %q: %w", newBucket.Name, err)
	}

	// Restore each shard for the bucket.
	for _, m := range r.shardManifests {
		if bkt.ID.String() != m.BucketID {
			continue
		}

		// Skip if shard metadata was not imported.
		newID, ok := shardIDMap[m.ShardID]
		if !ok {
			r.log.Warn(
				"Meta info not found, skipping shard",
				zap.Uint64("shard_id", m.ShardID),
				zap.String("bucket_id", newBucket.ID.String()),
				zap.String("path", filepath.Join(req.Path, m.FileName)),
			)
			continue
		}

		m.ShardID = newID
		if err := r.restoreShard(ctx, req.Path, m); err != nil {
			return err
		}
	}

	return nil
}
