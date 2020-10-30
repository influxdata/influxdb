package main

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
	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/kv"
	influxlogger "github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func cmdBackup(f *globalFlags, opts genericCLIOpts) *cobra.Command {
	return newCmdBackupBuilder(f, opts).cmdBackup()
}

type cmdBackupBuilder struct {
	genericCLIOpts
	*globalFlags

	bucketID   string
	bucketName string
	org        organization
	path       string

	manifest influxdb.Manifest
	baseName string

	backupService *http.BackupService
	kvStore       *bolt.KVStore
	kvService     *kv.Service
	metaClient    *meta.Client

	logger *zap.Logger
}

func newCmdBackupBuilder(f *globalFlags, opts genericCLIOpts) *cmdBackupBuilder {
	return &cmdBackupBuilder{
		genericCLIOpts: opts,
		globalFlags:    f,
	}
}

func (b *cmdBackupBuilder) cmdBackup() *cobra.Command {
	cmd := b.newCmd("backup", b.backupRunE)
	b.org.register(cmd, true)
	cmd.Flags().StringVar(&b.bucketID, "bucket-id", "", "The ID of the bucket to backup")
	cmd.Flags().StringVarP(&b.bucketName, "bucket", "b", "", "The name of the bucket to backup")
	cmd.Use = "backup [flags] path"
	cmd.Args = func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("must specify output path")
		} else if len(args) > 1 {
			return fmt.Errorf("too many args specified")
		}
		b.path = args[0]
		return nil
	}
	cmd.Short = "Backup database"
	cmd.Long = `
Backs up InfluxDB to a directory.

Examples:
	# backup all data
	influx backup /path/to/backup
`
	return cmd
}

func (b *cmdBackupBuilder) manifestPath() string {
	return fmt.Sprintf("%s.manifest", b.baseName)
}

func (b *cmdBackupBuilder) kvPath() string {
	return fmt.Sprintf("%s.bolt", b.baseName)
}

func (b *cmdBackupBuilder) shardPath(id uint64) string {
	return fmt.Sprintf("%s.s%d", b.baseName, id) + ".tar.gz"
}

func (b *cmdBackupBuilder) backupRunE(cmd *cobra.Command, args []string) (err error) {
	ctx := context.Background()

	// Create top level logger
	logconf := influxlogger.NewConfig()
	if b.logger, err = logconf.New(os.Stdout); err != nil {
		return err
	}

	// Determine a base
	b.baseName = time.Now().UTC().Format(influxdb.BackupFilenamePattern)

	// Ensure directory exsits.
	if err := os.MkdirAll(b.path, 0777); err != nil {
		return err
	}

	ac := flags.config()
	b.backupService = &http.BackupService{
		Addr:  ac.Host,
		Token: ac.Token,
	}

	// Back up Bolt database to file.
	if err := b.backupKVStore(ctx); err != nil {
		return err
	}

	// Open bolt DB.
	boltClient := bolt.NewClient(b.logger)
	boltClient.Path = filepath.Join(b.path, b.kvPath())
	if err := boltClient.Open(ctx); err != nil {
		return err
	}
	defer boltClient.Close()

	// Open meta store so we can iterate over meta data.
	b.kvStore = bolt.NewKVStore(b.logger, filepath.Join(b.path, b.kvPath()))
	b.kvStore.WithDB(boltClient.DB())
	b.kvService = kv.NewService(b.logger, b.kvStore, kv.ServiceConfig{})

	b.metaClient = meta.NewClient(meta.NewConfig(), b.kvStore)
	if err := b.metaClient.Open(); err != nil {
		return err
	}

	// Filter through organizations & buckets to backup appropriate shards.
	if err := b.backupOrganizations(ctx); err != nil {
		return err
	}

	if err := b.writeManifest(ctx); err != nil {
		return err
	}

	b.logger.Info("Backup complete")

	return nil
}

// backupKVStore streams the bolt KV file to a file at path.
func (b *cmdBackupBuilder) backupKVStore(ctx context.Context) error {
	path := filepath.Join(b.path, b.kvPath())
	b.logger.Info("Backing up KV store", zap.String("path", b.kvPath()))

	// Open writer to output file.
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Stream bolt file from server, sync, and ensure file closes correctly.
	if err := b.backupService.BackupKVStore(ctx, f); err != nil {
		return err
	} else if err := f.Sync(); err != nil {
		return err
	} else if err := f.Close(); err != nil {
		return err
	}

	// Lookup file size.
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	b.manifest.KV = influxdb.ManifestKVEntry{
		FileName: b.kvPath(),
		Size:     fi.Size(),
	}

	return nil
}

func (b *cmdBackupBuilder) backupOrganizations(ctx context.Context) (err error) {
	// Build a filter if org ID or org name were specified.
	var filter influxdb.OrganizationFilter
	if b.org.id != "" {
		if filter.ID, err = influxdb.IDFromString(b.org.id); err != nil {
			return err
		}
	} else if b.org.name != "" {
		filter.Name = &b.org.name
	}

	// Retrieve a list of all matching organizations.
	orgs, _, err := b.kvService.FindOrganizations(ctx, filter)
	if err != nil {
		return err
	}

	// Back up buckets in each matching organization.
	for _, org := range orgs {
		b.logger.Info("Backing up organization", zap.String("id", org.ID.String()), zap.String("name", org.Name))
		if err := b.backupBuckets(ctx, org); err != nil {
			return err
		}
	}
	return nil
}

func (b *cmdBackupBuilder) backupBuckets(ctx context.Context, org *influxdb.Organization) (err error) {
	// Build a filter if bucket ID or bucket name were specified.
	var filter influxdb.BucketFilter
	filter.OrganizationID = &org.ID
	if b.bucketID != "" {
		if filter.ID, err = influxdb.IDFromString(b.bucketID); err != nil {
			return err
		}
	} else if b.bucketName != "" {
		filter.Name = &b.bucketName
	}

	// Retrieve a list of all matching organizations.
	buckets, _, err := b.kvService.FindBuckets(ctx, filter)
	if err != nil {
		return err
	}

	// Back up shards in each matching bucket.
	for _, bkt := range buckets {
		if err := b.backupBucket(ctx, org, bkt); err != nil {
			return err
		}
	}
	return nil
}

func (b *cmdBackupBuilder) backupBucket(ctx context.Context, org *influxdb.Organization, bkt *influxdb.Bucket) (err error) {
	b.logger.Info("Backing up bucket", zap.String("id", bkt.ID.String()), zap.String("name", bkt.Name))

	// Lookup matching database from the meta store.
	dbi := b.metaClient.Database(bkt.ID.String())
	if dbi == nil {
		return fmt.Errorf("bucket database not found: %s", bkt.ID.String())
	}

	// Iterate over and backup each shard.
	for _, rpi := range dbi.RetentionPolicies {
		for _, sg := range rpi.ShardGroups {
			for _, sh := range sg.Shards {
				if err := b.backupShard(ctx, org, bkt, rpi.Name, sh.ID); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// backupShard streams a tar of TSM data for shard.
func (b *cmdBackupBuilder) backupShard(ctx context.Context, org *influxdb.Organization, bkt *influxdb.Bucket, policy string, shardID uint64) error {
	path := filepath.Join(b.path, b.shardPath(shardID))
	b.logger.Info("Backing up shard", zap.Uint64("id", shardID), zap.String("path", b.shardPath(shardID)))

	// Open writer to output file.
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Wrap file writer with a gzip writer.
	gw := gzip.NewWriter(f)
	defer gw.Close()

	// Stream file from server, sync, and ensure file closes correctly.
	if err := b.backupService.BackupShard(ctx, gw, shardID, time.Time{}); err != nil {
		return err
	} else if err := gw.Close(); err != nil {
		return err
	} else if err := f.Sync(); err != nil {
		return err
	} else if err := f.Close(); err != nil {
		return err
	}

	// Determine file size.
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}

	// Update manifest.
	b.manifest.Files = append(b.manifest.Files, influxdb.ManifestEntry{
		OrganizationID:   org.ID.String(),
		OrganizationName: org.Name,
		BucketID:         bkt.ID.String(),
		BucketName:       bkt.Name,
		ShardID:          shardID,
		FileName:         b.shardPath(shardID),
		Size:             fi.Size(),
		LastModified:     fi.ModTime().UTC(),
	})

	return nil
}

// writeManifest writes the manifest file out.
func (b *cmdBackupBuilder) writeManifest(ctx context.Context) error {
	path := filepath.Join(b.path, b.manifestPath())
	b.logger.Info("Writing manifest", zap.String("path", b.manifestPath()))

	buf, err := json.MarshalIndent(b.manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("create manifest: %w", err)
	}
	buf = append(buf, '\n')
	return ioutil.WriteFile(path, buf, 0600)
}

func (b *cmdBackupBuilder) newCmd(use string, runE func(*cobra.Command, []string) error) *cobra.Command {
	cmd := b.genericCLIOpts.newCmd(use, runE, true)
	b.genericCLIOpts.registerPrintOptions(cmd)
	b.globalFlags.registerFlags(cmd)
	return cmd
}
