package upgrade

import (
	"context"
	"fmt"
	"github.com/dustin/go-humanize"
	"path/filepath"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/pkg/fs"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"go.uber.org/zap"
)

// upgradeDatabases creates databases, buckets, retention policies and shard info according to 1.x meta and copies data
func upgradeDatabases(ctx context.Context, v1 *influxDBv1, v2 *influxDBv2, v1opts *optionsV1, v2opts *optionsV2, orgID influxdb.ID, log *zap.Logger) (map[string][]influxdb.ID, error) {
	db2BucketIds := make(map[string][]influxdb.ID)

	targetDataPath := filepath.Join(v2opts.enginePath, "data")
	targetWalPath := filepath.Join(v2opts.enginePath, "wal")
	dirFilterFunc := func(path string) bool {
		base := filepath.Base(path)
		if base == "_series" ||
			(len(base) > 0 && base[0] == '_') || //skip internal databases
			base == "index" {
			return true
		}
		return false
	}
	if len(v1.meta.Databases()) == 0 {
		log.Info("No database found in the 1.x meta")
		return db2BucketIds, nil
	}
	// Check space
	log.Info("Checking space")
	size, err := DirSize(v1opts.dataDir)
	if err != nil {
		return nil, fmt.Errorf("error getting size of %s: %w", v1opts.dataDir, err)
	}
	size2, err := DirSize(v1opts.walDir)
	if err != nil {
		return nil, fmt.Errorf("error getting size of %s: %w", v1opts.walDir, err)
	}
	size += size2
	v2dir := filepath.Dir(v2opts.boltPath)
	diskInfo, err := fs.DiskUsage(v2dir)
	if err != nil {
		return nil, fmt.Errorf("error getting info of disk %s: %w", v2dir, err)
	}
	if options.verbose {
		log.Info("Disk space info",
			zap.String("Free space", humanize.Bytes(diskInfo.Free)),
			zap.String("Requested space", humanize.Bytes(size)))
	}
	if size > diskInfo.Free {
		return nil, fmt.Errorf("not enough space on target disk of %s: need %d, available %d ", v2dir, size, diskInfo.Free)
	}
	log.Info("Upgrading databases")
	// read each database / retention policy from v1.meta and create bucket db-name/rp-name
	// create database in v2.meta
	// copy shard info from v1.meta
	for _, db := range v1.meta.Databases() {
		if db.Name == "_internal" {
			if options.verbose {
				log.Info("Skipping _internal ")
			}
			continue
		}
		if options.verbose {
			log.Info("Upgrading database ",
				zap.String("database", db.Name))
		}

		// db to buckets IDs mapping
		db2BucketIds[db.Name] = make([]influxdb.ID, 0, len(db.RetentionPolicies))

		for _, rp := range db.RetentionPolicies {
			sourcePath := filepath.Join(v1opts.dataDir, db.Name, rp.Name)

			bucket := &influxdb.Bucket{
				OrgID:               orgID,
				Type:                influxdb.BucketTypeUser,
				Name:                db.Name + "/" + rp.Name,
				Description:         fmt.Sprintf("Upgraded from v1 database %s with retention policy %s", db.Name, rp.Name),
				RetentionPolicyName: rp.Name,
				RetentionPeriod:     rp.Duration,
			}
			if options.verbose {
				log.Info("Creating bucket ",
					zap.String("Bucket", bucket.Name))
			}
			err = v2.bucketSvc.CreateBucket(ctx, bucket)
			if err != nil {
				return nil, fmt.Errorf("error creating bucket %s: %w", bucket.Name, err)

			}

			db2BucketIds[db.Name] = append(db2BucketIds[db.Name], bucket.ID)
			if options.verbose {
				log.Info("Creating database with retention policy",
					zap.String("database", bucket.ID.String()))
			}
			spec := rp.ToSpec()
			spec.Name = meta.DefaultRetentionPolicyName
			dbv2, err := v2.meta.CreateDatabaseWithRetentionPolicy(bucket.ID.String(), spec)
			if err != nil {
				return nil, fmt.Errorf("error creating database %s: %w", bucket.ID.String(), err)
			}

			mapping := &influxdb.DBRPMappingV2{
				Database:        db.Name,
				RetentionPolicy: rp.Name,
				Default:         db.DefaultRetentionPolicy == rp.Name,
				OrganizationID:  orgID,
				BucketID:        bucket.ID,
			}
			if options.verbose {
				log.Info("Creating mapping",
					zap.String("database", mapping.Database),
					zap.String("retention policy", mapping.RetentionPolicy),
					zap.String("orgID", mapping.OrganizationID.String()),
					zap.String("bucketID", mapping.BucketID.String()))
			}
			err = v2.dbrpSvc.Create(ctx, mapping)
			if err != nil {
				return nil, fmt.Errorf("error creating mapping  %s/%s -> Org %s, bucket %s: %w", mapping.Database, mapping.RetentionPolicy, mapping.OrganizationID.String(), mapping.BucketID.String(), err)
			}
			shardsNum := 0
			for _, sg := range rp.ShardGroups {
				if options.verbose {
					log.Info("Creating shard group",
						zap.String("database", dbv2.Name),
						zap.String("retention policy", dbv2.DefaultRetentionPolicy),
						zap.Time("time", sg.StartTime))
				}
				shardsNum += len(sg.Shards)
				_, err := v2.meta.CreateShardGroupWithShards(dbv2.Name, dbv2.DefaultRetentionPolicy, sg.StartTime, sg.Shards)
				if err != nil {
					return nil, fmt.Errorf("error creating database %s: %w", bucket.ID.String(), err)
				}
			}
			//empty retention policy doesn't have data
			if shardsNum > 0 {
				targetPath := filepath.Join(targetDataPath, dbv2.Name, spec.Name)
				if options.verbose {
					log.Info("Copying data",
						zap.String("source", sourcePath),
						zap.String("target", targetPath))
				}
				err = CopyDir(sourcePath,
					targetPath,
					nil,
					dirFilterFunc,
					nil)
				if err != nil {
					return nil, fmt.Errorf("error copying v1 data from %s to %s: %w", sourcePath, targetPath, err)
				}
				sourcePath = filepath.Join(v1opts.walDir, db.Name, rp.Name)
				targetPath = filepath.Join(targetWalPath, dbv2.Name, spec.Name)
				if options.verbose {
					log.Info("Copying wal",
						zap.String("source", sourcePath),
						zap.String("target", targetPath))
				}
				err = CopyDir(sourcePath,
					targetPath,
					nil,
					dirFilterFunc,
					nil)
				if err != nil {
					return nil, fmt.Errorf("error copying v1 data from %s to %s: %w", sourcePath, targetPath, err)
				}
			} else {
				log.Warn("Empty retention policy")
			}
		}
	}

	return db2BucketIds, nil
}
