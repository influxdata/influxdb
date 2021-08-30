package upgrade

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/influxdata/influx-cli/v2/clients"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/pkg/fs"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"go.uber.org/zap"
)

// upgradeDatabases creates databases, buckets, retention policies and shard info according to 1.x meta and copies data
func upgradeDatabases(ctx context.Context, cli clients.CLI, v1 *influxDBv1, v2 *influxDBv2, opts *options, orgID platform.ID, log *zap.Logger) (map[string][]platform.ID, error) {
	v1opts := opts.source
	v2opts := opts.target
	db2BucketIds := make(map[string][]platform.ID)

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
	if err := checkDiskSpace(cli, opts, log); err != nil {
		return nil, err
	}

	cqFile, err := os.OpenFile(v2opts.cqPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening file for CQ export %s: %w", v2opts.cqPath, err)
	}
	defer cqFile.Close()

	log.Info("Upgrading databases")
	// read each database / retention policy from v1.meta and create bucket db-name/rp-name
	// create database in v2.meta
	// copy shard info from v1.meta
	// export any continuous queries
	for _, db := range v1.meta.Databases() {
		if db.Name == "_internal" {
			log.Debug("Skipping _internal ")
			continue
		}
		log.Debug("Upgrading database", zap.String("database", db.Name))

		// db to buckets IDs mapping
		db2BucketIds[db.Name] = make([]platform.ID, 0, len(db.RetentionPolicies))

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
			log.Debug("Creating bucket", zap.String("Bucket", bucket.Name))
			err = v2.bucketSvc.CreateBucket(ctx, bucket)
			if err != nil {
				return nil, fmt.Errorf("error creating bucket %s: %w", bucket.Name, err)

			}

			db2BucketIds[db.Name] = append(db2BucketIds[db.Name], bucket.ID)
			log.Debug("Creating database with retention policy", zap.String("database", bucket.ID.String()))
			spec := rp.ToSpec()
			spec.Name = meta.DefaultRetentionPolicyName
			dbv2, err := v2.meta.CreateDatabaseWithRetentionPolicy(bucket.ID.String(), spec)
			if err != nil {
				return nil, fmt.Errorf("error creating database %s: %w", bucket.ID.String(), err)
			}

			mapping := &influxdb.DBRPMapping{
				Database:        db.Name,
				RetentionPolicy: rp.Name,
				Default:         db.DefaultRetentionPolicy == rp.Name,
				OrganizationID:  orgID,
				BucketID:        bucket.ID,
			}
			log.Debug(
				"Creating mapping",
				zap.String("database", mapping.Database),
				zap.String("retention policy", mapping.RetentionPolicy),
				zap.String("orgID", mapping.OrganizationID.String()),
				zap.String("bucketID", mapping.BucketID.String()),
			)
			err = v2.dbrpSvc.Create(ctx, mapping)
			if err != nil {
				return nil, fmt.Errorf("error creating mapping  %s/%s -> Org %s, bucket %s: %w", mapping.Database, mapping.RetentionPolicy, mapping.OrganizationID.String(), mapping.BucketID.String(), err)
			}
			shardsNum := 0
			for _, sg := range rp.ShardGroups {
				log.Debug(
					"Creating shard group",
					zap.String("database", dbv2.Name),
					zap.String("retention policy", dbv2.DefaultRetentionPolicy),
					zap.Time("time", sg.StartTime),
				)
				shardsNum += len(sg.Shards)
				_, err := v2.meta.CreateShardGroupWithShards(dbv2.Name, dbv2.DefaultRetentionPolicy, sg.StartTime, sg.Shards)
				if err != nil {
					return nil, fmt.Errorf("error creating database %s: %w", bucket.ID.String(), err)
				}
			}
			//empty retention policy doesn't have data
			if shardsNum > 0 {
				targetPath := filepath.Join(targetDataPath, dbv2.Name, spec.Name)
				log.Debug(
					"Copying data",
					zap.String("source", sourcePath),
					zap.String("target", targetPath),
				)
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
				log.Debug(
					"Copying wal",
					zap.String("source", sourcePath),
					zap.String("target", targetPath),
				)
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

		// Output CQs in the same format as SHOW CONTINUOUS QUERIES
		_, err := cqFile.WriteString(fmt.Sprintf("name: %s\n", db.Name))
		if err != nil {
			return nil, err
		}
		maxNameLen := 4 // 4 == len("name"), the column header
		for _, cq := range db.ContinuousQueries {
			if len(cq.Name) > maxNameLen {
				maxNameLen = len(cq.Name)
			}
		}

		headerPadding := maxNameLen - 4 + 1
		_, err = cqFile.WriteString(fmt.Sprintf("name%[1]squery\n----%[1]s-----\n", strings.Repeat(" ", headerPadding)))
		if err != nil {
			return nil, err
		}

		for _, cq := range db.ContinuousQueries {
			log.Debug("Exporting CQ", zap.String("db", db.Name), zap.String("cq_name", cq.Name))
			padding := maxNameLen - len(cq.Name) + 1

			_, err := cqFile.WriteString(fmt.Sprintf("%s%s%s\n", cq.Name, strings.Repeat(" ", padding), cq.Query))
			if err != nil {
				return nil, fmt.Errorf("error exporting continuous query %s from DB %s: %w", cq.Name, db.Name, err)
			}
		}
		_, err = cqFile.WriteString("\n")
		if err != nil {
			return nil, err
		}
	}

	log.Info("Database upgrade complete", zap.Int("upgraded_count", len(db2BucketIds)))
	return db2BucketIds, nil
}

// checkDiskSpace ensures there is enough room at the target path to store
// a full copy of all V1 data.
func checkDiskSpace(cli clients.CLI, opts *options, log *zap.Logger) error {
	log.Info("Checking available disk space")

	size, err := DirSize(opts.source.dataDir)
	if err != nil {
		return fmt.Errorf("error getting size of %s: %w", opts.source.dataDir, err)
	}

	walSize, err := DirSize(opts.source.walDir)
	if err != nil {
		return fmt.Errorf("error getting size of %s: %w", opts.source.walDir, err)
	}
	size += walSize

	v2dir := filepath.Dir(opts.target.boltPath)
	diskInfo, err := fs.DiskUsage(v2dir)
	if err != nil {
		return fmt.Errorf("error getting info of disk %s: %w", v2dir, err)
	}

	freeBytes := humanize.Bytes(diskInfo.Free)
	requiredBytes := humanize.Bytes(size)
	log.Info("Computed disk space", zap.String("free", freeBytes), zap.String("required", requiredBytes))

	if size > diskInfo.Free {
		return fmt.Errorf("not enough space on target disk of %s: need %d, available %d", v2dir, size, diskInfo.Free)
	}
	if !opts.force {
		if confirmed := cli.StdIO.GetConfirm(fmt.Sprintf(`Proceeding will copy all V1 data to %q
  Space available: %s
  Space required:  %s
`, v2dir, freeBytes, requiredBytes)); !confirmed {
			return errors.New("upgrade was canceled")
		}
	}
	return nil
}
