// Package migrate provides a tool to help migrate data from InfluxDB 1.x to 2.x
package migrate

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb/migrate"
	"github.com/spf13/cobra"
)

var Command = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate from InfluxDB >= 1.7.x to InfluxDB 2.x",
	Long: `NOTE!
	
⚠️⚠️ This tool is in a very early prototype stage. It can corrupt your 2.x TSM data.⚠️⚠️

Please see the following issues for progress towards making this tool production
ready:

- Ensure retention policy duration carried over to 2.x bucket: https://github.com/influxdata/influxdb/issues/17257
- Support migrating arbitrary time-ranges: https://github.com/influxdata/influxdb/issues/17249
- Ensure hot shards not migrated by default: https://github.com/influxdata/influxdb/issues/17250
- Handle cases where multiple 1.x shards have different field types for same series: https://github.com/influxdata/influxdb/issues/17251

This tool allows an operator to migrate their TSM data from an OSS 1.x server 
into an OSS 2.x server in an offline manner.

It is very important when running this tool that the 2.x server is not running, 
and ideally the 1.x server is not running, or at least, it is not writing into 
any of the shards that will be migrated.
`,
	Args: cobra.ExactArgs(0),
	RunE: migrateE,
}

var flags struct {
	basePath1x string // base path of 1.x installation
	db         string
	rp         string
	from       string // migrate only data at least as old as this (RFC3339Nano format)
	to         string // migrate only data at least as young as this (RFC3339Nano format)
	migrateHot bool   // migrate hot shards (can leave data behind)
	basePath2x string // base path of 2.x installation (defaults to ~/.influxdbv2)
	destOrg    string // destination 2.x organisation (base-16 format)

	dryRun  bool // enable dry-run mode (don't do any migration)
	verbose bool // enable verbose logging
}

// influx1Dir retrieves the InfluxDB 1.x directory.
func influx1Dir() (string, error) {
	var dir string
	// By default, store meta and data files in current users home directory
	u, err := user.Current()
	if err == nil {
		dir = u.HomeDir
	} else if home := os.Getenv("HOME"); home != "" {
		dir = home
	} else {
		wd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		dir = wd
	}
	dir = filepath.Join(dir, ".influxdb")

	return dir, nil
}

func init() {
	v1Dir, err := influx1Dir()
	if err != nil {
		panic(fmt.Errorf("failed to determine default InfluxDB 1.x directory: %s", err))
	}

	v2Dir, err := fs.InfluxDir()
	if err != nil {
		panic(fmt.Errorf("failed to determine default InfluxDB 2.x directory: %s", err))
	}

	opts := []cli.Opt{
		{
			DestP:   &flags.basePath1x,
			Flag:    "influxdb-1x-path",
			Default: v1Dir,
			Desc:    "path to 1.x InfluxDB",
		},
		{
			DestP:   &flags.basePath2x,
			Flag:    "influxdb-2x-path",
			Default: v2Dir,
			Desc:    "path to 2.x InfluxDB",
		},
		{
			DestP:   &flags.db,
			Flag:    "db",
			Default: "",
			Desc:    "only import the provided 1.x database",
		},
		{
			DestP:   &flags.rp,
			Flag:    "rp",
			Default: "",
			Desc:    "only import the provided 1.x retention policy. --db must be set",
		},
		{
			DestP:   &flags.from,
			Flag:    "from",
			Default: "",
			Desc:    "earliest point to import",
		},
		{
			DestP:   &flags.to,
			Flag:    "to",
			Default: "",
			Desc:    "latest point to import",
		},
		{
			DestP:   &flags.destOrg,
			Flag:    "org-id",
			Default: "",
			Desc:    "destination 2.x organization id (required)",
		},
		{
			DestP:   &flags.dryRun,
			Flag:    "dry-run",
			Default: false,
			Desc:    "simulate migration without running it",
		},
		{
			DestP:   &flags.migrateHot,
			Flag:    "migrate-hot-shards",
			Default: false,
			Desc:    "migrate all shards including hot ones. Can leave unsnapshotted data behind",
		},
		{
			DestP:   &flags.verbose,
			Flag:    "verbose",
			Default: false,
			Desc:    "enable verbose logging",
		},
	}
	cli.BindOptions(Command, opts)
}

func migrateE(cmd *cobra.Command, args []string) error {
	if flags.destOrg == "" {
		return errors.New("destination organization must be set")
	} else if flags.rp != "" && flags.db == "" {
		return errors.New("source database empty. Cannot filter by retention policy")
	}

	destOrg, err := influxdb.IDFromString(flags.destOrg)
	if err != nil {
		return err
	}

	fromNano := models.MinNanoTime
	if flags.from != "" {
		from, err := time.Parse(time.RFC3339Nano, flags.from)
		if err != nil {
			return err
		}
		fromNano = from.UnixNano()

		// TODO: enable this feature...
		_ = fromNano
		return errors.New("--from flag is currently unsupported; only all-time can be migrated")
	}

	toNano := models.MaxNanoTime
	if flags.to != "" {
		to, err := time.Parse(time.RFC3339Nano, flags.to)
		if err != nil {
			return err
		}
		toNano = to.UnixNano()

		// TODO: enable this feature...
		_ = toNano
		return errors.New("--to flag is currently unsupported; only all-time can be migrated")
	}

	// TODO: enable this feature...
	if flags.migrateHot {
		return errors.New("--migrate-hot-shards flag is currently unsupported; fully compact all shards before migrating")
	}

	migrator := migrate.NewMigrator(migrate.Config{
		SourcePath:      flags.basePath1x,
		DestPath:        flags.basePath2x,
		From:            fromNano,
		To:              toNano,
		MigrateHotShard: flags.migrateHot,
		Stdout:          os.Stdout,
		VerboseLogging:  flags.verbose,
		DestOrg:         *destOrg,
		DryRun:          flags.dryRun,
	})
	return migrator.Process1xShards(flags.db, flags.rp)
}
