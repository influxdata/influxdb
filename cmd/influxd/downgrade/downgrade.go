package downgrade

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/influxdata/influxdb/v2/kv/migration"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	influxlogger "github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/sqlite"
	sqliteMigrations "github.com/influxdata/influxdb/v2/sqlite/migrations"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type migrationTarget struct {
	kvMigration, sqlMigration int
}

var downgradeMigrationTargets = map[string]migrationTarget{
	"2.0": {kvMigration: 15, sqlMigration: 0},
}

func NewCommand(ctx context.Context, v *viper.Viper) (*cobra.Command, error) {
	v2dir, err := fs.InfluxDir()
	if err != nil {
		return nil, fmt.Errorf("error fetching default InfluxDB 2.0 dir: %w", err)
	}

	var validDowngradeTargets []string
	for k := range downgradeMigrationTargets {
		validDowngradeTargets = append(validDowngradeTargets, k)
	}
	var validTargetsHelp string
	if len(validDowngradeTargets) == 1 {
		validTargetsHelp = validDowngradeTargets[0]
	} else {
		validTargetsHelp = fmt.Sprintf("<%s>", strings.Join(validDowngradeTargets, "|"))
	}

	var boltPath, sqlitePath string
	var logLevel zapcore.Level

	cmd := &cobra.Command{
		Use:   fmt.Sprintf("downgrade [flags] %s", validTargetsHelp),
		Short: "Downgrade metadata schema used by influxd to match the expectations of an older release",
		Long: `Run this command prior to downgrading the influxd binary.

influxd does not guarantee backwards-compatibility with older releases in its embedded
metadata stores. Attempting to boot up an older influxd on a BoltDB/SQLite file that has
been migrated to a newer schema will result in a startup error. This command downgrades
those metadata schemas to match the expectations of an older release, allowing the older
influxd binary to boot successfully.

The target version of the downgrade must be specified, i.e. "influxd downgrade 2.0".
`,
		ValidArgs: validDowngradeTargets,
		Args:      cobra.ExactValidArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			logconf := &influxlogger.Config{
				Format: "auto",
				Level:  logLevel,
			}
			logger, err := logconf.New(os.Stdout)
			if err != nil {
				return err
			}

			return downgrade(ctx, boltPath, sqlitePath, args[0], logger)
		},
	}

	opts := []cli.Opt{
		{
			DestP:   &boltPath,
			Flag:    "bolt-path",
			Default: filepath.Join(v2dir, bolt.DefaultFilename),
			Desc:    "path for boltdb database",
			Short:   'b',
		},
		{
			DestP:   &sqlitePath,
			Flag:    "sqlite-path",
			Default: filepath.Join(v2dir, sqlite.DefaultFilename),
			Desc:    "path for sqlite database",
			Short:   's',
		},
		{
			DestP:   &logLevel,
			Flag:    "log-level",
			Default: zapcore.InfoLevel,
			Desc:    "supported log levels are debug, info, warn and error",
		},
	}
	if err := cli.BindOptions(v, cmd, opts); err != nil {
		return nil, err
	}

	return cmd, nil
}

func downgrade(ctx context.Context, boltPath, sqlitePath, targetVersion string, log *zap.Logger) error {
	if err := downgradeBolt(ctx, boltPath, targetVersion, log); err != nil {
		return err
	}

	if err := downgradeSQLite(ctx, sqlitePath, targetVersion, log); err != nil {
		return err
	}

	log.Info("Metadata successfully downgraded, you can now safely replace this `influxd` with the target older version",
		zap.String("version", targetVersion))
	return nil
}

func downgradeBolt(ctx context.Context, boltPath, targetVersion string, log *zap.Logger) error {
	boltClient := bolt.NewClient(log.With(zap.String("service", "bolt")))
	boltClient.Path = boltPath

	if err := boltClient.Open(ctx); err != nil {
		return fmt.Errorf("failed to open bolt DB: %w", err)
	}
	defer boltClient.Close()

	kvStore := bolt.NewKVStore(log.With(zap.String("service", "kvstore-bolt")), boltPath)
	kvStore.WithDB(boltClient.DB())

	kvMigrator, err := migration.NewMigrator(log.With(zap.String("service", "kv-migrator")), kvStore)
	if err != nil {
		return fmt.Errorf("failed to initialize KV migrator: %w", err)
	}
	info := influxdb.GetBuildInfo()
	kvMigrator.SetBackupPath(fmt.Sprintf("%s.%s-pre-%s-downgrade.backup", boltPath, info.Version, targetVersion))
	kvMigrator.AddMigrations(all.Migrations[:]...)

	log.Info("Downgrading KV metadata to target version", zap.String("version", targetVersion))
	if err := kvMigrator.Down(ctx, downgradeMigrationTargets[targetVersion].kvMigration); err != nil {
		return fmt.Errorf("failed to tear down KV migrations: %w", err)
	}

	return nil
}

func downgradeSQLite(ctx context.Context, sqlitePath, targetVersion string, log *zap.Logger) error {
	sqlStore, err := sqlite.NewSqlStore(sqlitePath, log.With(zap.String("service", "sqlstore-sqlite")))
	if err != nil {
		return fmt.Errorf("failed to initialize SQL migrator: %w", err)
	}

	sqlMigrator := sqlite.NewMigrator(sqlStore, log.With(zap.String("service", "sql-migrator")))
	info := influxdb.GetBuildInfo()
	sqlMigrator.SetBackupPath(fmt.Sprintf("%s.%s-pre-%s-downgrade.backup", sqlitePath, info.Version, targetVersion))

	log.Info("Downgrading SQL metadata to target version", zap.String("version", targetVersion))
	if err := sqlMigrator.Down(ctx, downgradeMigrationTargets[targetVersion].sqlMigration, sqliteMigrations.All); err != nil {
		return fmt.Errorf("failed to tear down SQL migrations: %w", err)
	}

	return nil
}
