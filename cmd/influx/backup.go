package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	"github.com/influxdata/influxdb/http"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/multierr"
)

func cmdBackup() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Backup the data in InfluxDB",
		Long: fmt.Sprintf(
			`Backs up data and meta data for the running InfluxDB instance.
Downloaded files are written to the directory indicated by --path.
The target directory, and any parent directories, are created automatically.
Data file have extension .tsm; meta data is written to %s in the same directory.`,
			bolt.DefaultFilename),
		RunE: backupF,
	}
	opts := flagOpts{
		{
			DestP:    &backupFlags.Path,
			Flag:     "path",
			Short:    'p',
			EnvVar:   "PATH",
			Desc:     "directory path to write backup files to",
			Required: true,
		},
	}
	opts.mustRegister(cmd)

	return cmd
}

var backupFlags struct {
	Path string
}

func init() {
	err := viper.BindEnv("PATH")
	if err != nil {
		panic(err)
	}
	if h := viper.GetString("PATH"); h != "" {
		backupFlags.Path = h
	}
}

func newBackupService() (influxdb.BackupService, error) {
	return &http.BackupService{
		Addr:  flags.host,
		Token: flags.token,
	}, nil
}

func backupF(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if flags.local {
		return fmt.Errorf("local flag not supported for backup command")
	}

	if backupFlags.Path == "" {
		return fmt.Errorf("must specify path")
	}

	err := os.MkdirAll(backupFlags.Path, 0770)
	if err != nil && !os.IsExist(err) {
		return err
	}

	backupService, err := newBackupService()
	if err != nil {
		return err
	}

	id, backupFilenames, err := backupService.CreateBackup(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("Backup ID %d contains %d files\n", id, len(backupFilenames))

	for _, backupFilename := range backupFilenames {
		dest := filepath.Join(backupFlags.Path, backupFilename)
		w, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660)
		if err != nil {
			return err
		}
		err = backupService.FetchBackupFile(ctx, id, backupFilename, w)
		if err != nil {
			return multierr.Append(err, w.Close())
		}
		if err = w.Close(); err != nil {
			return err
		}
	}

	fmt.Printf("Backup complete")

	return nil
}
