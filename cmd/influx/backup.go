package main

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2/backup"
	"github.com/influxdata/influxdb/v2/http"
	influxlogger "github.com/influxdata/influxdb/v2/logger"
	"github.com/spf13/cobra"
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
}

func newCmdBackupBuilder(f *globalFlags, opts genericCLIOpts) *cmdBackupBuilder {
	return &cmdBackupBuilder{
		genericCLIOpts: opts,
		globalFlags:    f,
	}
}

func (b *cmdBackupBuilder) cmdBackup() *cobra.Command {
	cmd := b.newCmd("backup", b.backupRunE)
	b.org.register(b.viper, cmd, true)
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

func (b *cmdBackupBuilder) backupRunE(cmd *cobra.Command, _ []string) error {
	// Create top level logger
	logconf := influxlogger.NewConfig()
	log, err := logconf.New(cmd.OutOrStdout())
	if err != nil {
		return err
	}

	ac := flags.config()
	backupService := &http.BackupService{
		Addr:               ac.Host,
		Token:              ac.Token,
		InsecureSkipVerify: flags.skipVerify,
	}

	var orgID platform.ID
	if b.org.id != "" {
		if err := orgID.DecodeFromString(b.org.id); err != nil {
			return err
		}
	}

	var bucketID platform.ID
	if b.bucketID != "" {
		if err := bucketID.DecodeFromString(b.bucketID); err != nil {
			return err
		}
	}

	req := backup.Request{
		OrgID:    orgID,
		Org:      b.org.name,
		BucketID: bucketID,
		Bucket:   b.bucketName,
		Path:     b.path,
	}

	if err := backup.RunBackup(context.Background(), req, backupService, log); err != nil {
		return err
	}
	return nil
}

func (b *cmdBackupBuilder) newCmd(use string, runE func(*cobra.Command, []string) error) *cobra.Command {
	cmd := b.genericCLIOpts.newCmd(use, runE, true)
	b.genericCLIOpts.registerPrintOptions(cmd)
	b.globalFlags.registerFlags(b.viper, cmd)
	return cmd
}
