package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/http"
	influxlogger "github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/restore"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/spf13/cobra"
)

func cmdRestore(f *globalFlags, opts genericCLIOpts) *cobra.Command {
	return newCmdRestoreBuilder(f, opts).cmdRestore()
}

type cmdRestoreBuilder struct {
	genericCLIOpts
	*globalFlags

	full          bool
	bucketID      string
	bucketName    string
	newBucketName string
	newOrgName    string
	org           organization
	path          string
}

func newCmdRestoreBuilder(f *globalFlags, opts genericCLIOpts) *cmdRestoreBuilder {
	return &cmdRestoreBuilder{
		genericCLIOpts: opts,
		globalFlags:    f,
	}
}

func (b *cmdRestoreBuilder) cmdRestore() *cobra.Command {
	cmd := b.newCmd("restore", b.restoreRunE)
	b.org.register(b.viper, cmd, true)
	cmd.Flags().BoolVar(&b.full, "full", false, "Fully restore and replace all data on server")
	cmd.Flags().StringVar(&b.bucketID, "bucket-id", "", "The ID of the bucket to restore")
	cmd.Flags().StringVarP(&b.bucketName, "bucket", "b", "", "The name of the bucket to restore")
	cmd.Flags().StringVar(&b.newBucketName, "new-bucket", "", "The name of the bucket to restore to")
	cmd.Flags().StringVar(&b.newOrgName, "new-org", "", "The name of the organization to restore to")
	cmd.Flags().StringVar(&b.path, "input", "", "Local backup data path")
	cmd.Flags().MarkDeprecated("input", "pass backup data path as a positional argument instead")
	cmd.Use = "restore [flags] path"
	cmd.Args = func(cmd *cobra.Command, args []string) error {
		// Legacy: path set by --input flag.
		if b.path != "" {
			if len(args) != 0 {
				return errors.New("cannot specify backup directory using both --input and a positional argument")
			}
			return nil
		}

		if len(args) == 0 {
			return errors.New("must specify path to backup directory")
		} else if len(args) > 1 {
			return errors.New("only one backup directory can be specified at a time")
		}
		b.path = args[0]
		return nil
	}
	cmd.Short = "Restores a backup directory to InfluxDB."
	cmd.Long = `
Restore influxdb.

Examples:
	# restore all data
	influx restore /path/to/restore
`
	return cmd
}

func (b *cmdRestoreBuilder) restoreRunE(cmd *cobra.Command, args []string) (err error) {
	// Create top level logger
	logconf := influxlogger.NewConfig()
	logger, err := logconf.New(os.Stdout)
	if err != nil {
		return err
	}

	// Ensure org/bucket filters are set if a new org/bucket name is specified.
	if b.newOrgName != "" && b.org.id == "" && b.org.name == "" {
		return fmt.Errorf("must specify source org id or name when renaming restored org")
	} else if b.newBucketName != "" && b.bucketID == "" && b.bucketName == "" {
		return fmt.Errorf("must specify source bucket id or name when renaming restored bucket")
	}

	ac := flags.config()
	client, err := newHTTPClient()
	if err != nil {
		return err
	}

	services := restore.Services{
		RestoreService: &http.RestoreService{
			Addr:               ac.Host,
			Token:              ac.Token,
			InsecureSkipVerify: flags.skipVerify,
		},
		OrgService:    &tenant.OrgClientService{Client: client},
		BucketService: &tenant.BucketClientService{Client: client},
	}

	var orgID influxdb.ID
	if b.org.id != "" {
		if err := orgID.DecodeFromString(b.org.id); err != nil {
			return err
		}
	}

	var bucketID influxdb.ID
	if b.bucketID != "" {
		if err := bucketID.DecodeFromString(b.bucketID); err != nil {
			return err
		}
	}

	request := restore.Request{
		OrgID:         orgID,
		Org:           b.org.name,
		NewOrgName:    b.newOrgName,
		BucketID:      bucketID,
		Bucket:        b.bucketName,
		NewBucketName: b.newBucketName,
		Path:          b.path,
		Full:          b.full,
	}

	return restore.RunRestore(context.Background(), request, services, logger)
}

func (b *cmdRestoreBuilder) newCmd(use string, runE func(*cobra.Command, []string) error) *cobra.Command {
	cmd := b.genericCLIOpts.newCmd(use, runE, true)
	b.genericCLIOpts.registerPrintOptions(cmd)
	b.globalFlags.registerFlags(b.viper, cmd)
	return cmd
}
