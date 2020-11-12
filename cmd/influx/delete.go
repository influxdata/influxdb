package main

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/kit/signals"
	"github.com/spf13/cobra"
)

func cmdDelete(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	builder := &cmdDeleteBuilder{
		genericCLIOpts: opt,
		globalFlags:    f,
	}
	return builder.cmd()
}

type cmdDeleteBuilder struct {
	genericCLIOpts
	*globalFlags

	flags http.DeleteRequest
}

func (b *cmdDeleteBuilder) cmd() *cobra.Command {
	cmd := b.newCmd("delete", b.fluxDeleteF)
	cmd.Short = "Delete points from influxDB"
	cmd.Long = `Delete points from influxDB, by specify start, end time
	and a sql like predicate string.`

	opts := flagOpts{
		{
			DestP:      &b.flags.OrgID,
			Flag:       "org-id",
			Desc:       "The ID of the organization that owns the bucket",
			Persistent: true,
		},
		{
			DestP:      &b.flags.Org,
			Flag:       "org",
			Short:      'o',
			Desc:       "The name of the organization that owns the bucket",
			Persistent: true,
		},
		{
			DestP:      &b.flags.BucketID,
			Flag:       "bucket-id",
			Desc:       "The ID of the destination bucket",
			Persistent: true,
		},
		{
			DestP:      &b.flags.Bucket,
			Flag:       "bucket",
			Desc:       "The name of destination bucket",
			EnvVar:     "BUCKET_NAME",
			Persistent: true,
		},
	}
	opts.mustRegister(b.viper, cmd)

	cmd.PersistentFlags().StringVar(&b.flags.Start, "start", "", "the start time in RFC3339Nano format, exp 2009-01-02T23:00:00Z")
	cmd.PersistentFlags().StringVar(&b.flags.Stop, "stop", "", "the stop time in RFC3339Nano format, exp 2009-01-02T23:00:00Z")
	cmd.PersistentFlags().StringVarP(&b.flags.Predicate, "predicate", "p", "", "sql like predicate string, exp 'tag1=\"v1\" and (tag2=123)'")

	return cmd
}

func (b *cmdDeleteBuilder) fluxDeleteF(cmd *cobra.Command, args []string) error {
	ac := b.globalFlags.config()

	org := b.flags.Org
	if org == "" {
		org = ac.Org
	}
	if org == "" && b.flags.OrgID == "" {
		return fmt.Errorf("please specify one of org or org-id")
	}

	if b.flags.Bucket == "" && b.flags.BucketID == "" {
		return fmt.Errorf("please specify one of bucket or bucket-id")
	}

	if b.flags.Start == "" || b.flags.Stop == "" {
		return fmt.Errorf("both start and stop are required")
	}

	s := &http.DeleteService{
		Addr:               ac.Host,
		Token:              ac.Token,
		InsecureSkipVerify: flags.skipVerify,
	}

	ctx := signals.WithStandardSignals(context.Background())
	if err := s.DeleteBucketRangePredicate(ctx, b.flags); err != nil && err != context.Canceled {
		return fmt.Errorf("failed to delete data: %v", err)
	}

	return nil
}

func (b *cmdDeleteBuilder) newCmd(use string, runE func(*cobra.Command, []string) error) *cobra.Command {
	cmd := b.genericCLIOpts.newCmd(use, runE, true)
	b.globalFlags.registerFlags(b.viper, cmd)
	return cmd
}
