package main

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/kit/signals"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var deleteCmd = &cobra.Command{
	Use:   "delete points from an influxDB bucket",
	Short: "Delete points from influxDB",
	Long: `Delete points from influxDB, by specify start, end time
	and a sql like predicate string.`,
	RunE: wrapCheckSetup(fluxDeleteF),
}

var deleteFlags http.DeleteRequest

func init() {
	deleteCmd.PersistentFlags().StringVar(&deleteFlags.OrgID, "org-id", "", "The ID of the organization that owns the bucket")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		deleteFlags.OrgID = h
	}

	deleteCmd.PersistentFlags().StringVarP(&deleteFlags.Org, "org", "o", "", "The name of the organization that owns the bucket")
	viper.BindEnv("ORG")
	if h := viper.GetString("ORG"); h != "" {
		deleteFlags.Org = h
	}

	deleteCmd.PersistentFlags().StringVar(&deleteFlags.BucketID, "bucket-id", "", "The ID of destination bucket")
	viper.BindEnv("BUCKET_ID")
	if h := viper.GetString("BUCKET_ID"); h != "" {
		deleteFlags.BucketID = h
	}

	deleteCmd.PersistentFlags().StringVarP(&deleteFlags.Bucket, "bucket", "b", "", "The name of destination bucket")
	viper.BindEnv("BUCKET_NAME")
	if h := viper.GetString("BUCKET_NAME"); h != "" {
		deleteFlags.Bucket = h
	}

	deleteCmd.PersistentFlags().StringVarP(&deleteFlags.Start, "start", "", "", "the start time in RFC3339Nano format, exp 2009-01-02T23:00:00Z")
	deleteCmd.PersistentFlags().StringVarP(&deleteFlags.Stop, "stop", "", "", "the stop time in RFC3339Nano format, exp 2009-01-02T23:00:00Z")
	deleteCmd.PersistentFlags().StringVarP(&deleteFlags.Predicate, "predicate", "p", "", "sql like predicate string, exp 'tag1=\"v1\" and (tag2=123)'")
}

func fluxDeleteF(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if deleteFlags.Org == "" && deleteFlags.OrgID == "" {
		return fmt.Errorf("please specify one of org or org-id")
	}

	if deleteFlags.Bucket == "" && deleteFlags.BucketID == "" {
		return fmt.Errorf("please specify one of bucket or bucket-id")
	}

	if deleteFlags.Start == "" || deleteFlags.Stop == "" {
		return fmt.Errorf("both start and stop are required")
	}

	s := &http.DeleteService{
		Addr:  flags.host,
		Token: flags.token,
	}

	ctx = signals.WithStandardSignals(ctx)
	if err := s.DeleteBucketRangePredicate(ctx, deleteFlags); err != nil && err != context.Canceled {
		return fmt.Errorf("failed to delete data: %v", err)
	}

	return nil
}
