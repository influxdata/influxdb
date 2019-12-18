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

type deleteCmdFlags struct {
	http.DeleteRequest
	organization
}

var deleteFlags deleteCmdFlags

func init() {
	deleteFlags.organization.register(deleteCmd)

	viper.BindEnv("BUCKET_ID")
	if h := viper.GetString("BUCKET_ID"); h != "" {
		deleteFlags.BucketID = h
	}
	deleteCmd.PersistentFlags().StringVar(&deleteFlags.DeleteRequest.BucketID, "bucket-id", "", "The ID of destination bucket")

	viper.BindEnv("BUCKET_NAME")
	if h := viper.GetString("BUCKET_NAME"); h != "" {
		deleteFlags.Bucket = h
	}
	deleteCmd.PersistentFlags().StringVarP(&deleteFlags.DeleteRequest.Bucket, "bucket", "b", "", "The name of destination bucket")

	deleteCmd.PersistentFlags().StringVarP(&deleteFlags.DeleteRequest.Start, "start", "", "", "the start time in RFC3339Nano format, exp 2009-01-02T23:00:00Z")
	deleteCmd.PersistentFlags().StringVarP(&deleteFlags.DeleteRequest.Stop, "stop", "", "", "the stop time in RFC3339Nano format, exp 2009-01-02T23:00:00Z")
	deleteCmd.PersistentFlags().StringVarP(&deleteFlags.DeleteRequest.Predicate, "predicate", "p", "", "sql like predicate string, exp 'tag1=\"v1\" and (tag2=123)'")
}

func fluxDeleteF(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if err := deleteFlags.organization.validOrgFlags(); err != nil {
		return err
	}

	s := &http.DeleteService{
		Addr:               flags.host,
		Token:              flags.token,
		InsecureSkipVerify: flags.skipVerify,
	}

	ctx = signals.WithStandardSignals(ctx)
	if err := s.DeleteBucketRangePredicate(ctx, deleteFlags.DeleteRequest); err != nil && err != context.Canceled {
		return fmt.Errorf("failed to delete data: %v", err)
	}

	return nil
}
