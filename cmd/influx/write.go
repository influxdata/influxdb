package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/http"
	"github.com/influxdata/platform/kit/signals"
	"github.com/influxdata/platform/models"
	"github.com/influxdata/platform/write"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var writeCmd = &cobra.Command{
	Use:   "write line protocol or @/path/to/points.txt",
	Short: "Write points to influxdb",
	Long: `Write a single line of line protocol to influx db,
		or add an entire file specified with an @ prefix`,
	Args: cobra.ExactArgs(1),
	RunE: fluxWriteF,
}

var writeFlags struct {
	OrgID     string
	Org       string
	BucketID  string
	Bucket    string
	Precision string
}

func init() {
	writeCmd.PersistentFlags().StringVar(&writeFlags.OrgID, "org-id", "", "id of the organization that owns the bucket")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		writeFlags.OrgID = h
	}

	writeCmd.PersistentFlags().StringVarP(&writeFlags.Org, "org", "o", "", "name of the organization that owns the bucket")
	viper.BindEnv("ORG")
	if h := viper.GetString("ORG"); h != "" {
		writeFlags.Org = h
	}

	writeCmd.PersistentFlags().StringVar(&writeFlags.BucketID, "bucket-id", "", "ID of destination bucket")
	viper.BindEnv("BUCKET_ID")
	if h := viper.GetString("BUCKET_ID"); h != "" {
		writeFlags.BucketID = h
	}

	writeCmd.PersistentFlags().StringVarP(&writeFlags.Bucket, "bucket", "b", "", "name of destination bucket")
	viper.BindEnv("BUCKET_NAME")
	if h := viper.GetString("BUCKET_NAME"); h != "" {
		writeFlags.Bucket = h
	}

	writeCmd.PersistentFlags().StringVarP(&writeFlags.Precision, "precision", "p", "ns", "precision of the timestamps of the lines")
	viper.BindEnv("PRECISION")
	if p := viper.GetString("PRECISION"); p != "" {
		writeFlags.Precision = p
	}
}

func fluxWriteF(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if writeFlags.Org != "" && writeFlags.OrgID != "" {
		cmd.Usage()
		return fmt.Errorf("Please specify one of org or org-id")
	}

	if writeFlags.Bucket != "" && writeFlags.BucketID != "" {
		cmd.Usage()
		return fmt.Errorf("Please specify one of bucket or bucket-id")
	}

	if !models.ValidPrecision(writeFlags.Precision) {
		cmd.Usage()
		return fmt.Errorf("invalid precision")
	}

	bs := &http.BucketService{
		Addr:  flags.host,
		Token: flags.token,
	}

	var err error
	filter := platform.BucketFilter{}

	if writeFlags.BucketID != "" {
		filter.ID, err = platform.IDFromString(writeFlags.BucketID)
		if err != nil {
			return err
		}
	}
	if writeFlags.Bucket != "" {
		filter.Name = &writeFlags.Bucket
	}

	if writeFlags.OrgID != "" {
		filter.OrganizationID, err = platform.IDFromString(writeFlags.OrgID)
		if err != nil {
			return err
		}
	}
	if writeFlags.Org != "" {
		filter.Organization = &writeFlags.Org
	}

	buckets, n, err := bs.FindBuckets(ctx, filter)
	if err != nil {
		return err
	}

	if n == 0 {
		return fmt.Errorf("bucket does not exist")
	}

	bucketID, orgID := buckets[0].ID, buckets[0].OrganizationID

	var r io.Reader
	if args[0] == "-" {
		r = os.Stdin
	} else if len(args[0]) > 0 && args[0][0] == '@' {
		f, err := os.Open(args[0][1:])
		if err != nil {
			return err
		}
		defer f.Close()
		r = f
	} else {
		r = strings.NewReader(args[0])
	}

	s := write.Batcher{
		Service: &http.WriteService{
			Addr:      flags.host,
			Token:     flags.token,
			Precision: writeFlags.Precision,
		},
	}

	ctx = signals.WithStandardSignals(ctx)
	if err := s.Write(ctx, orgID, bucketID, r); err != context.Canceled {
		return err
	}
	return nil
}
