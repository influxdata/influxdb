package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/kit/signals"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/write"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var writeCmd = &cobra.Command{
	Use:   "write line protocol or @/path/to/points.txt",
	Short: "Write points to InfluxDB",
	Long: `Write a single line of line protocol to InfluxDB,
or add an entire file specified with an @ prefix.`,
	Args: cobra.ExactArgs(1),
	RunE: wrapCheckSetup(fluxWriteF),
}

var writeFlags struct {
	OrgID     string
	Org       string
	BucketID  string
	Bucket    string
	Precision string
}

func init() {
	writeCmd.PersistentFlags().StringVar(&writeFlags.OrgID, "org-id", "", "The ID of the organization that owns the bucket")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		writeFlags.OrgID = h
	}

	writeCmd.PersistentFlags().StringVarP(&writeFlags.Org, "org", "o", "", "The name of the organization that owns the bucket")
	viper.BindEnv("ORG")
	if h := viper.GetString("ORG"); h != "" {
		writeFlags.Org = h
	}

	writeCmd.PersistentFlags().StringVar(&writeFlags.BucketID, "bucket-id", "", "The ID of destination bucket")
	viper.BindEnv("BUCKET_ID")
	if h := viper.GetString("BUCKET_ID"); h != "" {
		writeFlags.BucketID = h
	}

	writeCmd.PersistentFlags().StringVarP(&writeFlags.Bucket, "bucket", "b", "", "The name of destination bucket")
	viper.BindEnv("BUCKET_NAME")
	if h := viper.GetString("BUCKET_NAME"); h != "" {
		writeFlags.Bucket = h
	}

	writeCmd.PersistentFlags().StringVarP(&writeFlags.Precision, "precision", "p", "ns", "Precision of the timestamps of the lines")
	viper.BindEnv("PRECISION")
	if p := viper.GetString("PRECISION"); p != "" {
		writeFlags.Precision = p
	}
}

func fluxWriteF(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if writeFlags.Org != "" && writeFlags.OrgID != "" {
		cmd.Usage()
		return fmt.Errorf("please specify one of org or org-id")
	}

	if writeFlags.Bucket != "" && writeFlags.BucketID != "" {
		cmd.Usage()
		return fmt.Errorf("please specify one of bucket or bucket-id")
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
			return fmt.Errorf("failed to decode bucket-id: %v", err)
		}
	}
	if writeFlags.Bucket != "" {
		filter.Name = &writeFlags.Bucket
	}

	if writeFlags.OrgID != "" {
		filter.OrganizationID, err = platform.IDFromString(writeFlags.OrgID)
		if err != nil {
			return fmt.Errorf("failed to decode org-id id: %v", err)
		}
	}
	if writeFlags.Org != "" {
		filter.Organization = &writeFlags.Org
	}

	buckets, n, err := bs.FindBuckets(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to retrieve buckets: %v", err)
	}

	if n == 0 {
		if writeFlags.Bucket != "" {
			return fmt.Errorf("bucket %q was not found", writeFlags.Bucket)
		}

		if writeFlags.BucketID != "" {
			return fmt.Errorf("bucket with id %q does not exist", writeFlags.BucketID)
		}
	}

	bucketID, orgID := buckets[0].ID, buckets[0].OrganizationID

	var r io.Reader
	if args[0] == "-" {
		r = os.Stdin
	} else if len(args[0]) > 0 && args[0][0] == '@' {
		f, err := os.Open(args[0][1:])
		if err != nil {
			return fmt.Errorf("failed to open %q: %v", args[0][1:], err)
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
	if err := s.Write(ctx, orgID, bucketID, r); err != nil && err != context.Canceled {
		return fmt.Errorf("failed to write data: %v", err)
	}

	return nil
}
