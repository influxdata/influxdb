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
)

var writeFlags struct {
	OrgID     string
	Org       string
	BucketID  string
	Bucket    string
	Precision string
}

func cmdWrite(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := opt.newCmd("write line protocol or @/path/to/points.txt", fluxWriteF)
	cmd.Args = cobra.ExactArgs(1)
	cmd.Short = "Write points to InfluxDB"
	cmd.Long = `Write a single line of line protocol to InfluxDB,
or add an entire file specified with an @ prefix.`

	opts := flagOpts{
		{
			DestP:      &writeFlags.OrgID,
			Flag:       "org-id",
			Desc:       "The ID of the organization that owns the bucket",
			Persistent: true,
		},
		{
			DestP:      &writeFlags.Org,
			Flag:       "org",
			Short:      'o',
			Desc:       "The name of the organization that owns the bucket",
			Persistent: true,
		},
		{
			DestP:      &writeFlags.BucketID,
			Flag:       "bucket-id",
			Desc:       "The ID of destination bucket",
			Persistent: true,
		},
		{
			DestP:      &writeFlags.Bucket,
			Flag:       "bucket",
			Short:      'b',
			EnvVar:     "BUCKET_NAME",
			Desc:       "The name of destination bucket",
			Persistent: true,
		},
		{
			DestP:      &writeFlags.Precision,
			Flag:       "precision",
			Short:      'p',
			Default:    "ns",
			Desc:       "Precision of the timestamps of the lines",
			Persistent: true,
		},
	}
	opts.mustRegister(cmd)

	return cmd
}

func fluxWriteF(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if writeFlags.Org != "" && writeFlags.OrgID != "" {
		return fmt.Errorf("please specify one of org or org-id")
	}

	if writeFlags.Bucket != "" && writeFlags.BucketID != "" {
		return fmt.Errorf("please specify one of bucket or bucket-id")
	}

	if !models.ValidPrecision(writeFlags.Precision) {
		return fmt.Errorf("invalid precision")
	}

	bs, err := newBucketService()
	if err != nil {
		return err
	}

	var filter platform.BucketFilter
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
		filter.Org = &writeFlags.Org
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

	bucketID, orgID := buckets[0].ID, buckets[0].OrgID

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
			Addr:               flags.Host,
			Token:              flags.Token,
			Precision:          writeFlags.Precision,
			InsecureSkipVerify: flags.skipVerify,
		},
	}

	ctx = signals.WithStandardSignals(ctx)
	if err := s.Write(ctx, orgID, bucketID, r); err != nil && err != context.Canceled {
		return fmt.Errorf("failed to write data: %v", err)
	}

	return nil
}
