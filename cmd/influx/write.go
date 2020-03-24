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

const (
	inputFormatCsv          = "csv"
	inputFormatLineProtocol = "lp"
)

var writeFlags struct {
	OrgID     string
	Org       string
	BucketID  string
	Bucket    string
	Precision string
	Format    string
	File      string
	DryRun    bool
}

func cmdWrite(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := opt.newCmd("write", fluxWriteF, true)
	cmd.Args = cobra.MaximumNArgs(1)
	cmd.Short = "Write points to InfluxDB"
	cmd.Long = `Write data to InfluxDB via stdin, or add an entire file specified with the -f flag`

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
	cmd.Flags().StringVar(&writeFlags.Format, "format", "", "Input format, either lp (Line Protocol) or csv (Comma Separated Values). Defaults to lp unless '.csv' extension")
	cmd.Flags().StringVarP(&writeFlags.File, "file", "f", "", "The path to the file to import")
	cmd.Flags().BoolVar(&writeFlags.DryRun, "dry-run", false, "Write protocol lines to stdout")

	return cmd
}

func fluxWriteF(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	if writeFlags.DryRun {
		flags.Host = "-"
	}

	var bucketID, orgID platform.ID
	// validate flags unless writing to stdout
	if flags.Host != "-" {
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

		bucketID, orgID = buckets[0].ID, buckets[0].OrgID
	}

	var r io.Reader
	if len(args) > 0 && args[0][0] == '@' {
		// backward compatibility
		writeFlags.File = args[0][1:]
	}

	if len(writeFlags.File) > 0 {
		f, err := os.Open(writeFlags.File)
		if err != nil {
			return fmt.Errorf("failed to open %q: %v", writeFlags.File, err)
		}
		defer f.Close()
		r = f
		if len(writeFlags.Format) == 0 && strings.HasSuffix(writeFlags.File, ".csv") {
			writeFlags.Format = inputFormatCsv
		}
	} else if len(args) == 0 || args[0] == "-" {
		r = os.Stdin
	} else {
		r = strings.NewReader(args[0])
	}
	// validate input format
	if len(writeFlags.Format) > 0 && writeFlags.Format != inputFormatLineProtocol && writeFlags.Format != inputFormatCsv {
		return fmt.Errorf("unsupported input format: %s", writeFlags.Format)
	}

	if writeFlags.Format == inputFormatCsv {
		r = write.CsvToProtocolLines(r)
	}

	if flags.Host == "-" {
		// write lines to tdout
		_, err := io.Copy(os.Stdout, r)
		if err != nil {
			return fmt.Errorf("failed: %v", err)
		}
	} else {
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
	}

	return nil
}
