package report_db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/influxdata/influxdb/v2/cmd/influxd/inspect/report_db/aggregators"
	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/reporthelper"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
)

// ReportDB represents the program execution for "influxd report-db".
type ReportDB struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	dbPath   string
	exact    bool
	detailed bool
	// How many goroutines to dedicate to calculating cardinality.
	concurrency int
	// t, d, r, m for Total, Database, Retention Policy, Measurement
	rollup string
}

func NewReportDBCommand(v *viper.Viper) (*cobra.Command, error) {
	flags := &ReportDB{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}

	cmd := &cobra.Command{
		Use:   "report-db",
		Short: "Estimates cloud 2 cardinality for a database",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return reportDBRunE(cmd, flags)
		},
	}
	opts := []cli.Opt{
		{
			DestP:    &flags.dbPath,
			Flag:     "db-path",
			Desc:     "path to database",
			Required: true,
		},
		{
			DestP:   &flags.concurrency,
			Flag:    "c",
			Desc:    "set worker concurrency, defaults to one",
			Default: 1,
		},
		{
			DestP:   &flags.detailed,
			Flag:    "detailed",
			Desc:    "include counts for fields, tags",
			Default: false,
		},
		{
			DestP:   &flags.exact,
			Flag:    "exact",
			Desc:    "report exact counts",
			Default: false,
		},
		{
			DestP:   &flags.rollup,
			Flag:    "rollup",
			Desc:    "rollup level - t: total, b: bucket, r: retention policy, m: measurement",
			Default: "m",
		},
	}
	if err := cli.BindOptions(v, cmd, opts); err != nil {
		return nil, err
	}
	return cmd, nil
}

func reportDBRunE(_ *cobra.Command, reportdb *ReportDB) error {
	var legalRollups = map[string]int{"m": 3, "r": 2, "b": 1, "t": 0}
	if reportdb.dbPath == "" {
		return errors.New("path to database must be provided")
	}

	totalDepth, ok := legalRollups[reportdb.rollup]

	if !ok {
		return fmt.Errorf("invalid rollup specified: %q", reportdb.rollup)
	}

	factory := aggregators.CreateNodeFactory(reportdb.detailed, reportdb.exact)
	totalsTree := factory.NewNode(totalDepth == 0)

	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(reportdb.concurrency)
	processTSM := func(bucket, rp, id, path string) error {
		file, err := os.OpenFile(path, os.O_RDONLY, 0600)
		if err != nil {
			_, _ = fmt.Fprintf(reportdb.Stderr, "error: %s: %v. Skipping.\n", path, err)
			return nil
		}

		reader, err := tsm1.NewTSMReader(file)
		if err != nil {
			_, _ = fmt.Fprintf(reportdb.Stderr, "error: %s: %v. Skipping.\n", file.Name(), err)
			// NewTSMReader won't close the file handle on failure, so do it here.
			_ = file.Close()
			return nil
		}
		defer func() {
			// The TSMReader will close the underlying file handle here.
			if err := reader.Close(); err != nil {
				_, _ = fmt.Fprintf(reportdb.Stderr, "error closing: %s: %v.\n", file.Name(), err)
			}
		}()

		seriesCount := reader.KeyCount()
		for i := 0; i < seriesCount; i++ {
			func() {
				key, _ := reader.KeyAt(i)
				seriesKey, field, _ := bytes.Cut(key, []byte("#!~#"))
				measurement, tags := models.ParseKey(seriesKey)
				totalsTree.Record(0, totalDepth, bucket, rp, measurement, key, field, tags)
			}()
		}
		return nil
	}
	done := ctx.Done()
	err := reporthelper.WalkShardDirs(reportdb.dbPath, func(bucket, rp, id, path string) error {
		select {
		case <-done:
			return nil
		default:
			g.Go(func() error {
				return processTSM(bucket, rp, id, path)
			})
			return nil
		}
	})

	if err != nil {
		_, _ = fmt.Fprintf(reportdb.Stderr, "%s: %v\n", reportdb.dbPath, err)
		return err
	}
	err = g.Wait()
	if err != nil {
		_, _ = fmt.Fprintf(reportdb.Stderr, "%s: %v\n", reportdb.dbPath, err)
		return err
	}

	tw := tabwriter.NewWriter(reportdb.Stdout, 8, 2, 1, ' ', 0)

	if err = factory.PrintHeader(tw); err != nil {
		return err
	}
	if err = factory.PrintDivider(tw); err != nil {
		return err
	}
	for d, bucket := range totalsTree.Children() {
		for r, rp := range bucket.Children() {
			for m, measure := range rp.Children() {
				err = measure.Print(tw, true, fmt.Sprintf("%q", d), fmt.Sprintf("%q", r), fmt.Sprintf("%q", m))
				if err != nil {
					return err
				}
			}
			if err = rp.Print(tw, false, fmt.Sprintf("%q", d), fmt.Sprintf("%q", r), ""); err != nil {
				return err
			}
		}
		if err = bucket.Print(tw, false, fmt.Sprintf("%q", d), "", ""); err != nil {
			return err
		}
	}
	if err = totalsTree.Print(tw, false, "Total"+factory.EstTitle, "", ""); err != nil {
		return err
	}
	return tw.Flush()
}
