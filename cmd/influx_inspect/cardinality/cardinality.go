package cardinality

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/influxdata/influxdb/cmd/influx_inspect/cardinality/aggregators"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/reporthelper"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"golang.org/x/sync/errgroup"
)

// Command represents the program execution for "influxd cardinality".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	dbPath     string
	shardPaths map[uint64]string
	exact      bool
	detailed   bool
	// How many goroutines to dedicate to calculating cardinality.
	concurrency int
	// t, d, r, m for Total, Database, Retention Policy, Measurement
	rollup string
}

// NewCommand returns a new instance of Command with default setting applied.
func NewCommand() *Command {
	return &Command{
		Stderr:      os.Stderr,
		Stdout:      os.Stdout,
		shardPaths:  map[uint64]string{},
		concurrency: 1,
		detailed:    false,
		rollup:      "m",
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) (err error) {
	var legalRollups = map[string]int{"m": 3, "r": 2, "d": 1, "t": 0}
	fs := flag.NewFlagSet("report-db", flag.ExitOnError)
	fs.StringVar(&cmd.dbPath, "db-path", "", "Path to database. Required.")
	fs.IntVar(&cmd.concurrency, "c", 1, "Set worker concurrency. Defaults to one.")
	fs.BoolVar(&cmd.detailed, "detailed", false, "Include counts for fields, tags, ")
	fs.BoolVar(&cmd.exact, "exact", false, "Report exact counts")
	fs.StringVar(&cmd.rollup, "rollup", "m", "Rollup level - t: total, d: database, r: retention policy, m: measurement")
	fs.SetOutput(cmd.Stdout)
	if err := fs.Parse(args); err != nil {
		return err
	}

	if cmd.dbPath == "" {
		return errors.New("path to database must be provided")
	}

	totalDepth, ok := legalRollups[cmd.rollup]

	if !ok {
		return fmt.Errorf("invalid rollup specified: %q", cmd.rollup)
	}

	factory := aggregators.CreateNodeFactory(cmd.detailed, cmd.exact)
	totalsTree := factory.NewNode(totalDepth == 0)

	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(cmd.concurrency)
	processTSM := func(db, rp, id, path string) error {
		file, err := os.OpenFile(path, os.O_RDONLY, 0600)
		if err != nil {
			_, _ = fmt.Fprintf(cmd.Stderr, "error: %s: %v. Skipping.\n", path, err)
			return nil
		}

		reader, err := tsm1.NewTSMReader(file)
		if err != nil {
			_, _ = fmt.Fprintf(cmd.Stderr, "error: %s: %v. Skipping.\n", file.Name(), err)
			// NewTSMReader won't close the file handle on failure, so do it here.
			_ = file.Close()
			return nil
		}
		defer func() {
			// The TSMReader will close the underlying file handle here.
			if err := reader.Close(); err != nil {
				_, _ = fmt.Fprintf(cmd.Stderr, "error closing: %s: %v.\n", file.Name(), err)
			}
		}()

		seriesCount := reader.KeyCount()
		for i := 0; i < seriesCount; i++ {
			func() {
				key, _ := reader.KeyAt(i)
				seriesKey, field, _ := bytes.Cut(key, []byte("#!~#"))
				measurement, tags := models.ParseKey(seriesKey)
				totalsTree.Record(0, totalDepth, db, rp, measurement, key, field, tags)
			}()
		}
		return nil
	}
	done := ctx.Done()
	err = reporthelper.WalkShardDirs(cmd.dbPath, func(db, rp, id, path string) error {
		select {
		case <-done:
			return nil
		default:
			g.Go(func() error {
				return processTSM(db, rp, id, path)
			})
			return nil
		}
	})

	if err != nil {
		_, _ = fmt.Fprintf(cmd.Stderr, "%s: %v\n", cmd.dbPath, err)
		return err
	}
	err = g.Wait()
	if err != nil {
		_, _ = fmt.Fprintf(cmd.Stderr, "%s: %v\n", cmd.dbPath, err)
		return err
	}

	tw := tabwriter.NewWriter(cmd.Stdout, 8, 2, 1, ' ', 0)

	if err = factory.PrintHeader(tw); err != nil {
		return err
	}
	if err = factory.PrintDivider(tw); err != nil {
		return err
	}
	for d, db := range totalsTree.Children() {
		for r, rp := range db.Children() {
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
		if err = db.Print(tw, false, fmt.Sprintf("%q", d), "", ""); err != nil {
			return err
		}
	}
	if err = totalsTree.Print(tw, false, "Total"+factory.EstTitle, "", ""); err != nil {
		return err
	}
	return tw.Flush()
}
