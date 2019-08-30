package store

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb/storage"
	"github.com/spf13/cobra"
)

var seriesCommand = &cobra.Command{
	Use:  "series",
	RunE: seriesFE,
}

var seriesFlags struct {
	orgBucket
	count int
	print bool
	warm  bool
}

func init() {
	seriesFlags.orgBucket.AddFlags(seriesCommand)
	flagSet := seriesCommand.Flags()
	flagSet.IntVar(&seriesFlags.count, "count", 1, "Number of times to run benchmark")
	flagSet.BoolVar(&seriesFlags.print, "print", false, "Print series to STDOUT")
	flagSet.BoolVar(&seriesFlags.warm, "warm", false, "Warm the series file before benchmarking")
	RootCommand.AddCommand(seriesCommand)
}

func seriesFE(_ *cobra.Command, _ []string) error {
	ctx := context.Background()

	engine, err := newEngine(ctx)
	if err != nil {
		return err
	}
	defer engine.Close()

	name, err := seriesFlags.Name()
	if err != nil {
		return err
	}

	req := storage.SeriesCursorRequest{
		Name: name,
	}

	benchFn := func(stats bool) {
		cur, err := engine.CreateSeriesCursor(ctx, req, nil)
		if err != nil {
			panic(err)
		}
		defer cur.Close()

		out := seriesFlags.print

		var (
			series int
		)

		if stats {
			start := time.Now()
			defer func() {
				dur := time.Since(start)
				tw := tabwriter.NewWriter(os.Stdout, 10, 4, 0, ' ', 0)
				fmt.Fprintf(tw, "Series:\t%d\n", series)
				fmt.Fprintf(tw, "Time:\t%0.0fms\n", dur.Seconds()*1000)
				fmt.Fprintf(tw, "Series/s:\t%0.3f\n", float64(series)/dur.Seconds())
				tw.Flush()
				fmt.Println()
			}()
		}

		for {
			if row, err := cur.Next(); err != nil {
				panic(err)
			} else if row == nil {
				break
			} else if out {
				fmt.Println(row.Tags.String())
			}
			series++
		}
	}

	if seriesFlags.warm {
		benchFn(false)
	}

	stop := storeFlags.profile.Start()
	defer stop()

	for i := seriesFlags.count; i > 0; i-- {
		benchFn(true)
	}

	return nil
}
