// Package report reports statistics about TSM files.
package report

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/reporthelper"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/retailnext/hllpp"
)

// Command represents the program execution for "influxd report".
type Command struct {
	Stderr io.Writer
	Stdout io.Writer

	dir             string
	pattern         string
	detailed, exact bool
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	fs.StringVar(&cmd.pattern, "pattern", "", "Include only files matching a pattern")
	fs.BoolVar(&cmd.detailed, "detailed", false, "Report detailed cardinality estimates")
	fs.BoolVar(&cmd.exact, "exact", false, "Report exact counts")

	fs.SetOutput(cmd.Stdout)
	fs.Usage = cmd.printUsage

	if err := fs.Parse(args); err != nil {
		return err
	}

	newCounterFn := NewHLLCounter
	estTitle := " (est)"
	if cmd.exact {
		estTitle = ""
		newCounterFn = NewExactCounter
	}

	cmd.dir = fs.Arg(0)

	err := reporthelper.IsShardDir(cmd.dir)
	if cmd.detailed && err != nil {
		return fmt.Errorf("-detailed only supported for shard dirs")
	}

	totalSeries := newCounterFn()
	tagCardinalities := map[string]Counter{}
	measCardinalities := map[string]Counter{}
	fieldCardinalities := map[string]Counter{}

	dbCardinalities := map[string]Counter{}

	start := time.Now()

	tw := tabwriter.NewWriter(cmd.Stdout, 8, 2, 1, ' ', 0)
	fmt.Fprintln(tw, strings.Join([]string{"DB", "RP", "Shard", "File", "Series", "New" + estTitle, "Min Time", "Max Time", "Load Time"}, "\t"))

	minTime, maxTime := int64(math.MaxInt64), int64(math.MinInt64)
	var fileCount int
	if err := reporthelper.WalkShardDirs(cmd.dir, func(db, rp, id, path string) error {
		if cmd.pattern != "" && !strings.Contains(path, cmd.pattern) {
			return nil
		}

		file, err := os.OpenFile(path, os.O_RDONLY, 0600)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "error: %s: %v. Skipping.\n", path, err)
			return nil
		}

		loadStart := time.Now()
		reader, err := tsm1.NewTSMReader(file)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "error: %s: %v. Skipping.\n", file.Name(), err)
			return nil
		}
		loadTime := time.Since(loadStart)
		fileCount++

		dbCount := dbCardinalities[db]
		if dbCount == nil {
			dbCount = newCounterFn()
			dbCardinalities[db] = dbCount
		}

		oldCount := dbCount.Count()

		seriesCount := reader.KeyCount()
		for i := 0; i < seriesCount; i++ {
			key, _ := reader.KeyAt(i)
			totalSeries.Add([]byte(key))
			dbCount.Add([]byte(key))

			if cmd.detailed {
				seriesKey, field, _ := bytes.Cut(key, []byte("#!~#"))
				measurement, tags := models.ParseKey(seriesKey)

				measCount := measCardinalities[measurement]
				if measCount == nil {
					measCount = newCounterFn()
					measCardinalities[measurement] = measCount
				}
				measCount.Add([]byte(key))

				fieldCount := fieldCardinalities[measurement]
				if fieldCount == nil {
					fieldCount = newCounterFn()
					fieldCardinalities[measurement] = fieldCount
				}
				fieldCount.Add([]byte(field))

				for _, t := range tags {
					tagCount := tagCardinalities[string(t.Key)]
					if tagCount == nil {
						tagCount = newCounterFn()
						tagCardinalities[string(t.Key)] = tagCount
					}
					tagCount.Add(t.Value)
				}
			}
		}
		minT, maxT := reader.TimeRange()
		if minT < minTime {
			minTime = minT
		}
		if maxT > maxTime {
			maxTime = maxT
		}
		reader.Close()

		fmt.Fprintln(tw, strings.Join([]string{
			db, rp, id,
			filepath.Base(file.Name()),
			strconv.FormatInt(int64(seriesCount), 10),
			strconv.FormatInt(int64(dbCount.Count()-oldCount), 10),
			time.Unix(0, minT).UTC().Format(time.RFC3339Nano),
			time.Unix(0, maxT).UTC().Format(time.RFC3339Nano),
			loadTime.String(),
		}, "\t"))
		if cmd.detailed {
			tw.Flush()
		}
		return nil
	}); err != nil {
		return err
	}

	tw.Flush()
	println()

	println("Summary:")
	fmt.Printf("  Files: %d\n", fileCount)
	fmt.Printf("  Time Range: %s - %s\n",
		time.Unix(0, minTime).UTC().Format(time.RFC3339Nano),
		time.Unix(0, maxTime).UTC().Format(time.RFC3339Nano),
	)
	fmt.Printf("  Duration: %s \n", time.Unix(0, maxTime).Sub(time.Unix(0, minTime)))
	println()

	fmt.Printf("Statistics\n")
	fmt.Printf("  Series:\n")
	for db, counts := range dbCardinalities {
		fmt.Printf("     - %s%s: %d (%d%%)\n", db, estTitle, counts.Count(), int(float64(counts.Count())/float64(totalSeries.Count())*100))
	}
	fmt.Printf("  Total%s: %d\n", estTitle, totalSeries.Count())

	if cmd.detailed {
		fmt.Printf("\n  Measurements (est):\n")
		for _, t := range sortKeys(measCardinalities) {
			fmt.Printf("    - %v: %d (%d%%)\n", t, measCardinalities[t].Count(), int((float64(measCardinalities[t].Count())/float64(totalSeries.Count()))*100))
		}

		fmt.Printf("\n  Fields (est):\n")
		for _, t := range sortKeys(fieldCardinalities) {
			fmt.Printf("    - %v: %d\n", t, fieldCardinalities[t].Count())
		}

		fmt.Printf("\n  Tags (est):\n")
		for _, t := range sortKeys(tagCardinalities) {
			fmt.Printf("    - %v: %d\n", t, tagCardinalities[t].Count())
		}
	}

	fmt.Printf("Completed in %s\n", time.Since(start))
	return nil
}

// sortKeys is a quick helper to return the sorted set of a map's keys
func sortKeys(vals map[string]Counter) (keys []string) {
	for k := range vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return keys
}

// printUsage prints the usage message to STDERR.
func (cmd *Command) printUsage() {
	usage := `Displays shard level report.

Usage: influx_inspect report [flags]

    -pattern <pattern>
            Include only files matching a pattern.
    -exact
            Report exact cardinality counts instead of estimates.  Note: this can use a lot of memory.
            Defaults to "false".
    -detailed
            Report detailed cardinality estimates.
            Defaults to "false".
`

	fmt.Fprintf(cmd.Stdout, usage)
}

// Counter abstracts a a method of counting keys.
type Counter interface {
	Add(key []byte)
	Count() uint64
}

// NewHLLCounter returns an approximate Counter using HyperLogLogs for cardinality estimation.
func NewHLLCounter() Counter {
	return hllpp.New()
}

// exactCounter returns an exact count for keys using counting all distinct items in a set.
type exactCounter struct {
	m map[string]struct{}
}

func (c *exactCounter) Add(key []byte) {
	c.m[string(key)] = struct{}{}
}

func (c *exactCounter) Count() uint64 {
	return uint64(len(c.m))
}

func NewExactCounter() Counter {
	return &exactCounter{
		m: make(map[string]struct{}),
	}
}
