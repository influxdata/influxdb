// Package report reports statistics about TSM files.
package report

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/retailnext/hllpp"
)

// Command represents the program execution for "influxd report".
type Command struct {
	Stderr io.Writer
	Stdout io.Writer

	dir      string
	pattern  string
	detailed bool
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

	fs.SetOutput(cmd.Stdout)
	fs.Usage = cmd.printUsage

	if err := fs.Parse(args); err != nil {
		return err
	}
	cmd.dir = fs.Arg(0)

	start := time.Now()

	files, err := filepath.Glob(filepath.Join(cmd.dir, fmt.Sprintf("*.%s", tsm1.TSMFileExtension)))
	if err != nil {
		return err
	}

	var filtered []string
	if cmd.pattern != "" {
		for _, f := range files {
			if strings.Contains(f, cmd.pattern) {
				filtered = append(filtered, f)
			}
		}
		files = filtered
	}

	if len(files) == 0 {
		return fmt.Errorf("no tsm files at %v", cmd.dir)
	}

	tw := tabwriter.NewWriter(cmd.Stdout, 8, 8, 1, '\t', 0)
	fmt.Fprintln(tw, strings.Join([]string{"File", "Series", "Load Time"}, "\t"))

	totalSeries := hllpp.New()
	tagCardinalities := map[string]*hllpp.HLLPP{}
	measCardinalities := map[string]*hllpp.HLLPP{}
	fieldCardinalities := map[string]*hllpp.HLLPP{}

	for _, f := range files {
		file, err := os.OpenFile(f, os.O_RDONLY, 0600)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "error: %s: %v. Skipping.\n", f, err)
			continue
		}

		loadStart := time.Now()
		reader, err := tsm1.NewTSMReader(file)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "error: %s: %v. Skipping.\n", file.Name(), err)
			continue
		}
		loadTime := time.Since(loadStart)

		seriesCount := reader.KeyCount()
		for i := 0; i < seriesCount; i++ {
			key, _ := reader.KeyAt(i)
			totalSeries.Add([]byte(key))

			if cmd.detailed {
				sep := strings.Index(string(key), "#!~#")
				seriesKey, field := key[:sep], key[sep+4:]
				measurement, tags := models.ParseKey(seriesKey)

				measCount, ok := measCardinalities[measurement]
				if !ok {
					measCount = hllpp.New()
					measCardinalities[measurement] = measCount
				}
				measCount.Add([]byte(key))

				fieldCount, ok := fieldCardinalities[measurement]
				if !ok {
					fieldCount = hllpp.New()
					fieldCardinalities[measurement] = fieldCount
				}
				fieldCount.Add([]byte(field))

				for _, t := range tags {
					tagCount, ok := tagCardinalities[string(t.Key)]
					if !ok {
						tagCount = hllpp.New()
						tagCardinalities[string(t.Key)] = tagCount
					}
					tagCount.Add(t.Value)
				}
			}
		}
		reader.Close()

		fmt.Fprintln(tw, strings.Join([]string{
			filepath.Base(file.Name()),
			strconv.FormatInt(int64(seriesCount), 10),
			loadTime.String(),
		}, "\t"))
		tw.Flush()
	}

	tw.Flush()
	println()
	fmt.Printf("Statistics\n")
	fmt.Printf("\tSeries:\n")
	fmt.Printf("\t\tTotal (est): %d\n", totalSeries.Count())

	if cmd.detailed {
		fmt.Printf("\tMeasurements (est):\n")
		for _, t := range sortKeys(measCardinalities) {
			fmt.Printf("\t\t%v: %d (%d%%)\n", t, measCardinalities[t].Count(), int((float64(measCardinalities[t].Count())/float64(totalSeries.Count()))*100))
		}

		fmt.Printf("\tFields (est):\n")
		for _, t := range sortKeys(fieldCardinalities) {
			fmt.Printf("\t\t%v: %d\n", t, fieldCardinalities[t].Count())
		}

		fmt.Printf("\tTags (est):\n")
		for _, t := range sortKeys(tagCardinalities) {
			fmt.Printf("\t\t%v: %d\n", t, tagCardinalities[t].Count())
		}
	}

	fmt.Printf("Completed in %s\n", time.Since(start))
	return nil
}

// sortKeys is a quick helper to return the sorted set of a map's keys
func sortKeys(vals map[string]*hllpp.HLLPP) (keys []string) {
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
    -detailed
            Report detailed cardinality estimates.
            Defaults to "false".
`

	fmt.Fprintf(cmd.Stdout, usage)
}
