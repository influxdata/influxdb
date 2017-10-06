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

	err := cmd.isShardDir(cmd.dir)
	if cmd.detailed && err != nil {
		return fmt.Errorf("-detailed only supported for shard dirs.")
	}

	totalSeries := hllpp.New()
	tagCardinalities := map[string]*hllpp.HLLPP{}
	measCardinalities := map[string]*hllpp.HLLPP{}
	fieldCardinalities := map[string]*hllpp.HLLPP{}

	dbCardinalities := map[string]*hllpp.HLLPP{}

	start := time.Now()

	tw := tabwriter.NewWriter(cmd.Stdout, 8, 2, 1, ' ', 0)
	fmt.Fprintln(tw, strings.Join([]string{"DB", "RP", "Shard", "File", "Series", "New (Est)", "Load Time"}, "\t"))

	if err := cmd.WalkShardDirs(cmd.dir, func(db, rp, id, path string) error {
		if cmd.pattern != "" && strings.Contains(path, cmd.pattern) {
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

		dbCount := dbCardinalities[db]
		if dbCount == nil {
			dbCount = hllpp.New()
			dbCardinalities[db] = dbCount
		}

		oldCount := dbCount.Count()

		seriesCount := reader.KeyCount()
		for i := 0; i < seriesCount; i++ {
			key, _ := reader.KeyAt(i)
			totalSeries.Add([]byte(key))
			dbCount.Add([]byte(key))

			if cmd.detailed {
				sep := strings.Index(string(key), "#!~#")
				seriesKey, field := key[:sep], key[sep+4:]
				measurement, tags := models.ParseKey(seriesKey)

				measCount := measCardinalities[measurement]
				if measCount == nil {
					measCount = hllpp.New()
					measCardinalities[measurement] = measCount
				}
				measCount.Add([]byte(key))

				fieldCount := fieldCardinalities[measurement]
				if fieldCount == nil {
					fieldCount = hllpp.New()
					fieldCardinalities[measurement] = fieldCount
				}
				fieldCount.Add([]byte(field))

				for _, t := range tags {
					tagCount := tagCardinalities[string(t.Key)]
					if tagCount == nil {
						tagCount = hllpp.New()
						tagCardinalities[string(t.Key)] = tagCount
					}
					tagCount.Add(t.Value)
				}
			}
		}
		reader.Close()

		fmt.Fprintln(tw, strings.Join([]string{
			db, rp, id,
			filepath.Base(file.Name()),
			strconv.FormatInt(int64(seriesCount), 10),
			strconv.FormatInt(int64(dbCount.Count()-oldCount), 10),

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
	fmt.Printf("Statistics\n")
	fmt.Printf("  Series:\n")
	for db, counts := range dbCardinalities {
		fmt.Printf("     - %s (est): %d (%d%%)\n", db, counts.Count(), int(float64(counts.Count())/float64(totalSeries.Count())*100))
	}
	fmt.Printf("  Total (est): %d\n", totalSeries.Count())

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
func sortKeys(vals map[string]*hllpp.HLLPP) (keys []string) {
	for k := range vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return keys
}

func (cmd *Command) isShardDir(dir string) error {
	name := filepath.Base(dir)
	if id, err := strconv.Atoi(name); err != nil || id < 1 {
		return fmt.Errorf("not a valid shard dir: %v", dir)
	}

	return nil
}

func (cmd *Command) WalkShardDirs(root string, fn func(db, rp, id, path string) error) error {
	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if filepath.Ext(info.Name()) == "."+tsm1.TSMFileExtension {
			shardDir := filepath.Dir(path)

			if err := cmd.isShardDir(shardDir); err != nil {
				return err
			}
			absPath, err := filepath.Abs(path)
			if err != nil {
				return err
			}
			parts := strings.Split(absPath, string(filepath.Separator))
			db, rp, id := parts[len(parts)-4], parts[len(parts)-3], parts[len(parts)-2]

			return fn(db, rp, id, path)
		}
		return nil
	})
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
