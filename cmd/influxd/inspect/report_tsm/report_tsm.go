package report_tsm

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/retailnext/hllpp"
	"github.com/spf13/cobra"
)

type args struct {
	dir      string
	pattern  string
	detailed bool
	exact    bool
}

func NewReportTSMCommand() *cobra.Command {
	var arguments args
	cmd := &cobra.Command{
		Use:   "report-tsm",
		Short: "Run TSM report",
		Long: `
This command will analyze TSM files within a storage engine directory, reporting 
the cardinality within the files as well as the time range that the point data 
covers.
This command only interrogates the index within each file, and does not read any
block data. To reduce heap requirements, by default report-tsm estimates the 
overall cardinality in the file set by using the HLL++ algorithm. Exact 
cardinalities can be determined by using the --exact flag.
For each file, the following is output:
	* The full filename;
	* The series cardinality within the file;
	* The number of series first encountered within the file;
	* The min and max timestamp associated with TSM data in the file; and
	* The time taken to load the TSM index and apply any tombstones.
The summary section then outputs the total time range and series cardinality for 
the fileset. Depending on the --detailed flag, series cardinality is segmented 
in the following ways:
	* Series cardinality for each organization;
	* Series cardinality for each bucket;
	* Series cardinality for each measurement;
	* Number of field keys for each measurement; and
	* Number of tag values for each tag key.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {

			// Verify if shard dir
			err := arguments.isShardDir(arguments.dir)
			if arguments.detailed && err != nil {
				return errors.New("--detailed only supported for shard dirs")
			}

			return arguments.Run(cmd)
		},
	}

	cmd.Flags().StringVarP(&arguments.pattern, "pattern", "", "", "only process TSM files containing pattern")
	cmd.Flags().BoolVarP(&arguments.exact, "exact", "", false, "calculate and exact cardinality count. Warning, may use significant memory...")
	cmd.Flags().BoolVarP(&arguments.detailed, "detailed", "", false, "emit series cardinality segmented by measurements, tag keys and fields. Warning, may take a while.")

	dir, err := fs.InfluxDir()
	if err != nil {
		panic(err)
	}
	dir = filepath.Join(dir, "engine/data")
	cmd.Flags().StringVarP(&arguments.dir, "data-path", "", dir, "use provided data directory")

	return cmd
}

func (a *args) isShardDir(dir string) error {
	name := filepath.Base(dir)
	if id, err := strconv.Atoi(name); err != nil || id < 1 {
		return fmt.Errorf("not a valid shard dir: %s", dir)
	}

	return nil
}

func (a *args) Run(cmd *cobra.Command) error {
	// Create the cardinality counter
	newCounterFn := newHLLCounter
	estTitle := " (est)"
	if a.exact {
		estTitle = ""
		newCounterFn = newExactCounter
	}

	totalSeries := newCounterFn()
	tagCardinalities := map[string]counter{}
	measCardinalities := map[string]counter{}
	fieldCardinalities := map[string]counter{}

	dbCardinalities := map[string]counter{}

	start := time.Now()

	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 8, 2, 1, ' ', 0)
	_, _ = fmt.Fprintln(tw, strings.Join([]string{"DB", "RP", "Shard", "File", "Series", "New" + estTitle, "Min Time", "Max Time", "Load Time"}, "\t"))

	minTime, maxTime := int64(math.MaxInt64), int64(math.MinInt64)
	var fileCount int
	if err := a.walkShardDirs(a.dir, func(db, rp, id, path string) error {
		if a.pattern != "" && !strings.Contains(path, a.pattern) {
			return nil
		}

		file, err := os.OpenFile(path, os.O_RDONLY, 0600)
		if err != nil {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "error opening %q, skipping: %v\n", path, err)
			return nil
		}

		loadStart := time.Now()
		reader, err := tsm1.NewTSMReader(file)
		if err != nil {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "error reading %q, skipping: %v\n", file.Name(), err)
			return nil
		}
		loadTime := time.Since(loadStart)
		fileCount++

		dbCount, ok := dbCardinalities[db]
		if !ok {
			dbCount = newCounterFn()
			dbCardinalities[db] = dbCount
		}

		oldCount := dbCount.Count()

		seriesCount := reader.KeyCount()
		for i := 0; i < seriesCount; i++ {
			key, _ := reader.KeyAt(i)
			totalSeries.Add(key)
			dbCount.Add(key)

			if a.detailed {
				sep := strings.Index(string(key), "#!~#")
				seriesKey, field := key[:sep], key[sep+4:]
				measurement, tags := models.ParseKey(seriesKey)

				measCount, ok := measCardinalities[measurement]
				if !ok {
					measCount = newCounterFn()
					measCardinalities[measurement] = measCount
				}
				measCount.Add(key)

				fieldCount, ok := fieldCardinalities[measurement]
				if !ok {
					fieldCount = newCounterFn()
					fieldCardinalities[measurement] = fieldCount
				}
				fieldCount.Add(field)

				for _, t := range tags {
					tagCount, ok := tagCardinalities[string(t.Key)]
					if !ok {
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
		err = reader.Close()
		if err != nil {
			return fmt.Errorf("failed to close TSM Reader: %v", err)
		}

		_, _ = fmt.Fprintln(tw, strings.Join([]string{
			db, rp, id,
			filepath.Base(file.Name()),
			strconv.FormatInt(int64(seriesCount), 10),
			strconv.FormatInt(int64(dbCount.Count()-oldCount), 10),
			time.Unix(0, minT).UTC().Format(time.RFC3339Nano),
			time.Unix(0, maxT).UTC().Format(time.RFC3339Nano),
			loadTime.String(),
		}, "\t"))
		if a.detailed {
			err = tw.Flush()
			if err != nil {
				return fmt.Errorf("failed to flush tabwriter: %v", err)
			}
		}
		return nil
	}); err != nil {
		return err
	}

	err := tw.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush tabwriter: %v", err)
	}

	printSummary(cmd, printArgs{
		fileCount:          fileCount,
		minTime:            minTime,
		maxTime:            maxTime,
		estTitle:           estTitle,
		totalSeries:        totalSeries,
		detailed:           a.detailed,
		tagCardinalities:   tagCardinalities,
		measCardinalities:  measCardinalities,
		fieldCardinalities: fieldCardinalities,
		dbCardinalities:    dbCardinalities,
	})

	cmd.Printf("Completed in %s\n", time.Since(start))
	return nil
}

type printArgs struct {
	fileCount        int
	minTime, maxTime int64
	estTitle         string
	totalSeries      counter
	detailed         bool

	tagCardinalities   map[string]counter
	measCardinalities  map[string]counter
	fieldCardinalities map[string]counter
	dbCardinalities    map[string]counter
}

func printSummary(cmd *cobra.Command, p printArgs) {
	cmd.Printf("\nSummary:")
	cmd.Printf("  Files: %d\n", p.fileCount)
	cmd.Printf("  Time Range: %s - %s\n",
		time.Unix(0, p.minTime).UTC().Format(time.RFC3339Nano),
		time.Unix(0, p.maxTime).UTC().Format(time.RFC3339Nano),
	)
	cmd.Printf("  Duration: %s \n\n", time.Unix(0, p.maxTime).Sub(time.Unix(0, p.minTime)))

	cmd.Printf("Statistics\n")
	cmd.Printf("  Series:\n")
	for db, counts := range p.dbCardinalities {
		cmd.Printf("     - %s%s: %d (%d%%)\n", db, p.estTitle, counts.Count(), int(float64(counts.Count())/float64(p.totalSeries.Count())*100))
	}
	cmd.Printf("  Total%s: %d\n", p.estTitle, p.totalSeries.Count())

	if p.detailed {
		cmd.Printf("\n  Measurements (est):\n")
		for _, t := range sortKeys(p.measCardinalities) {
			cmd.Printf("    - %v: %d (%d%%)\n", t, p.measCardinalities[t].Count(), int((float64(p.measCardinalities[t].Count())/float64(p.totalSeries.Count()))*100))
		}

		cmd.Printf("\n  Fields (est):\n")
		for _, t := range sortKeys(p.fieldCardinalities) {
			cmd.Printf("    - %v: %d\n", t, p.fieldCardinalities[t].Count())
		}

		cmd.Printf("\n  Tags (est):\n")
		for _, t := range sortKeys(p.tagCardinalities) {
			cmd.Printf("    - %v: %d\n", t, p.tagCardinalities[t].Count())
		}
	}
}

// sortKeys is a quick helper to return the sorted set of a map's keys
func sortKeys(vals map[string]counter) (keys []string) {
	for k := range vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return keys
}

func (a *args) walkShardDirs(root string, fn func(db, rp, id, path string) error) error {
	type location struct {
		db, rp, id, path string
	}

	var tsms []location
	if err := filepath.WalkDir(root, func(path string, info os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if filepath.Ext(info.Name()) == "."+tsm1.TSMFileExtension {
			shardDir := filepath.Dir(path)

			if err := a.isShardDir(shardDir); err != nil {
				return err
			}
			absPath, err := filepath.Abs(path)
			if err != nil {
				return err
			}
			parts := strings.Split(absPath, string(filepath.Separator))
			db, rp, id := parts[len(parts)-4], parts[len(parts)-3], parts[len(parts)-2]
			tsms = append(tsms, location{db: db, rp: rp, id: id, path: path})
			return nil
		}
		return nil
	}); err != nil {
		return err
	}

	sort.Slice(tsms, func(i, j int) bool {
		a, _ := strconv.Atoi(tsms[i].id)
		b, _ := strconv.Atoi(tsms[j].id)
		return a < b
	})

	for _, shard := range tsms {
		if err := fn(shard.db, shard.rp, shard.id, shard.path); err != nil {
			return err
		}
	}
	return nil
}

// counter abstracts a a method of counting keys.
type counter interface {
	Add(key []byte)
	Count() uint64
}

// newHLLCounter returns an approximate counter using HyperLogLogs for cardinality estimation.
func newHLLCounter() counter {
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

func newExactCounter() counter {
	return &exactCounter{
		m: make(map[string]struct{}),
	}
}
