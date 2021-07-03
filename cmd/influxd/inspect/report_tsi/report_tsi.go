// Package report_tsi provides a report about the series cardinality in one or more TSI indexes.
package report_tsi

import (
	"errors"
	"fmt"
	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/index/tsi1"
	"github.com/spf13/cobra"
	"math"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync/atomic"
	"text/tabwriter"
)

const (
	// Number of series IDs to stored in slice before we convert to a roaring
	// bitmap. Roaring bitmaps have a non-trivial initial cost to construct.
	useBitmapN = 25
)

// reportTSI represents the program execution for "inspect report-tsi".
type reportTSI struct {
	// Flags
	enginePath     string // required
	bucketId       string // required
	seriesFilePath string
	topN           int
	concurrency    int

	// Variables for calculating and storing cardinalities
	sfile          *tsdb.SeriesFile
	shardPaths     map[uint64]string
	shardIdxs      map[uint64]*tsi1.Index
	cardinalities  map[uint64]map[string]*cardinality
}

// NewReportTSICommand returns a new instance of Command with default setting applied.
func NewReportTSICommand() *cobra.Command {
	var arguments reportTSI
	cmd := &cobra.Command{
		Use:   "report-tsi",
		Short: "Reports the cardinality of TSI files",
		Long: `This command will analyze TSI files within a storage engine directory, reporting 
		the cardinality of data within the files, divided into org and bucket cardinalities.
		
		For each report, the following is output:
		
			* All orgs and buckets in the index;
			* The series cardinality within each org and each bucket;
			* The time taken to read the index.
		
		Depending on the --measurements flag, series cardinality is segmented 
		in the following ways:
		
			* Series cardinality for each organization;
			* Series cardinality for each bucket;
			* Series cardinality for each measurement;`,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			//fmt.Printf("%+v\n", arguments)
			return arguments.Run()
		},
	}

	cmd.Flags().StringVarP(&arguments.bucketId, "bucket_id", "b", "", "If bucket is specified, org must be specified. A bucket id must be a base-16 string")
	cmd.Flags().StringVar(&arguments.enginePath, "path", os.Getenv("HOME")+"/.influxdbv2/engine/data", "Path to engine. Defaults $HOME/.influxdbv2/engine")
	cmd.Flags().StringVar(&arguments.seriesFilePath, "series-file", "", "Optional path to series file. Defaults $HOME/.influxdbv2/engine/<bucket_id>/_series")
	cmd.Flags().IntVarP(&arguments.topN, "top", "t", 0, "Limit results to top n")
	cmd.Flags().IntVar(&arguments.concurrency, "c", runtime.GOMAXPROCS(0), "How many concurrent workers to run. Default is 8.")

	arguments.shardPaths = map[uint64]string{}
	arguments.shardIdxs = map[uint64]*tsi1.Index{}
	arguments.cardinalities = map[uint64]map[string]*cardinality{}

	return cmd
}

// Run executes the command.
func (report *reportTSI) Run() error {
	if report.bucketId == "" {
		return errors.New("bucket_id is required, use -b or --bucket_id flag")
	}

	if report.seriesFilePath == "" {
		report.seriesFilePath = path.Join(report.enginePath, report.bucketId, tsdb.SeriesFileDirectory)
	}

	// Walk database directory to get shards.
	if err := filepath.Walk(report.enginePath, func(path string, info os.FileInfo, err error) error {

		if err != nil {
			return err
		}

		if !info.IsDir() {
			return nil
		}

		// TODO(edd): this would be a problem if the retention policy was named
		// "index".
		if info.Name() == tsdb.SeriesFileDirectory || info.Name() == "index" {
			return filepath.SkipDir
		}

		id, err := strconv.Atoi(info.Name())
		if err != nil {
			return nil
		}

		report.shardPaths[uint64(id)] = path

		return nil
	}); err != nil {
		return err
	}

	if len(report.shardPaths) == 0 {
		fmt.Fprintf(os.Stderr, "No shards under %s\n", report.enginePath)
		return nil
	}

	return report.run()
}

func (report *reportTSI) run() error {
	report.sfile = tsdb.NewSeriesFile(report.seriesFilePath)

	config := logger.NewConfig()
	newLogger, err := config.New(os.Stderr)
	if err != nil {
		return err
	}
	report.sfile.Logger = newLogger

	if err := report.sfile.Open(); err != nil {
		return err
	}
	defer report.sfile.Close()

	// Open all the indexes.
	for id, pth := range report.shardPaths {
		pth = path.Join(pth, "index")
		// Verify directory is an index before opening it.
		if ok, err := tsi1.IsIndexDir(pth); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("not a TSI index directory: %q", pth)
		}

		report.shardIdxs[id] = tsi1.NewIndex(report.sfile,
			"",
			tsi1.WithPath(pth),
			tsi1.DisableCompactions(),
		)
		if err := report.shardIdxs[id].Open(); err != nil {
			return err
		}
		defer report.shardIdxs[id].Close()

		// Initialise cardinality set to store cardinalities for this shard.
		report.cardinalities[id] = map[string]*cardinality{}
	}

	// Calculate cardinalities of shards.
	fn := report.cardinalityByMeasurement

	// Blocks until all work done.
	report.calculateCardinalities(fn)

	// Print summary.
	if err := report.printSummaryByMeasurement(); err != nil {
		return err
	}

	allIDs := make([]uint64, 0, len(report.shardIdxs))
	for id := range report.shardIdxs {
		allIDs = append(allIDs, id)
	}
	sort.Slice(allIDs, func(i int, j int) bool { return allIDs[i] < allIDs[j] })

	for _, id := range allIDs {
		if err := report.printShardByMeasurement(id); err != nil {
			return err
		}
	}
	return nil
}

// calculateCardinalities calculates the cardinalities of the set of shard being
// worked on concurrently. The provided function determines how cardinality is
// calculated and broken down.
func (report *reportTSI) calculateCardinalities(fn func(id uint64) error) error {
	// Get list of shards to work on.
	shardIDs := make([]uint64, 0, len(report.shardIdxs))
	for id := range report.shardIdxs {
		shardIDs = append(shardIDs, id)
	}

	errC := make(chan error, len(shardIDs))
	var maxi uint32 // index of maximumm shard being worked on.
	for k := 0; k < report.concurrency; k++ {
		go func() {
			for {
				i := int(atomic.AddUint32(&maxi, 1) - 1) // Get next partition to work on.
				if i >= len(shardIDs) {
					return // No more work.
				}
				errC <- fn(shardIDs[i])
			}
		}()
	}

	// Check for error
	for i := 0; i < cap(errC); i++ {
		if err := <-errC; err != nil {
			return err
		}
	}
	return nil
}

// Cardinality struct and methods
type cardinality struct {
	name  []byte
	short []uint32
	set   *tsdb.SeriesIDSet
}

func (c *cardinality) add(x uint64) {
	if c.set != nil {
		c.set.AddNoLock(x)
		return
	}

	c.short = append(c.short, uint32(x)) // Series IDs never get beyond 2^32

	// Cheaper to store in bitmap.
	if len(c.short) > useBitmapN {
		c.set = tsdb.NewSeriesIDSet()
		for i := 0; i < len(c.short); i++ {
			c.set.AddNoLock(uint64(c.short[i]))
		}
		c.short = nil
		return
	}
}

func (c *cardinality) cardinality() int64 {
	if c == nil || (c.short == nil && c.set == nil) {
		return 0
	}

	if c.short != nil {
		return int64(len(c.short))
	}
	return int64(c.set.Cardinality())
}

type cardinalities []*cardinality

func (a cardinalities) Len() int           { return len(a) }
func (a cardinalities) Less(i, j int) bool { return a[i].cardinality() < a[j].cardinality() }
func (a cardinalities) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func (report *reportTSI) cardinalityByMeasurement(shardID uint64) error {
	idx := report.shardIdxs[shardID]
	itr, err := idx.MeasurementIterator()
	if err != nil {
		return err
	} else if itr == nil {
		return nil
	}
	defer itr.Close()

OUTER:
	for {
		name, err := itr.Next()
		if err != nil {
			return err
		} else if name == nil {
			break OUTER
		}

		// Get series ID set to track cardinality under measurement.
		c, ok := report.cardinalities[shardID][string(name)]
		if !ok {
			c = &cardinality{name: name}
			report.cardinalities[shardID][string(name)] = c
		}

		sitr, err := idx.MeasurementSeriesIDIterator(name)
		if err != nil {
			return err
		} else if sitr == nil {
			continue
		}

		var e tsdb.SeriesIDElem
		for e, err = sitr.Next(); err == nil && e.SeriesID != 0; e, err = sitr.Next() {
			if e.SeriesID > math.MaxUint32 {
				panic(fmt.Sprintf("series ID is too large: %d (max %d). Corrupted series file?", e.SeriesID, uint32(math.MaxUint32)))
			}
			c.add(e.SeriesID)
		}
		sitr.Close()

		if err != nil {
			return err
		}
	}
	return nil
}

type result struct {
	name  []byte
	count int64

	// For low cardinality measurements just track series using map
	lowCardinality map[uint32]struct{}

	// For higher cardinality measurements track using bitmap.
	set *tsdb.SeriesIDSet
}

func (r *result) addShort(ids []uint32) {
	// There is already a bitset of this result.
	if r.set != nil {
		for _, id := range ids {
			r.set.AddNoLock(uint64(id))
		}
		return
	}

	// Still tracking low cardinality sets
	if r.lowCardinality == nil {
		r.lowCardinality = map[uint32]struct{}{}
	}

	for _, id := range ids {
		r.lowCardinality[id] = struct{}{}
	}

	// Cardinality is large enough that we will benefit from using a bitmap
	if len(r.lowCardinality) > useBitmapN {
		r.set = tsdb.NewSeriesIDSet()
		for id := range r.lowCardinality {
			r.set.AddNoLock(uint64(id))
		}
		r.lowCardinality = nil
	}
}

func (r *result) merge(other *tsdb.SeriesIDSet) {
	if r.set == nil {
		r.set = tsdb.NewSeriesIDSet()
		for id := range r.lowCardinality {
			r.set.AddNoLock(uint64(id))
		}
		r.lowCardinality = nil
	}
	r.set.Merge(other)
}

type results []*result

func (a results) Len() int           { return len(a) }
func (a results) Less(i, j int) bool { return a[i].count < a[j].count }
func (a results) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func (report *reportTSI) printSummaryByMeasurement() error {
	// Get global set of measurement names across shards.
	idxs := &tsdb.IndexSet{SeriesFile: report.sfile}
	for _, idx := range report.shardIdxs {
		idxs.Indexes = append(idxs.Indexes, idx)
	}

	mitr, err := idxs.MeasurementIterator()
	if err != nil {
		return err
	} else if mitr == nil {
		return errors.New("got nil measurement iterator for index set")
	}
	defer mitr.Close()

	var name []byte
	var totalCardinality int64
	measurements := results{}
	for name, err = mitr.Next(); err == nil && name != nil; name, err = mitr.Next() {
		res := &result{name: name}
		for _, shardCards := range report.cardinalities {
			other, ok := shardCards[string(name)]
			if !ok {
				continue // this shard doesn't have anything for this measurement.
			}

			if other.short != nil && other.set != nil {
				panic("cardinality stored incorrectly")
			}

			if other.short != nil { // low cardinality case
				res.addShort(other.short)
			} else if other.set != nil { // High cardinality case
				res.merge(other.set)
			}

			// Shard does not have any series for this measurement.
		}

		// Determine final cardinality and allow intermediate structures to be
		// GCd.
		if res.lowCardinality != nil {
			res.count = int64(len(res.lowCardinality))
		} else {
			res.count = int64(res.set.Cardinality())
		}
		totalCardinality += res.count
		res.set = nil
		res.lowCardinality = nil
		measurements = append(measurements, res)
	}

	if err != nil {
		return err
	}

	// sort measurements by cardinality.
	sort.Sort(sort.Reverse(measurements))

	if report.topN > 0 {
		// There may not be "topN" measurement cardinality to sub-slice.
		n := int(math.Min(float64(report.topN), float64(len(measurements))))
		measurements = measurements[:n]
	}

	tw := tabwriter.NewWriter(os.Stdout, 4, 4, 1, '\t', 0)

	fmt.Fprintf(tw, "Summary\nDatabase Path: %s\nCardinality (exact): %d\n\n", report.enginePath, totalCardinality)
	fmt.Fprint(tw, "Measurement\tCardinality (exact)\n\n")
	for _, res := range measurements {
		fmt.Fprintf(tw, "%q\t\t%d\n", res.name, res.count)
	}

	if err := tw.Flush(); err != nil {
		return err
	}
	fmt.Fprint(os.Stdout, "\n\n")
	return nil
}

func (report *reportTSI) printShardByMeasurement(id uint64) error {
	allMap, ok := report.cardinalities[id]
	if !ok {
		return nil
	}

	var totalCardinality int64
	all := make(cardinalities, 0, len(allMap))
	for _, card := range allMap {
		n := card.cardinality()
		if n == 0 {
			continue
		}

		totalCardinality += n
		all = append(all, card)
	}

	sort.Sort(sort.Reverse(all))

	// Trim to top-n
	if report.topN > 0 {
		// There may not be "topN" measurement cardinality to sub-slice.
		n := int(math.Min(float64(report.topN), float64(len(all))))
		all = all[:n]
	}

	tw := tabwriter.NewWriter(os.Stdout, 4, 4, 1, '\t', 0)
	fmt.Fprintf(tw, "===============\nShard ID: %d\nPath: %s\nCardinality (exact): %d\n\n", id, report.shardPaths[id], totalCardinality)
	fmt.Fprint(tw, "Measurement\tCardinality (exact)\n\n")
	for _, card := range all {
		fmt.Fprintf(tw, "%q\t\t%d\n", card.name, card.cardinality())
	}
	fmt.Fprint(tw, "===============\n\n")
	if err := tw.Flush(); err != nil {
		return err
	}
	fmt.Fprint(os.Stdout, "\n\n")
	return nil
}
