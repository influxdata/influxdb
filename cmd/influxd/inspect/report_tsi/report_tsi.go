// Package report_tsi provides a report about the series cardinality in one or more TSI indexes.
package report_tsi

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync/atomic"
	"text/tabwriter"

	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/index/tsi1"
	"github.com/spf13/cobra"
)

const (
	// Number of series IDs to stored in slice before we convert to a roaring
	// bitmap. Roaring bitmaps have a non-trivial initial cost to construct.
	useBitmapN = 25
)

// reportTSI represents the program execution for "inspect report-tsi".
type reportTSI struct {
	// Flags
	bucketId    string // required
	dataPath    string
	topN        int
	concurrency int

	// Variables for calculating and storing cardinalities
	sfile         *tsdb.SeriesFile
	shardPaths    map[uint64]string
	shardIdxs     map[uint64]*tsi1.Index
	cardinalities map[uint64]map[string]*cardinality
}

// NewReportTSICommand returns a new instance of Command with default setting applied.
func NewReportTSICommand() *cobra.Command {
	var arguments reportTSI
	cmd := &cobra.Command{
		Use:   "report-tsi",
		Short: "Reports the cardinality of TSI files",
		Long: `This command will analyze TSI files within a specified bucket, reporting the 
cardinality of data within the files, segmented by shard and further by measurement.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {

			arguments.shardPaths = map[uint64]string{}
			arguments.shardIdxs = map[uint64]*tsi1.Index{}
			arguments.cardinalities = map[uint64]map[string]*cardinality{}

			return arguments.run(cmd)
		},
	}

	cmd.Flags().StringVarP(&arguments.bucketId, "bucket-id", "b", "", "Required - specify which bucket to report on. A bucket id must be a base-16 string")
	cmd.Flags().StringVar(&arguments.dataPath, "data-path", os.Getenv("HOME")+"/.influxdbv2/engine/data", "Path to data directory")
	cmd.Flags().IntVarP(&arguments.topN, "top", "t", 0, "Limit results to top n")
	cmd.Flags().IntVar(&arguments.concurrency, "concurrency", runtime.GOMAXPROCS(0), "How many concurrent workers to run")
	cmd.MarkFlagRequired("bucket-id")

	return cmd
}

// Run executes the command.
func (report *reportTSI) run(cmd *cobra.Command) error {
	// Get all shards from specified bucket
	dirEntries, err := os.ReadDir(filepath.Join(report.dataPath, report.bucketId, "autogen"))

	if err != nil {
		return err
	}

	for _, entry := range dirEntries {

		if !entry.IsDir() {
			continue
		}

		if entry.Name() == tsdb.SeriesFileDirectory || entry.Name() == "index" {
			continue
		}

		id, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}

		report.shardPaths[uint64(id)] = filepath.Join(report.dataPath, report.bucketId, "autogen", entry.Name())
	}

	if len(report.shardPaths) == 0 {
		cmd.Printf("No shards under %s\n", filepath.Join(report.dataPath, report.bucketId, "autogen"))
		return nil
	}

	report.sfile = tsdb.NewSeriesFile(filepath.Join(report.dataPath, report.bucketId, tsdb.SeriesFileDirectory))

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

	// Blocks until all work done.
	if err = report.calculateCardinalities(report.cardinalityByMeasurement); err != nil {
		return err
	}

	allIDs := make([]uint64, 0, len(report.shardIdxs))

	for id := range report.shardIdxs {
		allIDs = append(allIDs, id)
	}

	// Print summary.
	if err = report.printSummaryByMeasurement(cmd); err != nil {
		return err
	}

	sort.Slice(allIDs, func(i int, j int) bool { return allIDs[i] < allIDs[j] })

	for _, id := range allIDs {
		if err := report.printShardByMeasurement(cmd, id); err != nil {
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
	shardIDs := make([]uint64, 0, len(report.shardPaths))
	for id := range report.shardPaths {
		pth := filepath.Join(report.shardPaths[id], "index")

		// Verify directory is an index before opening it.
		if ok, err := tsi1.IsIndexDir(pth); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("not a TSI index directory: %s", pth)
		}

		report.shardIdxs[id] = tsi1.NewIndex(report.sfile,
			"",
			tsi1.WithPath(pth),
			tsi1.DisableCompactions(),
		)

		// Initialise cardinality set to store cardinalities for each shard
		report.cardinalities[id] = map[string]*cardinality{}

		shardIDs = append(shardIDs, id)
	}

	errC := make(chan error, len(shardIDs))
	var maxi uint32 // index of maximum shard being worked on.
	for k := 0; k < report.concurrency; k++ {
		go func() {
			for {
				i := int(atomic.AddUint32(&maxi, 1) - 1) // Get next shard to work on.
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
		for _, s := range c.short {
			c.set.AddNoLock(uint64(s))
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
	if err := report.shardIdxs[shardID].Open(); err != nil {
		return err
	}

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
				return fmt.Errorf("series ID is too large: %d (max %d)", e.SeriesID, uint32(math.MaxUint32))
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

func (report *reportTSI) printSummaryByMeasurement(cmd *cobra.Command) error {
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

			if other.short != nil { // low cardinality case
				res.addShort(other.short)
			} else if other.set != nil { // High cardinality case
				res.merge(other.set)
			}
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

	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 8, 8, 1, '\t', tabwriter.AlignRight)

	fmt.Fprintf(tw, "Summary\nDatabase Path: %s\nCardinality (exact): %d\n\n", filepath.Join(report.dataPath, report.bucketId), totalCardinality)
	fmt.Fprint(tw, "Measurement\tCardinality (exact)\n\n")
	for _, res := range measurements {
		fmt.Fprintf(tw, "%q\t%d\t\n", res.name, res.count)
	}

	if err := tw.Flush(); err != nil {
		return err
	}
	fmt.Fprint(tw, "\n\n")

	return nil
}

func (report *reportTSI) printShardByMeasurement(cmd *cobra.Command, id uint64) error {
	defer report.shardIdxs[id].Close()

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

	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 8, 8, 1, '\t', 0)
	fmt.Fprintf(tw, "===============\nShard ID: %d\nPath: %s\nCardinality (exact): %d\n\n", id, report.shardPaths[id], totalCardinality)
	fmt.Fprint(tw, "Measurement\tCardinality (exact)\n\n")
	for _, card := range all {
		fmt.Fprintf(tw, "%q\t%d\t\n", card.name, card.cardinality())
	}
	fmt.Fprint(tw, "===============\n\n")
	if err := tw.Flush(); err != nil {
		return err
	}

	return nil
}
