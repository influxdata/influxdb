package tsi1

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"text/tabwriter"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

const (
	// Number of series IDs to stored in slice before we convert to a roaring
	// bitmap. Roaring bitmaps have a non-trivial initial cost to construct.
	useBitmapN = 25
)

// ReportCommand represents the program execution for "influxd reporttsi".
type ReportCommand struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer
	Logger *zap.Logger

	// Filters
	DataPath        string
	OrgID, BucketID *influxdb.ID

	// Maps org and bucket IDs with measurement name
	orgBucketCardinality map[influxdb.ID]map[influxdb.ID]*cardinality

	SeriesDirPath string // optional. Defaults to dbPath/_series
	sfile         *tsdb.SeriesFile
	indexFile     *Index

	topN          int
	byMeasurement bool
	byTagKey      bool

	// How many goroutines to dedicate to calculating cardinality.
	Concurrency int
}

// NewCommand returns a new instance of Command with default setting applied.
func NewReportCommand() *ReportCommand {
	return &ReportCommand{
		Logger:               zap.NewNop(),
		orgBucketCardinality: make(map[influxdb.ID]map[influxdb.ID]*cardinality),
		topN:                 0,
		byMeasurement:        true,
		byTagKey:             false,
		Concurrency:          runtime.GOMAXPROCS(0),
	}
}

func (report *ReportCommand) Run() error {
	report.Stdout = os.Stdout

	if report.SeriesDirPath == "" {
		report.SeriesDirPath = filepath.Join(report.DataPath, "_series")
	}

	sFile := tsdb.NewSeriesFile(report.SeriesDirPath)
	sFile.WithLogger(report.Logger)
	if err := sFile.Open(context.Background()); err != nil {
		report.Logger.Error("failed to open series")
		return err
	}
	defer sFile.Close()
	report.sfile = sFile

	path := filepath.Join(report.DataPath, "index")
	report.indexFile = NewIndex(sFile, NewConfig(), WithPath(path))
	if err := report.indexFile.Open(context.Background()); err != nil {
		return err
	}
	defer report.indexFile.Close()

	// Calculate cardinalities for every org and bucket
	fn := report.cardinalityByMeasurement

	// Blocks until all work done.
	report.calculateCardinalities(fn)

	// Print summary.
	if err := report.printSummaryByMeasurement(); err != nil {
		return err
	}

	//allIDs := make([]uint64, 0, len(report.shardIdxs))
	//for id := range report.shardIdxs {
	//	allIDs = append(allIDs, id)
	//}
	//sort.Slice(allIDs, func(i int, j int) bool { return allIDs[i] < allIDs[j] })
	//
	//for _, id := range allIDs {
	//	if err := report.printShardByMeasurement(id); err != nil {
	//		return err
	//	}
	//}
	return nil
}

// calculateCardinalities calculates the cardinalities of the set of shard being
// worked on concurrently. The provided function determines how cardinality is
// calculated and broken down.
func (report *ReportCommand) calculateCardinalities(fn func() error) error {
	// Get list of shards to work on.
	//shardIDs := make([]uint64, 0, len(report.shardIdxs))
	//for id := range report.shardIdxs {
	//	shardIDs = append(shardIDs, id)
	//}
	//
	//errC := make(chan error, len(shardIDs))
	//var maxi uint32 // index of maximumm shard being worked on.
	//for k := 0; k < report.Concurrency; k++ {
	//	go func() {
	//		for {
	//			i := int(atomic.AddUint32(&maxi, 1) - 1) // Get next partition to work on.
	//			if i >= len(shardIDs) {
	//				return // No more work.
	//			}
	//			errC <- fn(shardIDs[i])
	//		}
	//	}()
	//}
	//
	//// Check for error
	//for i := 0; i < cap(errC); i++ {
	//	if err := <-errC; err != nil {
	//		return err
	//	}
	//}
	if err := fn(); err != nil {
		return err
	}
	return nil
}

type cardinality struct {
	name  []byte
	short []uint32
	set   *tsdb.SeriesIDSet
}

func (c *cardinality) add(x uint64) {
	if c.set != nil {
		c.set.AddNoLock(tsdb.NewSeriesID(x))
		return
	}

	c.short = append(c.short, uint32(x)) // Series IDs never get beyond 2^32

	// Cheaper to store in bitmap.
	if len(c.short) > useBitmapN {
		c.set = tsdb.NewSeriesIDSet()
		for i := 0; i < len(c.short); i++ {
			c.set.AddNoLock(tsdb.NewSeriesID(uint64(c.short[i])))
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

func (report *ReportCommand) cardinalityByMeasurement() error {
	idx := report.indexFile
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

		var a [16]byte // TODO(edd) if this shows up we can use a different API to DecodeName.
		copy(a[:], name[:16])
		org, bucket := tsdb.DecodeName(a)

		if report.OrgID != nil && *report.OrgID != org {
			continue
		} else if report.BucketID != nil && *report.BucketID != bucket {
			continue
		}

		sitr, err := idx.MeasurementSeriesIDIterator(name)
		if err != nil {
			return err
		} else if sitr == nil {
			continue
		}

		// initialize map of bucket to cardinality
		if _, ok := report.orgBucketCardinality[org]; !ok {
			report.orgBucketCardinality[org] = make(map[influxdb.ID]*cardinality)
		}

		var card *cardinality
		if c, ok := report.orgBucketCardinality[org][bucket]; !ok {
			card = &cardinality{name: []byte(bucket.String())}
			report.orgBucketCardinality[org][bucket] = card
		} else {
			card = c
		}

		var e tsdb.SeriesIDElem
		for e, err = sitr.Next(); err == nil && e.SeriesID.ID != 0; e, err = sitr.Next() {
			id := e.SeriesID.ID
			if id > math.MaxUint32 {
				panic(fmt.Sprintf("series ID is too large: %d (max %d). Corrupted series file?", e.SeriesID, uint32(math.MaxUint32)))
			}
			// note: first tag in array (from sfile.Series(id) is measurement

			card.add(id)
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
			r.set.AddNoLock(tsdb.NewSeriesID(uint64(id)))
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
			r.set.AddNoLock(tsdb.NewSeriesID(uint64(id)))
		}
		r.lowCardinality = nil
	}
}

func (r *result) merge(other *tsdb.SeriesIDSet) {
	if r.set == nil {
		r.set = tsdb.NewSeriesIDSet()
		for id := range r.lowCardinality {
			r.set.AddNoLock(tsdb.NewSeriesID(uint64(id)))
		}
		r.lowCardinality = nil
	}
	r.set.Merge(other)
}

type results []*result

func (a results) Len() int           { return len(a) }
func (a results) Less(i, j int) bool { return a[i].count < a[j].count }
func (a results) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func (report *ReportCommand) printSummaryByMeasurement() error {
	//Get global set of measurement names across shards.
	//idxs := &tsdb.IndexSet{SeriesFile: report.sfile}
	// for _, idx := range report.shardIdxs {
	// 	idxs.Indexes = append(idxs.Indexes, idx)
	// }

	// we are going to get a measurement iterator for each index
	//var mitr tsdb.MeasurementIterator
	//mitr, err := report.indexFile.MeasurementIterator()
	//if err != nil {
	//	report.Logger.Error("got err")
	//	return err
	//}
	//defer mitr.Close()

	// for _, index := range report.shardIdxs {
	// 	small, err := index.MeasurementIterator()
	// 	name, _ := small.Next()
	// 	report.Logger.Error("called small.Next1 " + strconv.Itoa(len(name)))
	// 	name, _ = small.Next()
	// 	report.Logger.Error("called small.Next2 " + strconv.Itoa(len(name)))
	// 	name, _ = small.Next()
	// 	report.Logger.Error("called small.Next3 " + strconv.Itoa(len(name)))
	// 	if err != nil {
	// 		return err
	// 	} else if small == nil {
	// 		return errors.New("got nil measurement iterator for index set")
	// 	}
	// 	//defer small.Close()
	// 	// name, _ := small.Next()
	// 	// report.Logger.Error("called small.Next " + string(name))
	// 	mitr = tsdb.MergeMeasurementIterators(mitr, small)
	// 	count++
	// }
	//defer mitr.Close()
	// report.Logger.Error("calling mitr next")
	// name, _ := mitr.Next()
	// report.Logger.Error("mitr next: " + string(name))

	//var name []byte
	//var totalCardinality int64
	//measurements := results{}
	//for name, err := mitr.Next(); err == nil && name != nil; name, err = mitr.Next() {
	//	res := &result{name: name}
	//	for _, shardCards := range report.cardinalities {
	//		other, ok := shardCards[string(name)]
	//		if !ok {
	//			continue // this shard doesn't have anything for this measurement.
	//		}
	//
	//		if other.short != nil && other.set != nil {
	//			panic("cardinality stored incorrectly")
	//		}
	//
	//		if other.short != nil { // low cardinality case
	//			res.addShort(other.short)
	//		} else if other.set != nil { // High cardinality case
	//			res.merge(other.set)
	//		}
	//
	//		// Shard does not have any series for this measurement.
	//	}
	//
	//	// Determine final cardinality and allow intermediate structures to be
	//	// GCd.
	//	if res.lowCardinality != nil {
	//		res.count = int64(len(res.lowCardinality))
	//	} else {
	//		res.count = int64(res.set.Cardinality())
	//	}
	//	totalCardinality += res.count
	//	res.set = nil
	//	res.lowCardinality = nil
	//	measurements = append(measurements, res)
	//}

	// if err != nil {
	// 	return err
	// }

	// sort measurements by cardinality.
	//sort.Sort(sort.Reverse(measurements))
	//
	//if report.topN > 0 {
	//	// There may not be "topN" measurement cardinality to sub-slice.
	//	n := int(math.Min(float64(report.topN), float64(len(measurements))))
	//	measurements = measurements[:n]
	//}

	// tw := tabwriter.NewWriter(report.Stdout, 4, 4, 1, '\t', 0)
	// fmt.Fprintf(tw, "Summary\nDatabase Path: %s\nCardinality (exact): %d\n\n", report.Path, totalCardinality)
	// fmt.Fprint(tw, "Measurement\tCardinality (exact)\n\n")
	// for _, res := range measurements {
	// 	fmt.Fprintf(tw, "%q\t\t%d\n", res.name, res.count)
	// }

	// if err := tw.Flush(); err != nil {
	// 	return err
	// }
	// fmt.Fprint(report.Stdout, "\n\n")
	report.printOrgBucketCardinality()
	return nil
}

//func (report *ReportCommand) printShardByMeasurement(id uint64) error {
//	allMap, ok := report.cardinalities[id]
//	if !ok {
//		return nil
//	}
//
//	var totalCardinality int64
//	all := make(cardinalities, 0, len(allMap))
//	for _, card := range allMap {
//		n := card.cardinality()
//		if n == 0 {
//			continue
//		}
//
//		totalCardinality += n
//		all = append(all, card)
//	}
//
//	sort.Sort(sort.Reverse(all))
//
//	// Trim to top-n
//	if report.topN > 0 {
//		// There may not be "topN" measurement cardinality to sub-slice.
//		n := int(math.Min(float64(report.topN), float64(len(all))))
//		all = all[:n]
//	}
//
//	tw := tabwriter.NewWriter(report.Stdout, 4, 4, 1, '\t', 0)
//	fmt.Fprintf(tw, "===============\nShard ID: %d\nPath: %s\nCardinality (exact): %d\n\n", id, report.shardPaths[id], totalCardinality)
//	fmt.Fprint(tw, "Measurement\tCardinality (exact)\n\n")
//	for _, card := range all {
//		fmt.Fprintf(tw, "%q\t\t%d\n", card.name, card.cardinality())
//	}
//	fmt.Fprint(tw, "===============\n\n")
//	if err := tw.Flush(); err != nil {
//		return err
//	}
//	fmt.Fprint(report.Stdout, "\n\n")
//	return nil
//}

func (report *ReportCommand) printOrgBucketCardinality() {
	tw := tabwriter.NewWriter(report.Stdout, 4, 4, 1, '\t', 0)
	//for k, v := range report.orgs {
	//	report.Logger.Error("calculating org")
	//	for _, j := range v {
	//		cCard := j.cardinality()
	//		totalCount = totalCount + j.cardinality()
	//		if influxdb.ID(k).String() == report.Org {
	//			orgCount += cCard
	//		}
	//	}
	//}
	//
	//for k, v := range report.buckets {
	//	for _, j := range v {
	//		cCard := j.cardinality()
	//		if influxdb.ID(k).String() == report.Bucket {
	//			bucketCount += cCard
	//		}
	//	}
	//}

	totalCard := int64(0)
	orgTotals := make(map[influxdb.ID]int64)
	for org, orgToBucket := range report.orgBucketCardinality {
		orgTotal := int64(0)
		for _, bucketCard := range orgToBucket {
			totalCard += bucketCard.cardinality()
			orgTotal += bucketCard.cardinality()
		}
		orgTotals[org] = orgTotal
	}

	fmt.Fprintf(tw, "Summary (total): %v \n\n", totalCard)

	fmt.Println(report.orgBucketCardinality)

	for orgName, orgToBucket := range report.orgBucketCardinality {
		fmt.Fprintf(tw, "Org %s total: %d \n\n", orgName.String(), orgTotals[orgName])
		for bucketName, bucketCard := range orgToBucket {
			fmt.Fprintf(tw, "    Bucket    %s    %d\n", bucketName.String(), bucketCard.cardinality())
		}
	}
}
