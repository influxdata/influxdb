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

// NewReportCommand returns a new instance of ReportCommand with default setting applied.
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

// ReportTsiSummary is returned by a report-tsi Run() command and is used to access cardinality information
type ReportTsiSummary struct {
	OrgCardinality    map[influxdb.ID]int64
	BucketCardinality map[influxdb.ID]int64
}

func newTsiSummary() *ReportTsiSummary {
	return &ReportTsiSummary{
		OrgCardinality:    map[influxdb.ID]int64{},
		BucketCardinality: map[influxdb.ID]int64{},
	}
}

// Run runs the report-tsi tool which can be used to find the cardinality
// any org or bucket. Run returns a *ReportTsiSummary, which contains maps for finding
// the cardinality of a bucket or org based on it's influxdb.ID
// The *ReportTsiSummary will be nil if there is a failure
func (report *ReportCommand) Run() (*ReportTsiSummary, error) {
	report.Stdout = os.Stdout

	if report.SeriesDirPath == "" {
		report.SeriesDirPath = filepath.Join(report.DataPath, "_series")
	}

	sFile := tsdb.NewSeriesFile(report.SeriesDirPath)
	sFile.WithLogger(report.Logger)
	if err := sFile.Open(context.Background()); err != nil {
		report.Logger.Error("failed to open series")
		return nil, err
	}
	defer sFile.Close()
	report.sfile = sFile

	path := filepath.Join(report.DataPath, "index")
	report.indexFile = NewIndex(sFile, NewConfig(), WithPath(path))
	if err := report.indexFile.Open(context.Background()); err != nil {
		return nil, err
	}
	defer report.indexFile.Close()

	// Calculate cardinalities for every org and bucket
	fn := report.cardinalityByMeasurement

	// Blocks until all work done.
	report.calculateCardinalities(fn)

	// Generate and print summary
	var summary *ReportTsiSummary
	tw := tabwriter.NewWriter(report.Stdout, 4, 4, 1, '\t', 0)

	// if no org or bucket flags have been specified, print everything
	// if not, only print the specified org/bucket
	if report.OrgID == nil {
		summary = report.printOrgBucketCardinality(true)
	} else {
		// still need to generate a summary, just without printing
		summary = report.printOrgBucketCardinality(false)

		// if we do not have a bucket, print the cardinality of OrgID
		if report.BucketID == nil {
			fmt.Fprintf(tw, "Org (%v) Cardinality: %v \n\n", report.OrgID, summary.OrgCardinality[*report.OrgID])
		} else {
			fmt.Fprintf(tw, "Bucket (%v) Cardinality: %v \n\n", report.BucketID, summary.BucketCardinality[*report.BucketID])
		}
		tw.Flush()
	}
	return summary, nil
}

// calculateCardinalities calculates the cardinalities of the set of shard being
// worked on concurrently. The provided function determines how cardinality is
// calculated and broken down.
func (report *ReportCommand) calculateCardinalities(fn func() error) error {
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

// GetOrgCardinality returns the total cardinality of the org provided.
// Can only be called after Run()
func (report *ReportCommand) printOrgCardinality(orgID influxdb.ID) int64 {
	orgTotal := int64(0)
	for _, bucket := range report.orgBucketCardinality[orgID] {
		orgTotal += bucket.cardinality()
	}
	return orgTotal
}

// GetBucketCardinality returns the total cardinality of the bucket in the org provided
// Can only be called after Run()
func (report *ReportCommand) printBucketCardinality(orgID, bucketID influxdb.ID) int64 {
	return report.orgBucketCardinality[orgID][bucketID].cardinality()
}

func (report *ReportCommand) printOrgBucketCardinality(print bool) *ReportTsiSummary {
	tw := tabwriter.NewWriter(report.Stdout, 4, 4, 1, '\t', 0)

	// Generate a new summary
	summary := newTsiSummary()

	totalCard := int64(0)
	orgTotals := make(map[influxdb.ID]int64)
	for org, orgToBucket := range report.orgBucketCardinality {
		orgTotal := int64(0)
		for bucketID, bucketCard := range orgToBucket {
			c := bucketCard.cardinality()
			totalCard += c
			orgTotal += c
			summary.BucketCardinality[bucketID] = c
		}
		orgTotals[org] = orgTotal
		summary.OrgCardinality[org] = orgTotal
	}

	if print {
		fmt.Fprintf(tw, "Summary (total): %v \n\n", totalCard)

		fmt.Println(report.orgBucketCardinality)

		for orgName, orgToBucket := range report.orgBucketCardinality {
			fmt.Fprintf(tw, "Org %s total: %d \n\n", orgName.String(), summary.OrgCardinality[orgName])
			for bucketName := range orgToBucket {
				fmt.Fprintf(tw, "    Bucket    %s    %d\n", bucketName.String(), summary.BucketCardinality[bucketName])
			}
		}
	}

	return summary
}
