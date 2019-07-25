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

	byOrg               map[influxdb.ID]*cardinality
	byBucket            map[influxdb.ID]*cardinality
	byBucketMeasurement map[influxdb.ID]map[string]*cardinality
	orgToBucket         map[influxdb.ID][]influxdb.ID

	SeriesDirPath string // optional. Defaults to dbPath/_series
	sfile         *tsdb.SeriesFile
	indexFile     *Index

	topN          int
	ByMeasurement bool
	byTagKey      bool

	// How many goroutines to dedicate to calculating cardinality.
	Concurrency int
}

// NewReportCommand returns a new instance of ReportCommand with default setting applied.
func NewReportCommand() *ReportCommand {
	return &ReportCommand{
		Logger:              zap.NewNop(),
		byOrg:               make(map[influxdb.ID]*cardinality),
		byBucket:            make(map[influxdb.ID]*cardinality),
		byBucketMeasurement: make(map[influxdb.ID]map[string]*cardinality),
		orgToBucket:         make(map[influxdb.ID][]influxdb.ID),
		topN:                0,
		byTagKey:            false,
		Concurrency:         runtime.GOMAXPROCS(0),
	}
}

// ReportTSISummary is returned by a report-tsi Run() command and is used to access cardinality information
type Summary struct {
	OrgCardinality               map[influxdb.ID]int64
	BucketCardinality            map[influxdb.ID]int64
	BucketMeasurementCardinality map[influxdb.ID]map[string]int64
}

func newSummary() *Summary {
	return &Summary{
		OrgCardinality:               make(map[influxdb.ID]int64),
		BucketCardinality:            make(map[influxdb.ID]int64),
		BucketMeasurementCardinality: make(map[influxdb.ID]map[string]int64),
	}
}

// Run runs the report-tsi tool which can be used to find the cardinality
// any org or bucket. Run returns a *ReportTSISummary, which contains maps for finding
// the cardinality of a bucket or org based on it's influxdb.ID
// The *ReportTSISummary will be nil if there is a failure
func (report *ReportCommand) Run(print bool) (*Summary, error) {
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

	indexPath := filepath.Join(report.DataPath, "index")
	report.indexFile = NewIndex(sFile, NewConfig(), WithPath(indexPath))
	if err := report.indexFile.Open(context.Background()); err != nil {
		return nil, err
	}
	defer report.indexFile.Close()

	summary, err := report.calculateOrgBucketCardinality()
	if err != nil {
		return nil, err
	}

	if print {
		report.printCardinalitySummary(summary)
	}

	return summary, nil
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

func (report *ReportCommand) calculateCardinalities() error {
	idx := report.indexFile
	itr, err := idx.MeasurementIterator()
	if err != nil {
		return err
	} else if itr == nil {
		return nil
	}
	defer itr.Close()

	var totalCard = &cardinality{name: []byte("total")}
OUTER:
	for {
		name, err := itr.Next()
		if err != nil {
			return err
		} else if name == nil {
			break OUTER
		}

		var a [16]byte
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

		var orgCard, bucketCard *cardinality

		// initialize map of bucket to measurements
		if _, ok := report.byBucketMeasurement[bucket]; !ok {
			report.byBucketMeasurement[bucket] = make(map[string]*cardinality)
		}

		if c, ok := report.byBucket[bucket]; !ok {
			bucketCard = &cardinality{name: []byte(bucket.String())}
			report.byBucket[bucket] = bucketCard
		} else {
			bucketCard = c
		}

		if c, ok := report.byOrg[org]; !ok {
			orgCard = &cardinality{name: []byte(bucket.String())}
			report.byOrg[org] = orgCard
		} else {
			orgCard = c
		}

		if _, ok := report.orgToBucket[org]; !ok {
			report.orgToBucket[org] = []influxdb.ID{}
		}

		report.orgToBucket[org] = append(report.orgToBucket[org], bucket)

		var e tsdb.SeriesIDElem
		for e, err = sitr.Next(); err == nil && e.SeriesID.ID != 0; e, err = sitr.Next() {
			id := e.SeriesID.ID

			if id > math.MaxUint32 {
				panic(fmt.Sprintf("series ID is too large: %d (max %d). Corrupted series file?", e.SeriesID, uint32(math.MaxUint32)))
			}

			totalCard.add(id)

			// add cardinalities to org and bucket maps
			orgCard.add(id)
			bucketCard.add(id)

			_, tags := report.sfile.Series(e.SeriesID)
			if len(tags) == 0 {
				panic(fmt.Sprintf("series key too short"))
			}

			mName := string(tags[0].Value) // measurement name should be first tag.
			fmt.Println("")

			if report.ByMeasurement {
				var mCard *cardinality
				if cardForM, ok := report.byBucketMeasurement[bucket][mName]; !ok {
					mCard = &cardinality{name: []byte(mName)}
					report.byBucketMeasurement[bucket][mName] = mCard
				} else {
					mCard = cardForM
				}

				mCard.add(id)
			}
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

// TODO: remove... not needed (though possibly if we add concurrency)
type results []*result

func (a results) Len() int           { return len(a) }
func (a results) Less(i, j int) bool { return a[i].count < a[j].count }
func (a results) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// GetOrgCardinality returns the total cardinality of the org provided.
//// Can only be called after Run()
//func (report *ReportCommand) printOrgCardinality(orgID influxdb.ID) int64 {
//	orgTotal := int64(0)
//	for _, bucket := range report.orgBucketCardinality[orgID] {
//		orgTotal += bucket.cardinality()
//	}
//	return orgTotal
//}

// GetBucketCardinality returns the total cardinality of the bucket in the org provided
//// Can only be called after Run()
//func (report *ReportCommand) printBucketCardinality(orgID, bucketID influxdb.ID) int64 {
//	return report.orgBucketCardinality[orgID][bucketID].cardinality()
//}

func (report *ReportCommand) calculateOrgBucketCardinality() (*Summary, error) {
	if err := report.calculateCardinalities(); err != nil {
		return nil, err
	}
	// Generate a new summary
	summary := newSummary()
	for bucketID, bucketCard := range report.byBucket {
		summary.BucketCardinality[bucketID] = bucketCard.cardinality()
		summary.BucketMeasurementCardinality[bucketID] = make(map[string]int64)
	}

	for orgID, orgCard := range report.byOrg {
		summary.OrgCardinality[orgID] = orgCard.cardinality()
	}

	for bucketID, bucketMeasurement := range report.byBucketMeasurement {
		for mName, mCard := range bucketMeasurement {
			summary.BucketMeasurementCardinality[bucketID][mName] = mCard.cardinality()
		}
	}

	return summary, nil
}

func (report *ReportCommand) printCardinalitySummary(summary *Summary) {
	tw := tabwriter.NewWriter(report.Stdout, 4, 4, 1, '\t', 0)

	for orgID, orgCard := range summary.OrgCardinality {
		fmt.Fprintf(tw, "Org %s total: %d\n", orgID.String(), orgCard)

		for _, bucketID := range report.orgToBucket[orgID] {
			fmt.Fprintf(tw, "\tBucket %s total: %d\n", bucketID.String(), summary.BucketCardinality[bucketID])
			if report.ByMeasurement {
				for mName, mCard := range summary.BucketMeasurementCardinality[bucketID] {
					fmt.Fprintf(tw, "\t\t_m=%s\t%d\n", mName, mCard)
				}
			}
		}
	}

	tw.Flush()
}
