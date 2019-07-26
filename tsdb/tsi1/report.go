package tsi1

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

const (
	// Number of series IDs to stored in slice before we convert to a roaring
	// bitmap. Roaring bitmaps have a non-trivial initial cost to construct.
	useBitmapN = 25
)

// ReportCommand represents the program execution for "influxd inspect report-tsi".
type ReportCommand struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer
	Logger *zap.Logger

	// Filters
	DataPath        string
	OrgID, BucketID *influxdb.ID

	byOrgBucket         map[influxdb.ID]map[influxdb.ID]*cardinality
	byBucketMeasurement map[influxdb.ID]map[string]*cardinality
	orgToBucket         map[influxdb.ID][]influxdb.ID

	SeriesDirPath string // optional. Defaults to dbPath/_series
	sfile         *tsdb.SeriesFile
	indexFile     *Index

	TopN          int
	ByMeasurement bool
	byTagKey      bool

	start time.Time
}

// NewReportCommand returns a new instance of ReportCommand with default setting applied.
func NewReportCommand() *ReportCommand {
	return &ReportCommand{
		Logger:              zap.NewNop(),
		byOrgBucket:         make(map[influxdb.ID]map[influxdb.ID]*cardinality),
		byBucketMeasurement: make(map[influxdb.ID]map[string]*cardinality),
		orgToBucket:         make(map[influxdb.ID][]influxdb.ID),
		TopN:                0,
		byTagKey:            false,
	}
}

// ReportTSISummary is returned by a report-tsi Run() command and is used to access cardinality information
type Summary struct {
	TotalCardinality             int64
	OrgCardinality               map[influxdb.ID]int64
	BucketByOrgCardinality       map[influxdb.ID]map[influxdb.ID]int64
	BucketMeasurementCardinality map[influxdb.ID]map[string]int64
}

func newSummary() *Summary {
	return &Summary{
		OrgCardinality:               make(map[influxdb.ID]int64),
		BucketByOrgCardinality:       make(map[influxdb.ID]map[influxdb.ID]int64),
		BucketMeasurementCardinality: make(map[influxdb.ID]map[string]int64),
	}
}

// Run runs the report-tsi tool which can be used to find the cardinality
// any org or bucket. Run returns a *ReportTSISummary, which contains maps for finding
// the cardinality of a bucket or org based on its influxdb.ID
func (report *ReportCommand) Run(print bool) (*Summary, error) {
	report.start = time.Now()
	report.Stdout = os.Stdout

	if report.SeriesDirPath == "" {
		report.SeriesDirPath = filepath.Join(report.DataPath, "_series")
	}

	sFile := tsdb.NewSeriesFile(report.SeriesDirPath)

	// TODO: do we actually want the seriesfile logging?
	// sFile.WithLogger(report.Logger)

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

func (report *ReportCommand) calculateCardinalities() error {
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

		// decode org and bucket from measurement name
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

		var bucketCard *cardinality

		// initialize map of bucket to measurements
		if _, ok := report.byBucketMeasurement[bucket]; !ok {
			report.byBucketMeasurement[bucket] = make(map[string]*cardinality)
		}

		if _, ok := report.byOrgBucket[org]; !ok {
			report.byOrgBucket[org] = make(map[influxdb.ID]*cardinality)
		}

		// initialize total cardinality tracking struct for this bucket
		if c, ok := report.byOrgBucket[org][bucket]; !ok {
			bucketCard = &cardinality{name: []byte(bucket.String())}
			report.byOrgBucket[org][bucket] = bucketCard
		} else {
			bucketCard = c
		}

		var e tsdb.SeriesIDElem
		for e, err = sitr.Next(); err == nil && e.SeriesID.ID != 0; e, err = sitr.Next() {
			id := e.SeriesID.ID

			if id > math.MaxUint32 {
				panic(fmt.Sprintf("series ID is too large: %d (max %d). Corrupted series file?", e.SeriesID, uint32(math.MaxUint32)))
			}

			// add cardinality to bucket
			bucketCard.add(id)

			// retrieve tags associated with series id so we can get
			// associated measurement
			_, tags := report.sfile.Series(e.SeriesID)
			if len(tags) == 0 {
				panic(fmt.Sprintf("empty series key"))
			}

			// measurement name should be first tag
			mName := string(tags[0].Value)

			// update measurement-level cardinality if tracking by measurement
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

func (report *ReportCommand) calculateOrgBucketCardinality() (*Summary, error) {
	if err := report.calculateCardinalities(); err != nil {
		return nil, err
	}

	var totalCard int64
	// Generate a new summary
	summary := newSummary()
	for orgID, bucketMap := range report.byOrgBucket {
		summary.BucketByOrgCardinality[orgID] = make(map[influxdb.ID]int64)
		orgTotal := int64(0)
		for bucketID, bucketCard := range bucketMap {
			count := bucketCard.cardinality()
			summary.BucketByOrgCardinality[orgID][bucketID] = count
			summary.BucketMeasurementCardinality[bucketID] = make(map[string]int64)
			orgTotal += count
			totalCard += count
		}
		summary.OrgCardinality[orgID] = orgTotal
	}

	summary.TotalCardinality = totalCard

	for bucketID, bucketMeasurement := range report.byBucketMeasurement {
		for mName, mCard := range bucketMeasurement {
			summary.BucketMeasurementCardinality[bucketID][mName] = mCard.cardinality()
		}
	}

	return summary, nil
}

func (report *ReportCommand) printCardinalitySummary(summary *Summary) {
	tw := tabwriter.NewWriter(report.Stdout, 4, 4, 1, '\t', 0)
	fmt.Fprint(tw, "\n")

	fmt.Fprintf(tw, "Total: %d\n", summary.TotalCardinality)
	// sort total org and bucket  and limit to top n values
	sortedOrgs := sortKeys(summary.OrgCardinality, report.TopN)

	for i, orgResult := range sortedOrgs {
		orgID, _ := influxdb.IDFromString(orgResult.id)
		sortedBuckets := sortKeys(summary.BucketByOrgCardinality[*orgID], report.TopN)
		// if we specify a bucket, we do not print the org cardinality
		fmt.Fprintln(tw, "===============")
		if report.BucketID == nil {
			fmt.Fprintf(tw, "Org %s total: %d\n", orgResult.id, orgResult.card)
		}

		for _, bucketResult := range sortedBuckets {
			fmt.Fprintf(tw, "\tBucket %s total: %d\n", bucketResult.id, bucketResult.card)

			if report.ByMeasurement {
				bucketID, _ := influxdb.IDFromString(bucketResult.id)
				sortedMeasurements := sortMeasurements(summary.BucketMeasurementCardinality[*bucketID], report.TopN)

				for _, measResult := range sortedMeasurements {
					fmt.Fprintf(tw, "\t\t_m=%s\t%d\n", measResult.id, measResult.card)
				}
			}
		}
		if i == len(sortedOrgs)-1 {
			fmt.Fprintln(tw, "===============")
		}
	}
	fmt.Fprint(tw, "\n\n")

	elapsed := time.Since(report.start)
	fmt.Fprintf(tw, "Finished in %v\n", elapsed)

	tw.Flush()
}

// sortKeys is a quick helper to return the sorted set of a map's keys
// sortKeys will only return report.topN keys if the flag is set
type result struct {
	id   string
	card int64
}

type resultList []result

func (a resultList) Len() int           { return len(a) }
func (a resultList) Less(i, j int) bool { return a[i].card < a[j].card }
func (a resultList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func sortKeys(vals map[influxdb.ID]int64, topN int) resultList {
	sorted := make(resultList, 0)
	for k, v := range vals {
		sorted = append(sorted, result{k.String(), v})
	}
	sort.Sort(sort.Reverse(sorted))

	if topN == 0 {
		return sorted
	}
	if topN > len(sorted) {
		topN = len(sorted)
	}
	return sorted[:topN]
}

func sortMeasurements(vals map[string]int64, topN int) resultList {
	sorted := make(resultList, 0)
	for k, v := range vals {
		sorted = append(sorted, result{k, v})
	}
	sort.Sort(sort.Reverse(sorted))

	if topN == 0 {
		return sorted
	}
	if topN > len(sorted) {
		topN = len(sorted)
	}
	return sorted[:topN]
}
