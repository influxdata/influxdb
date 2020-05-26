package tsm1

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/hll"
	"github.com/influxdata/influxdb/v2/tsdb"
)

// Report runs a report over tsm data
type Report struct {
	Stderr io.Writer
	Stdout io.Writer

	Dir             string
	OrgID, BucketID *influxdb.ID // Calculate only results for the provided org or bucket id.
	Pattern         string       // Providing "01.tsm" for example would filter for level 1 files.
	Detailed        bool         // Detailed will segment cardinality by tag keys.
	Exact           bool         // Exact determines if estimation or exact methods are used to determine cardinality.
}

// ReportSummary provides a summary of the cardinalities in the processed fileset.
type ReportSummary struct {
	Min, Max      int64
	Total         uint64            //The exact or estimated unique set of series keys across all files.
	Organizations map[string]uint64 // The exact or estimated unique set of series keys segmented by org.
	Buckets       map[string]uint64 // The exact or estimated unique set of series keys segmented by bucket.

	// These are calculated when the detailed flag is in use.
	Measurements map[string]uint64 // The exact or estimated unique set of series keys segmented by the measurement tag.
	FieldKeys    map[string]uint64 // The exact or estimated unique set of series keys segmented by the field tag.
	TagKeys      map[string]uint64 // The exact or estimated unique set of series keys segmented by tag keys.
}

func newReportSummary() *ReportSummary {
	return &ReportSummary{
		Organizations: map[string]uint64{},
		Buckets:       map[string]uint64{},
		Measurements:  map[string]uint64{},
		FieldKeys:     map[string]uint64{},
		TagKeys:       map[string]uint64{},
	}
}

// Run executes the Report.
//
// Calling Run with print set to true emits data about each file to the report's
// Stdout fd as it is generated.
func (r *Report) Run(print bool) (*ReportSummary, error) {
	if r.Stderr == nil {
		r.Stderr = os.Stderr
	}
	if r.Stdout == nil {
		r.Stdout = os.Stdout
	}

	if !print {
		r.Stderr, r.Stdout = ioutil.Discard, ioutil.Discard
	}

	newCounterFn := newHLLCounter
	estTitle := " (est)"
	if r.Exact {
		estTitle = ""
		newCounterFn = newExactCounter
	}

	fi, err := os.Stat(r.Dir)
	if err != nil {
		return nil, err
	} else if !fi.IsDir() {
		return nil, errors.New("data directory not valid")
	}

	totalSeries := newCounterFn()               // The exact or estimated unique set of series keys across all files.
	orgCardinalities := map[string]counter{}    // The exact or estimated unique set of series keys segmented by org.
	bucketCardinalities := map[string]counter{} // The exact or estimated unique set of series keys segmented by bucket.

	// These are calculated when the detailed flag is in use.
	mCardinalities := map[string]counter{} // The exact or estimated unique set of series keys segmented by the measurement tag.
	fCardinalities := map[string]counter{} // The exact or estimated unique set of series keys segmented by the field tag.
	tCardinalities := map[string]counter{} // The exact or estimated unique set of series keys segmented by tag keys.

	start := time.Now()

	tw := tabwriter.NewWriter(r.Stdout, 8, 2, 1, ' ', 0)
	fmt.Fprintln(tw, strings.Join([]string{"File", "Series", "New" + estTitle, "Min Time", "Max Time", "Load Time"}, "\t"))

	minTime, maxTime := int64(math.MaxInt64), int64(math.MinInt64)

	files, err := filepath.Glob(filepath.Join(r.Dir, "*.tsm"))
	if err != nil {
		panic(err) // Only error would be a bad pattern; not runtime related.
	}
	var processedFiles int

	var tagBuf models.Tags // Buffer that can be re-used when parsing keys.
	for _, path := range files {
		if r.Pattern != "" && !strings.Contains(path, r.Pattern) {
			continue
		}

		file, err := os.OpenFile(path, os.O_RDONLY, 0600)
		if err != nil {
			fmt.Fprintf(r.Stderr, "error: %s: %v. Exiting.\n", path, err)
			return nil, err
		}

		loadStart := time.Now()
		reader, err := NewTSMReader(file)
		if err != nil {
			fmt.Fprintf(r.Stderr, "error: %s: %v. Skipping file.\n", file.Name(), err)
			continue
		}
		loadTime := time.Since(loadStart)
		processedFiles++

		// Tracks the current total, so it's possible to know how many new series this file adds.
		currentTotalCount := totalSeries.Count()

		seriesCount := reader.KeyCount()
		itr := reader.Iterator(nil)
		if itr == nil {
			return nil, errors.New("invalid TSM file, no index iterator")
		}

		for itr.Next() {
			key := itr.Key()

			var a [16]byte // TODO(edd) if this shows up we can use a different API to DecodeName.
			copy(a[:], key[:16])
			org, bucket := tsdb.DecodeName(a)
			if r.OrgID != nil && *r.OrgID != org { // If filtering on single org or bucket then skip if no match
				// org does not match.
				continue
			} else if r.BucketID != nil && *r.BucketID != bucket {
				// bucket does not match.
				continue
			}

			totalSeries.Add(key) // Update total cardinality.

			// Update org cardinality
			orgCount := orgCardinalities[org.String()]
			if orgCount == nil {
				orgCount = newCounterFn()
				orgCardinalities[org.String()] = orgCount
			}
			orgCount.Add(key)

			// Update bucket cardinality.
			bucketCount := bucketCardinalities[bucket.String()]
			if bucketCount == nil {
				bucketCount = newCounterFn()
				bucketCardinalities[bucket.String()] = bucketCount
			}
			bucketCount.Add(key)

			// Update tag cardinalities.
			if r.Detailed {
				sep := bytes.Index(key, KeyFieldSeparatorBytes)
				seriesKey := key[:sep] // Snip the tsm1 field key off.
				_, tagBuf = models.ParseKeyBytesWithTags(seriesKey, tagBuf)

				for _, t := range tagBuf {
					tk := string(t.Key)
					switch tk {
					case models.MeasurementTagKey:
						mname := string(t.Value)
						// Total series cardinality segmented by measurement name.
						mCount := mCardinalities[mname] // measurement name.
						if mCount == nil {
							mCount = newCounterFn()
							mCardinalities[mname] = mCount
						}
						mCount.Add(key) // full series keys associated with measurement name.
					case models.FieldKeyTagKey:
						mname := tagBuf.GetString(models.MeasurementTagKey)
						fCount := fCardinalities[mname]
						if fCount == nil {
							fCount = newCounterFn()
							fCardinalities[mname] = fCount
						}
						fCount.Add(t.Value) // field keys associated with measurement name.
					default:
						tagCount := tCardinalities[tk]
						if tagCount == nil {
							tagCount = newCounterFn()
							tCardinalities[tk] = tagCount
						}
						tagCount.Add(t.Value)
					}
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

		if err := reader.Close(); err != nil {
			return nil, fmt.Errorf("error: %s: %v. Exiting", path, err)
		}

		fmt.Fprintln(tw, strings.Join([]string{
			filepath.Base(file.Name()),
			strconv.FormatInt(int64(seriesCount), 10),
			strconv.FormatInt(int64(totalSeries.Count()-currentTotalCount), 10),
			time.Unix(0, minT).UTC().Format(time.RFC3339Nano),
			time.Unix(0, maxT).UTC().Format(time.RFC3339Nano),
			loadTime.String(),
		}, "\t"))
		if r.Detailed {
			if err := tw.Flush(); err != nil {
				return nil, err
			}
		}
	}

	if err := tw.Flush(); err != nil {
		return nil, err
	}

	summary := newReportSummary()
	summary.Min = minTime
	summary.Max = maxTime
	summary.Total = totalSeries.Count()

	println()

	println("Summary:")
	fmt.Printf("  Files: %d (%d skipped)\n", processedFiles, len(files)-processedFiles)
	fmt.Printf("  Series Cardinality%s: %d\n", estTitle, totalSeries.Count())
	fmt.Printf("  Time Range: %s - %s\n",
		time.Unix(0, minTime).UTC().Format(time.RFC3339Nano),
		time.Unix(0, maxTime).UTC().Format(time.RFC3339Nano),
	)
	fmt.Printf("  Duration: %s \n", time.Unix(0, maxTime).Sub(time.Unix(0, minTime)))
	println()

	fmt.Printf("Statistics\n")
	fmt.Printf("  Organizations (%d):\n", len(orgCardinalities))
	for _, org := range sortKeys(orgCardinalities) {
		cardinality := orgCardinalities[org].Count()
		summary.Organizations[org] = cardinality
		fmt.Printf("     - %s: %d%s (%d%%)\n", org, cardinality, estTitle, int(float64(cardinality)/float64(totalSeries.Count())*100))
	}
	fmt.Printf("  Total%s: %d\n", estTitle, totalSeries.Count())

	fmt.Printf(" \n Buckets (%d):\n", len(bucketCardinalities))
	for _, bucket := range sortKeys(bucketCardinalities) {
		cardinality := bucketCardinalities[bucket].Count()
		summary.Buckets[bucket] = cardinality
		fmt.Printf("     - %s: %d%s (%d%%)\n", bucket, cardinality, estTitle, int(float64(cardinality)/float64(totalSeries.Count())*100))
	}
	fmt.Printf("  Total%s: %d\n", estTitle, totalSeries.Count())

	if r.Detailed {
		fmt.Printf("\n  Series By Measurements (%d):\n", len(mCardinalities))
		for _, mname := range sortKeys(mCardinalities) {
			cardinality := mCardinalities[mname].Count()
			summary.Measurements[mname] = cardinality
			fmt.Printf("    - %v: %d%s (%d%%)\n", mname, cardinality, estTitle, int((float64(cardinality)/float64(totalSeries.Count()))*100))
		}

		fmt.Printf("\n  Fields By Measurements (%d):\n", len(fCardinalities))
		for _, mname := range sortKeys(fCardinalities) {
			cardinality := fCardinalities[mname].Count()
			summary.FieldKeys[mname] = cardinality
			fmt.Printf("    - %v: %d%s\n", mname, cardinality, estTitle)
		}

		fmt.Printf("\n  Tag Values By Tag Keys (%d):\n", len(tCardinalities))
		for _, tkey := range sortKeys(tCardinalities) {
			cardinality := tCardinalities[tkey].Count()
			summary.TagKeys[tkey] = cardinality
			fmt.Printf("    - %v: %d%s\n", tkey, cardinality, estTitle)
		}
	}

	fmt.Printf("\nCompleted in %s\n", time.Since(start))
	return summary, nil
}

// sortKeys is a quick helper to return the sorted set of a map's keys
func sortKeys(vals map[string]counter) (keys []string) {
	for k := range vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return keys
}

// counter abstracts a a method of counting keys.
type counter interface {
	Add(key []byte)
	Count() uint64
}

// newHLLCounter returns an approximate counter using HyperLogLogs for cardinality estimation.
func newHLLCounter() counter {
	return hll.NewDefaultPlus()
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
