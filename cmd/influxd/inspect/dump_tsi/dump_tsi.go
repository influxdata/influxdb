package dump_tsi

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"text/tabwriter"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/index/tsi1"
	"github.com/spf13/cobra"
)

type args struct {
	seriesFilePath string
	paths          []string

	showSeries         bool
	showMeasurements   bool
	showTagKeys        bool
	showTagValues      bool
	showTagValueSeries bool

	measurementFilter *regexp.Regexp
	tagKeyFilter      *regexp.Regexp
	tagValueFilter    *regexp.Regexp

	w io.Writer
}

func NewDumpTSICommand() *cobra.Command {
	var arguments args
	var measurementFilter, tagKeyFilter, tagValueFilter string
	cmd := &cobra.Command{
		Use:   "dumptsi",
		Short: "Dumps low-level details about tsi1 files.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Parse filters.
			if measurementFilter != "" {
				re, err := regexp.Compile(measurementFilter)
				if err != nil {
					return fmt.Errorf("failed to parse regex %q: %w", measurementFilter, err)
				}
				arguments.measurementFilter = re
			}
			if tagKeyFilter != "" {
				re, err := regexp.Compile(tagKeyFilter)
				if err != nil {
					return fmt.Errorf("failed to parse regex %q: %w", tagKeyFilter, err)
				}
				arguments.tagKeyFilter = re
			}
			if tagValueFilter != "" {
				re, err := regexp.Compile(tagValueFilter)
				if err != nil {
					return fmt.Errorf("failed to parse regex %q: %w", tagValueFilter, err)
				}
				arguments.tagValueFilter = re
			}

			arguments.paths = args
			if len(arguments.paths) == 0 {
				return fmt.Errorf("at least one path required")
			}

			// Some flags imply other flags.
			if arguments.showTagValueSeries {
				arguments.showTagValues = true
			}
			if arguments.showTagValues {
				arguments.showTagKeys = true
			}
			if arguments.showTagKeys {
				arguments.showMeasurements = true
			}

			arguments.w = cmd.OutOrStdout()
			return arguments.run()
		},
	}

	cmd.Flags().StringVar(&arguments.seriesFilePath, "series-file", "",
		"Path to series file")
	cmd.Flags().BoolVar(&arguments.showSeries, "series", false,
		"Show raw series data")
	cmd.Flags().BoolVar(&arguments.showMeasurements, "measurements", false,
		"Show raw measurement data")
	cmd.Flags().BoolVar(&arguments.showTagKeys, "tag-keys", false,
		"Show raw tag key data")
	cmd.Flags().BoolVar(&arguments.showTagValues, "tag-values", false,
		"Show raw tag value data")
	cmd.Flags().BoolVar(&arguments.showTagValueSeries, "tag-value-series", false,
		"Show raw series data for each value")
	cmd.Flags().StringVar(&measurementFilter, "measurement-filter", "",
		"Regex measurement filter")
	cmd.Flags().StringVar(&tagKeyFilter, "tag-key-filter", "",
		"Regex tag key filter")
	cmd.Flags().StringVar(&tagValueFilter, "tag-value-filter", "",
		"Regex tag value filter")

	cmd.MarkFlagRequired("series-file")

	return cmd
}

func (a *args) run() error {
	sfile := tsdb.NewSeriesFile(a.seriesFilePath)
	if err := sfile.Open(); err != nil {
		return err
	}
	defer sfile.Close()

	// Build a file set from the paths on the command line.
	idx, fs, err := a.readFileSet(sfile)
	if err != nil {
		return err
	}
	if fs != nil {
		defer fs.Release()
		defer fs.Close()
	}
	defer idx.Close()

	if a.showSeries {
		if err := a.printSeries(sfile); err != nil {
			return err
		}
	}

	// If this is an ad-hoc fileset then process it and close afterward.
	if fs != nil {
		if a.showSeries || a.showMeasurements {
			return a.printMeasurements(sfile, fs)
		}
		return a.printFileSummaries(fs)
	}

	// Otherwise iterate over each partition in the index.
	for i := 0; i < int(idx.PartitionN); i++ {
		if err := func() error {
			fs, err := idx.PartitionAt(i).RetainFileSet()
			if err != nil {
				return err
			}
			defer fs.Release()

			if a.showSeries || a.showMeasurements {
				return a.printMeasurements(sfile, fs)
			}
			return a.printFileSummaries(fs)
		}(); err != nil {
			return err
		}
	}
	return nil
}

func (a *args) readFileSet(sfile *tsdb.SeriesFile) (*tsi1.Index, *tsi1.FileSet, error) {
	// If only one path exists and it's a directory then open as an index.
	if len(a.paths) == 1 {
		fi, err := os.Stat(a.paths[0])
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get FileInfo of %q: %w", a.paths[0], err)
		} else if fi.IsDir() {
			// Verify directory is an index before opening it.
			if ok, err := tsi1.IsIndexDir(a.paths[0]); err != nil {
				return nil, nil, err
			} else if !ok {
				return nil, nil, fmt.Errorf("not an index directory: %q", a.paths[0])
			}

			idx := tsi1.NewIndex(sfile,
				"",
				tsi1.WithPath(a.paths[0]),
				tsi1.DisableCompactions(),
			)
			if err := idx.Open(); err != nil {
				return nil, nil, fmt.Errorf("failed to open TSI Index at %q: %w", idx.Path(), err)
			}
			return idx, nil, nil
		}
	}

	// Open each file and group into a fileset.
	var files []tsi1.File
	for _, path := range a.paths {
		switch ext := filepath.Ext(path); ext {
		case tsi1.LogFileExt:
			f := tsi1.NewLogFile(sfile, path)
			if err := f.Open(); err != nil {
				return nil, nil, fmt.Errorf("failed to get TSI logfile at %q: %w", sfile.Path(), err)
			}
			files = append(files, f)

		case tsi1.IndexFileExt:
			f := tsi1.NewIndexFile(sfile)
			f.SetPath(path)
			if err := f.Open(); err != nil {
				return nil, nil, fmt.Errorf("failed to open index file at %q: %w", f.Path(), err)
			}
			files = append(files, f)

		default:
			return nil, nil, fmt.Errorf("unexpected file extension: %s", ext)
		}
	}

	fs := tsi1.NewFileSet(sfile, files)
	fs.Retain()

	return nil, fs, nil
}

func (a *args) printSeries(sfile *tsdb.SeriesFile) error {
	if !a.showSeries {
		return nil
	}

	// Print header.
	tw := tabwriter.NewWriter(a.w, 8, 8, 1, '\t', 0)
	fmt.Fprintln(tw, "Series\t")

	// Iterate over each series.
	itr := sfile.SeriesIDIterator()
	for {
		e, err := itr.Next()
		if err != nil {
			return fmt.Errorf("failed to get next series ID in %q: %w", sfile.Path(), err)
		} else if e.SeriesID == 0 {
			break
		}
		name, tags := tsdb.ParseSeriesKey(sfile.SeriesKey(e.SeriesID))

		if !a.matchSeries(name, tags) {
			continue
		}

		deleted := sfile.IsDeleted(e.SeriesID)

		fmt.Fprintf(tw, "%s%s\t%v\n", name, tags.HashKey(), deletedString(deleted))
	}

	// Flush & write footer spacing.
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("failed to flush tabwriter: %w", err)
	}
	fmt.Fprint(a.w, "\n\n")

	return nil
}

func (a *args) printMeasurements(sfile *tsdb.SeriesFile, fs *tsi1.FileSet) error {
	if !a.showMeasurements {
		return nil
	}

	tw := tabwriter.NewWriter(a.w, 8, 8, 1, '\t', 0)
	fmt.Fprintln(tw, "Measurement\t")

	// Iterate over each series.
	if itr := fs.MeasurementIterator(); itr != nil {
		for e := itr.Next(); e != nil; e = itr.Next() {
			if a.measurementFilter != nil && !a.measurementFilter.Match(e.Name()) {
				continue
			}

			fmt.Fprintf(tw, "%s\t%v\n", e.Name(), deletedString(e.Deleted()))
			if err := tw.Flush(); err != nil {
				return fmt.Errorf("failed to flush tabwriter: %w", err)
			}

			if err := a.printTagKeys(sfile, fs, e.Name()); err != nil {
				return err
			}
		}
	}

	fmt.Fprint(a.w, "\n\n")

	return nil
}

func (a *args) printTagKeys(sfile *tsdb.SeriesFile, fs *tsi1.FileSet, name []byte) error {
	if !a.showTagKeys {
		return nil
	}

	// Iterate over each key.
	tw := tabwriter.NewWriter(a.w, 8, 8, 1, '\t', 0)
	itr := fs.TagKeyIterator(name)
	for e := itr.Next(); e != nil; e = itr.Next() {
		if a.tagKeyFilter != nil && !a.tagKeyFilter.Match(e.Key()) {
			continue
		}

		fmt.Fprintf(tw, "    %s\t%v\n", e.Key(), deletedString(e.Deleted()))
		if err := tw.Flush(); err != nil {
			return fmt.Errorf("failed to flush tabwriter: %w", err)
		}

		if err := a.printTagValues(sfile, fs, name, e.Key()); err != nil {
			return err
		}
	}
	fmt.Fprint(a.w, "\n")

	return nil
}

func (a *args) printTagValues(sfile *tsdb.SeriesFile, fs *tsi1.FileSet, name, key []byte) error {
	if !a.showTagValues {
		return nil
	}

	// Iterate over each value.
	tw := tabwriter.NewWriter(a.w, 8, 8, 1, '\t', 0)
	itr := fs.TagValueIterator(name, key)
	for e := itr.Next(); e != nil; e = itr.Next() {
		if a.tagValueFilter != nil && !a.tagValueFilter.Match(e.Value()) {
			continue
		}

		fmt.Fprintf(tw, "        %s\t%v\n", e.Value(), deletedString(e.Deleted()))
		if err := tw.Flush(); err != nil {
			return fmt.Errorf("failed to flush tabwriter: %w", err)
		}

		if err := a.printTagValueSeries(sfile, fs, name, key, e.Value()); err != nil {
			return err
		}
	}
	fmt.Fprint(a.w, "\n")

	return nil
}

func (a *args) printTagValueSeries(sfile *tsdb.SeriesFile, fs *tsi1.FileSet, name, key, value []byte) error {
	if !a.showTagValueSeries {
		return nil
	}

	// Iterate over each series.
	tw := tabwriter.NewWriter(a.w, 8, 8, 1, '\t', 0)
	itr, err := fs.TagValueSeriesIDIterator(name, key, value)
	if err != nil {
		return fmt.Errorf("failed to get series ID iterator with name %q: %w", name, err)
	}
	for {
		e, err := itr.Next()
		if err != nil {
			return fmt.Errorf("failed to print tag value series: %w", err)
		} else if e.SeriesID == 0 {
			break
		}

		name, tags := tsdb.ParseSeriesKey(sfile.SeriesKey(e.SeriesID))

		if !a.matchSeries(name, tags) {
			continue
		}

		fmt.Fprintf(tw, "            %s%s\n", name, tags.HashKey())
		if err := tw.Flush(); err != nil {
			return fmt.Errorf("failed to flush tabwriter: %w", err)
		}
	}
	fmt.Fprint(a.w, "\n")

	return nil
}

func (a *args) printFileSummaries(fs *tsi1.FileSet) error {
	for _, f := range fs.Files() {
		switch f := f.(type) {
		case *tsi1.LogFile:
			if err := a.printLogFileSummary(f); err != nil {
				return err
			}
		case *tsi1.IndexFile:
			if err := a.printIndexFileSummary(f); err != nil {
				return err
			}
		default:
			panic("unreachable")
		}
		fmt.Fprintln(a.w, "")
	}
	return nil
}

func (a *args) printLogFileSummary(f *tsi1.LogFile) error {
	fmt.Fprintf(a.w, "[LOG FILE] %s\n", filepath.Base(f.Path()))
	tw := tabwriter.NewWriter(a.w, 8, 8, 1, '\t', 0)
	fmt.Fprintf(tw, "Series:\t%d\n", f.SeriesN())
	fmt.Fprintf(tw, "Measurements:\t%d\n", f.MeasurementN())
	fmt.Fprintf(tw, "Tag Keys:\t%d\n", f.TagKeyN())
	fmt.Fprintf(tw, "Tag Values:\t%d\n", f.TagValueN())
	return tw.Flush()
}

func (a *args) printIndexFileSummary(f *tsi1.IndexFile) error {
	fmt.Fprintf(a.w, "[INDEX FILE] %s\n", filepath.Base(f.Path()))

	// Calculate summary stats.
	var measurementN, measurementSeriesN, measurementSeriesSize uint64
	var keyN uint64
	var valueN, valueSeriesN, valueSeriesSize uint64

	if mitr := f.MeasurementIterator(); mitr != nil {
		for me, _ := mitr.Next().(*tsi1.MeasurementBlockElem); me != nil; me, _ = mitr.Next().(*tsi1.MeasurementBlockElem) {
			kitr := f.TagKeyIterator(me.Name())
			for ke, _ := kitr.Next().(*tsi1.TagBlockKeyElem); ke != nil; ke, _ = kitr.Next().(*tsi1.TagBlockKeyElem) {
				vitr := f.TagValueIterator(me.Name(), ke.Key())
				for ve, _ := vitr.Next().(*tsi1.TagBlockValueElem); ve != nil; ve, _ = vitr.Next().(*tsi1.TagBlockValueElem) {
					valueN++
					valueSeriesN += ve.SeriesN()
					valueSeriesSize += uint64(len(ve.SeriesData()))
				}
				keyN++
			}
			measurementN++
			measurementSeriesN += me.SeriesN()
			measurementSeriesSize += uint64(len(me.SeriesData()))
		}
	}

	// Write stats.
	tw := tabwriter.NewWriter(a.w, 8, 8, 1, '\t', 0)
	fmt.Fprintf(tw, "Measurements:\t%d\n", measurementN)
	fmt.Fprintf(tw, "  Series data size:\t%d (%s)\n", measurementSeriesSize, formatSize(measurementSeriesSize))
	fmt.Fprintf(tw, "  Bytes per series:\t%.01fb\n", float64(measurementSeriesSize)/float64(measurementSeriesN))
	fmt.Fprintf(tw, "Tag Keys:\t%d\n", keyN)
	fmt.Fprintf(tw, "Tag Values:\t%d\n", valueN)
	fmt.Fprintf(tw, "  Series:\t%d\n", valueSeriesN)
	fmt.Fprintf(tw, "  Series data size:\t%d (%s)\n", valueSeriesSize, formatSize(valueSeriesSize))
	fmt.Fprintf(tw, "  Bytes per series:\t%.01fb\n", float64(valueSeriesSize)/float64(valueSeriesN))
	return tw.Flush()
}

// matchSeries returns true if the command filters matches the series.
func (a *args) matchSeries(name []byte, tags models.Tags) bool {
	// Filter by measurement.
	if a.measurementFilter != nil && !a.measurementFilter.Match(name) {
		return false
	}

	// Filter by tag key/value.
	if a.tagKeyFilter != nil || a.tagValueFilter != nil {
		var matched bool
		for _, tag := range tags {
			if (a.tagKeyFilter == nil || a.tagKeyFilter.Match(tag.Key)) && (a.tagValueFilter == nil || a.tagValueFilter.Match(tag.Value)) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	return true
}

// deletedString returns "(deleted)" if v is true.
func deletedString(v bool) string {
	if v {
		return "(deleted)"
	}
	return ""
}

func formatSize(v uint64) string {
	denom := uint64(1)
	var uom string
	for _, uom = range []string{"b", "kb", "mb", "gb", "tb"} {
		if denom*1024 > v {
			break
		}
		denom *= 1024
	}
	return fmt.Sprintf("%0.01f%s", float64(v)/float64(denom), uom)
}
