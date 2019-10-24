// Package dumptsi inspects low-level details about tsi1 files.
package dumptsi

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"text/tabwriter"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

// Command represents the program execution for "influxd dumptsi".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

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
	var measurementFilter, tagKeyFilter, tagValueFilter string
	fs := flag.NewFlagSet("dumptsi", flag.ExitOnError)
	fs.StringVar(&cmd.seriesFilePath, "series-file", "", "Path to series file")
	fs.BoolVar(&cmd.showSeries, "series", false, "Show raw series data")
	fs.BoolVar(&cmd.showMeasurements, "measurements", false, "Show raw measurement data")
	fs.BoolVar(&cmd.showTagKeys, "tag-keys", false, "Show raw tag key data")
	fs.BoolVar(&cmd.showTagValues, "tag-values", false, "Show raw tag value data")
	fs.BoolVar(&cmd.showTagValueSeries, "tag-value-series", false, "Show raw series data for each value")
	fs.StringVar(&measurementFilter, "measurement-filter", "", "Regex measurement filter")
	fs.StringVar(&tagKeyFilter, "tag-key-filter", "", "Regex tag key filter")
	fs.StringVar(&tagValueFilter, "tag-value-filter", "", "Regex tag value filter")
	fs.SetOutput(cmd.Stdout)
	fs.Usage = cmd.printUsage
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Parse filters.
	if measurementFilter != "" {
		re, err := regexp.Compile(measurementFilter)
		if err != nil {
			return err
		}
		cmd.measurementFilter = re
	}
	if tagKeyFilter != "" {
		re, err := regexp.Compile(tagKeyFilter)
		if err != nil {
			return err
		}
		cmd.tagKeyFilter = re
	}
	if tagValueFilter != "" {
		re, err := regexp.Compile(tagValueFilter)
		if err != nil {
			return err
		}
		cmd.tagValueFilter = re
	}

	// Validate series file path.
	if cmd.seriesFilePath == "" {
		return errors.New("series file path required")
	}

	cmd.paths = fs.Args()
	if len(cmd.paths) == 0 {
		fmt.Printf("at least one path required\n\n")
		fs.Usage()
		return nil
	}

	// Some flags imply other flags.
	if cmd.showTagValueSeries {
		cmd.showTagValues = true
	}
	if cmd.showTagValues {
		cmd.showTagKeys = true
	}
	if cmd.showTagKeys {
		cmd.showMeasurements = true
	}

	return cmd.run()
}

func (cmd *Command) run() error {
	sfile := tsdb.NewSeriesFile(cmd.seriesFilePath)
	sfile.Logger = logger.New(os.Stderr)
	if err := sfile.Open(); err != nil {
		return err
	}
	defer sfile.Close()

	// Build a file set from the paths on the command line.
	idx, fs, err := cmd.readFileSet(sfile)
	if err != nil {
		return err
	}

	if cmd.showSeries {
		if err := cmd.printSeries(sfile); err != nil {
			return err
		}
	}

	// If this is an ad-hoc fileset then process it and close afterward.
	if fs != nil {
		defer fs.Release()
		defer fs.Close()
		if cmd.showSeries || cmd.showMeasurements {
			return cmd.printMeasurements(sfile, fs)
		}
		return cmd.printFileSummaries(fs)
	}

	// Otherwise iterate over each partition in the index.
	defer idx.Close()
	for i := 0; i < int(idx.PartitionN); i++ {
		if err := func() error {
			fs, err := idx.PartitionAt(i).RetainFileSet()
			if err != nil {
				return err
			}
			defer fs.Release()

			if cmd.showSeries || cmd.showMeasurements {
				return cmd.printMeasurements(sfile, fs)
			}
			return cmd.printFileSummaries(fs)
		}(); err != nil {
			return err
		}
	}
	return nil
}

func (cmd *Command) readFileSet(sfile *tsdb.SeriesFile) (*tsi1.Index, *tsi1.FileSet, error) {
	// If only one path exists and it's a directory then open as an index.
	if len(cmd.paths) == 1 {
		fi, err := os.Stat(cmd.paths[0])
		if err != nil {
			return nil, nil, err
		} else if fi.IsDir() {
			// Verify directory is an index before opening it.
			if ok, err := tsi1.IsIndexDir(cmd.paths[0]); err != nil {
				return nil, nil, err
			} else if !ok {
				return nil, nil, fmt.Errorf("Not an index directory: %q", cmd.paths[0])
			}

			idx := tsi1.NewIndex(sfile,
				"",
				tsi1.WithPath(cmd.paths[0]),
				tsi1.DisableCompactions(),
			)
			if err := idx.Open(); err != nil {
				return nil, nil, err
			}
			return idx, nil, nil
		}
	}

	// Open each file and group into a fileset.
	var files []tsi1.File
	for _, path := range cmd.paths {
		switch ext := filepath.Ext(path); ext {
		case tsi1.LogFileExt:
			f := tsi1.NewLogFile(sfile, path)
			if err := f.Open(); err != nil {
				return nil, nil, err
			}
			files = append(files, f)

		case tsi1.IndexFileExt:
			f := tsi1.NewIndexFile(sfile)
			f.SetPath(path)
			if err := f.Open(); err != nil {
				return nil, nil, err
			}
			files = append(files, f)

		default:
			return nil, nil, fmt.Errorf("unexpected file extension: %s", ext)
		}
	}

	fs, err := tsi1.NewFileSet(nil, sfile, files)
	if err != nil {
		return nil, nil, err
	}
	fs.Retain()

	return nil, fs, nil
}

func (cmd *Command) printSeries(sfile *tsdb.SeriesFile) error {
	if !cmd.showSeries {
		return nil
	}

	// Print header.
	tw := tabwriter.NewWriter(cmd.Stdout, 8, 8, 1, '\t', 0)
	fmt.Fprintln(tw, "Series\t")

	// Iterate over each series.
	itr := sfile.SeriesIDIterator()
	for {
		e, err := itr.Next()
		if err != nil {
			return err
		} else if e.SeriesID == 0 {
			break
		}
		name, tags := tsdb.ParseSeriesKey(sfile.SeriesKey(e.SeriesID))

		if !cmd.matchSeries(name, tags) {
			continue
		}

		deleted := sfile.IsDeleted(e.SeriesID)

		fmt.Fprintf(tw, "%s%s\t%v\n", name, tags.HashKey(), deletedString(deleted))
	}

	// Flush & write footer spacing.
	if err := tw.Flush(); err != nil {
		return err
	}
	fmt.Fprint(cmd.Stdout, "\n\n")

	return nil
}

func (cmd *Command) printMeasurements(sfile *tsdb.SeriesFile, fs *tsi1.FileSet) error {
	if !cmd.showMeasurements {
		return nil
	}

	tw := tabwriter.NewWriter(cmd.Stdout, 8, 8, 1, '\t', 0)
	fmt.Fprintln(tw, "Measurement\t")

	// Iterate over each series.
	if itr := fs.MeasurementIterator(); itr != nil {
		for e := itr.Next(); e != nil; e = itr.Next() {
			if cmd.measurementFilter != nil && !cmd.measurementFilter.Match(e.Name()) {
				continue
			}

			fmt.Fprintf(tw, "%s\t%v\n", e.Name(), deletedString(e.Deleted()))
			if err := tw.Flush(); err != nil {
				return err
			}

			if err := cmd.printTagKeys(sfile, fs, e.Name()); err != nil {
				return err
			}
		}
	}

	fmt.Fprint(cmd.Stdout, "\n\n")

	return nil
}

func (cmd *Command) printTagKeys(sfile *tsdb.SeriesFile, fs *tsi1.FileSet, name []byte) error {
	if !cmd.showTagKeys {
		return nil
	}

	// Iterate over each key.
	tw := tabwriter.NewWriter(cmd.Stdout, 8, 8, 1, '\t', 0)
	itr := fs.TagKeyIterator(name)
	for e := itr.Next(); e != nil; e = itr.Next() {
		if cmd.tagKeyFilter != nil && !cmd.tagKeyFilter.Match(e.Key()) {
			continue
		}

		fmt.Fprintf(tw, "    %s\t%v\n", e.Key(), deletedString(e.Deleted()))
		if err := tw.Flush(); err != nil {
			return err
		}

		if err := cmd.printTagValues(sfile, fs, name, e.Key()); err != nil {
			return err
		}
	}
	fmt.Fprint(cmd.Stdout, "\n")

	return nil
}

func (cmd *Command) printTagValues(sfile *tsdb.SeriesFile, fs *tsi1.FileSet, name, key []byte) error {
	if !cmd.showTagValues {
		return nil
	}

	// Iterate over each value.
	tw := tabwriter.NewWriter(cmd.Stdout, 8, 8, 1, '\t', 0)
	itr := fs.TagValueIterator(name, key)
	for e := itr.Next(); e != nil; e = itr.Next() {
		if cmd.tagValueFilter != nil && !cmd.tagValueFilter.Match(e.Value()) {
			continue
		}

		fmt.Fprintf(tw, "        %s\t%v\n", e.Value(), deletedString(e.Deleted()))
		if err := tw.Flush(); err != nil {
			return err
		}

		if err := cmd.printTagValueSeries(sfile, fs, name, key, e.Value()); err != nil {
			return err
		}
	}
	fmt.Fprint(cmd.Stdout, "\n")

	return nil
}

func (cmd *Command) printTagValueSeries(sfile *tsdb.SeriesFile, fs *tsi1.FileSet, name, key, value []byte) error {
	if !cmd.showTagValueSeries {
		return nil
	}

	// Iterate over each series.
	tw := tabwriter.NewWriter(cmd.Stdout, 8, 8, 1, '\t', 0)
	itr, err := fs.TagValueSeriesIDIterator(name, key, value)
	if err != nil {
		return err
	}
	for {
		e, err := itr.Next()
		if err != nil {
			return err
		} else if e.SeriesID == 0 {
			break
		}

		name, tags := tsdb.ParseSeriesKey(sfile.SeriesKey(e.SeriesID))

		if !cmd.matchSeries(name, tags) {
			continue
		}

		fmt.Fprintf(tw, "            %s%s\n", name, tags.HashKey())
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	fmt.Fprint(cmd.Stdout, "\n")

	return nil
}

func (cmd *Command) printFileSummaries(fs *tsi1.FileSet) error {
	for _, f := range fs.Files() {
		switch f := f.(type) {
		case *tsi1.LogFile:
			if err := cmd.printLogFileSummary(f); err != nil {
				return err
			}
		case *tsi1.IndexFile:
			if err := cmd.printIndexFileSummary(f); err != nil {
				return err
			}
		default:
			panic("unreachable")
		}
		fmt.Fprintln(cmd.Stdout, "")
	}
	return nil
}

func (cmd *Command) printLogFileSummary(f *tsi1.LogFile) error {
	fmt.Fprintf(cmd.Stdout, "[LOG FILE] %s\n", filepath.Base(f.Path()))
	tw := tabwriter.NewWriter(cmd.Stdout, 8, 8, 1, '\t', 0)
	fmt.Fprintf(tw, "Series:\t%d\n", f.SeriesN())
	fmt.Fprintf(tw, "Measurements:\t%d\n", f.MeasurementN())
	fmt.Fprintf(tw, "Tag Keys:\t%d\n", f.TagKeyN())
	fmt.Fprintf(tw, "Tag Values:\t%d\n", f.TagValueN())
	return tw.Flush()
}

func (cmd *Command) printIndexFileSummary(f *tsi1.IndexFile) error {
	fmt.Fprintf(cmd.Stdout, "[INDEX FILE] %s\n", filepath.Base(f.Path()))

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
					valueSeriesN += uint64(ve.SeriesN())
					valueSeriesSize += uint64(len(ve.SeriesData()))
				}
				keyN++
			}
			measurementN++
			measurementSeriesN += uint64(me.SeriesN())
			measurementSeriesSize += uint64(len(me.SeriesData()))
		}
	}

	// Write stats.
	tw := tabwriter.NewWriter(cmd.Stdout, 8, 8, 1, '\t', 0)
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
func (cmd *Command) matchSeries(name []byte, tags models.Tags) bool {
	// Filter by measurement.
	if cmd.measurementFilter != nil && !cmd.measurementFilter.Match(name) {
		return false
	}

	// Filter by tag key/value.
	if cmd.tagKeyFilter != nil || cmd.tagValueFilter != nil {
		var matched bool
		for _, tag := range tags {
			if (cmd.tagKeyFilter == nil || cmd.tagKeyFilter.Match(tag.Key)) && (cmd.tagValueFilter == nil || cmd.tagValueFilter.Match(tag.Value)) {
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

// printUsage prints the usage message to STDERR.
func (cmd *Command) printUsage() {
	usage := `Dumps low-level details about tsi1 files.

Usage: influx_inspect dumptsi [flags] path...

    -series
            Dump raw series data
    -measurements
            Dump raw measurement data
    -tag-keys
            Dump raw tag keys
    -tag-values
            Dump raw tag values
    -tag-value-series
            Dump raw series for each tag value
    -measurement-filter REGEXP
            Filters data by measurement regular expression
    -series-file PATH
            Path to the "_series" directory under the database data directory.
            Required.
    -tag-key-filter REGEXP
            Filters data by tag key regular expression
    -tag-value-filter REGEXP
            Filters data by tag value regular expression

One or more paths are required. Path must specify either a TSI index directory
or it should specify one or more .tsi/.tsl files. If no flags are specified
then summary stats are provided for each file.
`

	fmt.Fprintf(cmd.Stdout, usage)
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
