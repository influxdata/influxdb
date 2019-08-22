package tsi1

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"text/tabwriter"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

// Command represents the program execution for "influxd dumptsi".
type DumpTsi struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	Logger *zap.Logger

	// Optional: defaults to DataPath/_series
	seriesFilePath string

	// root dir of the engine
	DataPath string

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
func NewDumpTsi(logger *zap.Logger, seriesPath string, dataPath string, showSeries, showMeasurements, showTagKeys, showTagValues, showTagValueSeries bool, measurementFilter, tagKeyFilter, tagValueFilter *regexp.Regexp) DumpTsi {
	dump := DumpTsi{
		Logger:             logger,
		seriesFilePath:     seriesPath,
		DataPath:           dataPath,
		showSeries:         showSeries,
		showMeasurements:   showMeasurements,
		showTagKeys:        showTagKeys,
		showTagValues:      showTagValues,
		showTagValueSeries: showTagValueSeries,
		measurementFilter:  measurementFilter,
		tagKeyFilter:       tagKeyFilter,
		tagValueFilter:     tagValueFilter,
		Stderr:             os.Stderr,
		Stdout:             os.Stdout,
	}
	return dump
}

// Run executes the command.
func (cmd *DumpTsi) Run() error {
	sfile := tsdb.NewSeriesFile(cmd.seriesFilePath)
	sfile.Logger = cmd.Logger
	if err := sfile.Open(context.Background()); err != nil {
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
		// defer fs.Close()
		if cmd.showSeries || cmd.showMeasurements {
			return cmd.printMeasurements(sfile, fs)
		}
		return cmd.printFileSummaries(fs)
	}

	// Otherwise iterate over each partition in the index.
	defer idx.Close()
	for i := 0; i < int(idx.PartitionN); i++ {
		if err := func() error {
			// original
			// fs, err := idx.PartitionAt(i).RetainFileSet()
			// may need to call Acquire for each file
			fs := idx.PartitionAt(i).fileSet
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

func (cmd *DumpTsi) readFileSet(sfile *tsdb.SeriesFile) (*Index, *FileSet, error) {

	index := NewIndex(sfile, NewConfig(), WithPath(cmd.DataPath), DisableCompactions())

	if err := index.Open(context.Background()); err != nil {
		return nil, nil, err
	}
	return index, nil, nil

	// // If only one path exists and it's a directory then open as an index.
	// if len(cmd.paths) == 1 {
	// 	fi, err := os.Stat(cmd.paths[0])
	// 	if err != nil {
	// 		return nil, nil, err
	// 	} else if fi.IsDir() {
	// 		// Verify directory is an index before opening it.
	// 		if ok, err := IsIndexDir(cmd.paths[0]); err != nil {
	// 			return nil, nil, err
	// 		} else if !ok {
	// 			return nil, nil, fmt.Errorf("Not an index directory: %q", cmd.paths[0])
	// 		}

	// 		idx := NewIndex(sfile,
	// 			NewConfig(),
	// 			WithPath(cmd.paths[0]),
	// 			DisableCompactions(),
	// 		)
	// 		if err := idx.Open(context.Background()); err != nil {
	// 			return nil, nil, err
	// 		}
	// 		return idx, nil, nil
	// 	}
	// }

	// TODO: use this logic if we want to support entering custom file paths
	// Open each file and group into a fileset.
	// var files []File
	// for _, path := range cmd.paths {
	// 	switch ext := filepath.Ext(path); ext {
	// 	case LogFileExt:
	// 		f := NewLogFile(sfile, path)
	// 		if err := f.Open(); err != nil {
	// 			return nil, nil, err
	// 		}
	// 		files = append(files, f)

	// 	case IndexFileExt:
	// 		f := NewIndexFile(sfile)
	// 		f.SetPath(path)
	// 		if err := f.Open(); err != nil {
	// 			return nil, nil, err
	// 		}
	// 		files = append(files, f)

	// 	default:
	// 		return nil, nil, fmt.Errorf("unexpected file extension: %s", ext)
	// 	}
	// }

	// fs, err := NewFileSet(sfile, files)
	// if err != nil {
	// 	return nil, nil, err
	// }
	// return nil, fs, nil
}

func (cmd *DumpTsi) printSeries(sfile *tsdb.SeriesFile) error {
	if !cmd.showSeries {
		return nil
	}

	// Print header.
	tw := tabwriter.NewWriter(cmd.Stdout, 8, 8, 1, '\t', 0)
	fmt.Fprintln(tw, "Series\t")

	// Iterate over each series.
	fmt.Printf("series file path: %v\n", sfile.Path())
	itr := sfile.SeriesIDIterator()
	for {
		e, err := itr.Next()
		if err != nil {
			return err
		} else if e.SeriesID.ID == 0 {
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

func (cmd *DumpTsi) printMeasurements(sfile *tsdb.SeriesFile, fs *FileSet) error {
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

func (cmd *DumpTsi) printTagKeys(sfile *tsdb.SeriesFile, fs *FileSet, name []byte) error {
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

func (cmd *DumpTsi) printTagValues(sfile *tsdb.SeriesFile, fs *FileSet, name, key []byte) error {
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

func (cmd *DumpTsi) printTagValueSeries(sfile *tsdb.SeriesFile, fs *FileSet, name, key, value []byte) error {
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
		} else if e.SeriesID.ID == 0 {
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

func (cmd *DumpTsi) printFileSummaries(fs *FileSet) error {
	for _, f := range fs.Files() {
		switch f := f.(type) {
		case *LogFile:
			fmt.Printf("got an alleged LogFile: %v\n", f.Path())
			if err := cmd.printLogFileSummary(f); err != nil {
				return err
			}
		case *IndexFile:
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

func (cmd *DumpTsi) printLogFileSummary(f *LogFile) error {
	fmt.Fprintf(cmd.Stdout, "[LOG FILE] %s\n", filepath.Base(f.Path()))
	tw := tabwriter.NewWriter(cmd.Stdout, 8, 8, 1, '\t', 0)
	fmt.Fprintf(tw, "Series:\t%d\n", f.SeriesN())
	fmt.Fprintf(tw, "Measurements:\t%d\n", f.MeasurementN())
	fmt.Fprintf(tw, "Tag Keys:\t%d\n", f.TagKeyN())
	fmt.Fprintf(tw, "Tag Values:\t%d\n", f.TagValueN())
	return tw.Flush()
}

func (cmd *DumpTsi) printIndexFileSummary(f *IndexFile) error {
	fmt.Fprintf(cmd.Stdout, "[INDEX FILE] %s\n", filepath.Base(f.Path()))

	// Calculate summary stats.
	var measurementN, measurementSeriesN, measurementSeriesSize uint64
	var keyN uint64
	var valueN, valueSeriesN, valueSeriesSize uint64

	if mitr := f.MeasurementIterator(); mitr != nil {
		for me, _ := mitr.Next().(*MeasurementBlockElem); me != nil; me, _ = mitr.Next().(*MeasurementBlockElem) {
			kitr := f.TagKeyIterator(me.Name())
			for ke, _ := kitr.Next().(*TagBlockKeyElem); ke != nil; ke, _ = kitr.Next().(*TagBlockKeyElem) {
				vitr := f.TagValueIterator(me.Name(), ke.Key())
				for ve, _ := vitr.Next().(*TagBlockValueElem); ve != nil; ve, _ = vitr.Next().(*TagBlockValueElem) {
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
func (cmd *DumpTsi) matchSeries(name []byte, tags models.Tags) bool {
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
