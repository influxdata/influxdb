// inspects low-level details about tsi1 files.
package inspect

import (
	"errors"
	"io"
	"path/filepath"
	"regexp"

	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/v1/tsdb/index/tsi1"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// Command represents the program execution for "influxd dumptsi".
var measurementFilter, tagKeyFilter, tagValueFilter string
var dumpTSIFlags = struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	seriesFilePath string
	dataPath       string

	showSeries         bool
	showMeasurements   bool
	showTagKeys        bool
	showTagValues      bool
	showTagValueSeries bool

	measurementFilter *regexp.Regexp
	tagKeyFilter      *regexp.Regexp
	tagValueFilter    *regexp.Regexp
}{}

// NewCommand returns a new instance of Command.
func NewDumpTSICommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dump-tsi",
		Short: "Dump low level tsi information",
		Long: `Dumps low-level details about tsi1 files.

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
			-series-path PATH
					Path to the "_series" directory under the database data directory.
			-index-path PATH
					Path to the "index" directory under the database data directory.
			-tag-key-filter REGEXP
					Filters data by tag key regular expression
			-tag-value-filter REGEXP
					Filters data by tag value regular expression
		`,
		RunE: dumpTsi,
	}
	defaultDataDir, _ := fs.InfluxDir()
	defaultDataDir = filepath.Join(defaultDataDir, "engine")
	defaultIndexDir := filepath.Join(defaultDataDir, "index")
	defaultSeriesDir := filepath.Join(defaultDataDir, "_series")

	cmd.Flags().StringVar(&dumpTSIFlags.seriesFilePath, "series-path", defaultSeriesDir, "Path to series file")
	cmd.Flags().StringVar(&dumpTSIFlags.dataPath, "index-path", defaultIndexDir, "Path to the index directory of the data engine")
	cmd.Flags().BoolVar(&dumpTSIFlags.showSeries, "series", false, "Show raw series data")
	cmd.Flags().BoolVar(&dumpTSIFlags.showMeasurements, "measurements", false, "Show raw measurement data")
	cmd.Flags().BoolVar(&dumpTSIFlags.showTagKeys, "tag-keys", false, "Show raw tag key data")
	cmd.Flags().BoolVar(&dumpTSIFlags.showTagValues, "tag-values", false, "Show raw tag value data")
	cmd.Flags().BoolVar(&dumpTSIFlags.showTagValueSeries, "tag-value-series", false, "Show raw series data for each value")
	cmd.Flags().StringVar(&measurementFilter, "measurement-filter", "", "Regex measurement filter")
	cmd.Flags().StringVar(&tagKeyFilter, "tag-key-filter", "", "Regex tag key filter")
	cmd.Flags().StringVar(&tagValueFilter, "tag-value-filter", "", "Regex tag value filter")

	return cmd
}

func dumpTsi(cmd *cobra.Command, args []string) error {
	logger := zap.NewNop()

	// Parse filters.
	if measurementFilter != "" {
		re, err := regexp.Compile(measurementFilter)
		if err != nil {
			return err
		}
		dumpTSIFlags.measurementFilter = re
	}
	if tagKeyFilter != "" {
		re, err := regexp.Compile(tagKeyFilter)
		if err != nil {
			return err
		}
		dumpTSIFlags.tagKeyFilter = re
	}
	if tagValueFilter != "" {
		re, err := regexp.Compile(tagValueFilter)
		if err != nil {
			return err
		}
		dumpTSIFlags.tagValueFilter = re
	}

	if dumpTSIFlags.dataPath == "" {
		return errors.New("data path must be specified")
	}

	// Some flags imply other flags.
	if dumpTSIFlags.showTagValueSeries {
		dumpTSIFlags.showTagValues = true
	}
	if dumpTSIFlags.showTagValues {
		dumpTSIFlags.showTagKeys = true
	}
	if dumpTSIFlags.showTagKeys {
		dumpTSIFlags.showMeasurements = true
	}

	dump := tsi1.NewDumpTSI(logger)
	dump.SeriesFilePath = dumpTSIFlags.seriesFilePath
	dump.DataPath = dumpTSIFlags.dataPath
	dump.ShowSeries = dumpTSIFlags.showSeries
	dump.ShowMeasurements = dumpTSIFlags.showMeasurements
	dump.ShowTagKeys = dumpTSIFlags.showTagKeys
	dump.ShowTagValueSeries = dumpTSIFlags.showTagValueSeries
	dump.MeasurementFilter = dumpTSIFlags.measurementFilter
	dump.TagKeyFilter = dumpTSIFlags.tagKeyFilter
	dump.TagValueFilter = dumpTSIFlags.tagValueFilter

	return dump.Run()
}
