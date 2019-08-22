// inspects low-level details about tsi1 files.
package inspect

import (
	"errors"
	"io"
	"path/filepath"
	"regexp"

	"github.com/influxdata/influxdb/internal/fs"
	"github.com/influxdata/influxdb/tsdb/tsi1"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// Command represents the program execution for "influxd dumptsi".
var measurementFilter, tagKeyFilter, tagValueFilter string
var dumpTsiFlags = struct {
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
func NewDumpTsiCommand() *cobra.Command {
	dumpTsiCommand := &cobra.Command{
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

	dumpTsiCommand.Flags().StringVar(&dumpTsiFlags.seriesFilePath, "series-path", defaultSeriesDir, "Path to series file")
	dumpTsiCommand.Flags().StringVar(&dumpTsiFlags.dataPath, "index-path", defaultIndexDir, "Path to the index directory of the data engine")
	dumpTsiCommand.Flags().BoolVar(&dumpTsiFlags.showSeries, "series", false, "Show raw series data")
	dumpTsiCommand.Flags().BoolVar(&dumpTsiFlags.showMeasurements, "measurements", false, "Show raw measurement data")
	dumpTsiCommand.Flags().BoolVar(&dumpTsiFlags.showTagKeys, "tag-keys", false, "Show raw tag key data")
	dumpTsiCommand.Flags().BoolVar(&dumpTsiFlags.showTagValues, "tag-values", false, "Show raw tag value data")
	dumpTsiCommand.Flags().BoolVar(&dumpTsiFlags.showTagValueSeries, "tag-value-series", false, "Show raw series data for each value")
	dumpTsiCommand.Flags().StringVar(&measurementFilter, "measurement-filter", "", "Regex measurement filter")
	dumpTsiCommand.Flags().StringVar(&tagKeyFilter, "tag-key-filter", "", "Regex tag key filter")
	dumpTsiCommand.Flags().StringVar(&tagValueFilter, "tag-value-filter", "", "Regex tag value filter")

	return dumpTsiCommand
}

func dumpTsi(cmd *cobra.Command, args []string) error {
	logger := zap.NewNop()

	// Parse filters.
	if measurementFilter != "" {
		re, err := regexp.Compile(measurementFilter)
		if err != nil {
			return err
		}
		dumpTsiFlags.measurementFilter = re
	}
	if tagKeyFilter != "" {
		re, err := regexp.Compile(tagKeyFilter)
		if err != nil {
			return err
		}
		dumpTsiFlags.tagKeyFilter = re
	}
	if tagValueFilter != "" {
		re, err := regexp.Compile(tagValueFilter)
		if err != nil {
			return err
		}
		dumpTsiFlags.tagValueFilter = re
	}

	if dumpTsiFlags.dataPath == "" {
		return errors.New("data path must be specified")
	}

	// Some flags imply other flags.
	if dumpTsiFlags.showTagValueSeries {
		dumpTsiFlags.showTagValues = true
	}
	if dumpTsiFlags.showTagValues {
		dumpTsiFlags.showTagKeys = true
	}
	if dumpTsiFlags.showTagKeys {
		dumpTsiFlags.showMeasurements = true
	}

	dump := tsi1.NewDumpTsi(logger, dumpTsiFlags.seriesFilePath, dumpTsiFlags.dataPath,
		dumpTsiFlags.showSeries, dumpTsiFlags.showMeasurements, dumpTsiFlags.showTagKeys,
		dumpTsiFlags.showTagValues, dumpTsiFlags.showTagValueSeries, dumpTsiFlags.measurementFilter,
		dumpTsiFlags.tagKeyFilter, dumpTsiFlags.tagValueFilter)

	return dump.Run()
}
