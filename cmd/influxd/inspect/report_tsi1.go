package inspect

import (
	"errors"
	"io"
	"os"
	"path"

	"github.com/influxdata/influxdb"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/tsdb/tsi1"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"
)

// Command represents the program execution for "influxd inspect report-tsi".
var reportTSIFlags = struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	// Data path options
	Path           string // optional. Defaults to dbPath/engine/index
	SeriesFilePath string // optional. Defaults to dbPath/_series

	// Tenant filtering options
	Org    string
	Bucket string

	// Reporting options
	TopN          int
	ByMeasurement bool
	byTagKey      bool // currently unused
}{}

// NewReportTsiCommand returns a new instance of Command with default setting applied.
func NewReportTSICommand() *cobra.Command {
	reportTSICommand := &cobra.Command{
		Use:   "report-tsi",
		Short: "Reports the cardinality of tsi files short",
		Long:  `Reports the cardinality of tsi files long.`,
		RunE:  RunReportTSI,
	}

	reportTSICommand.Flags().StringVar(&reportTSIFlags.Path, "path", os.Getenv("HOME")+"/.influxdbv2/engine", "Path to data engine. Defaults $HOME/.influxdbv2/engine")
	reportTSICommand.Flags().StringVar(&reportTSIFlags.SeriesFilePath, "series-file", "", "Optional path to series file. Defaults /path/to/db-path/_series")
	reportTSICommand.Flags().BoolVarP(&reportTSIFlags.ByMeasurement, "measurements", "m", false, "Segment cardinality by measurements")
	// fs.BoolVar(&cmd.byTagKey, "tag-key", false, "Segment cardinality by tag keys (overrides `measurements`")
	reportTSICommand.Flags().IntVarP(&reportTSIFlags.TopN, "top", "t", 0, "Limit results to top n")
	reportTSICommand.Flags().StringVarP(&reportTSIFlags.Bucket, "bucket", "b", "", "If bucket is specified, org must be specified")
	reportTSICommand.Flags().StringVarP(&reportTSIFlags.Org, "org", "o", "", "Org to be reported")

	reportTSICommand.SetOutput(reportTSIFlags.Stdout)

	return reportTSICommand
}

// RunReportTSI executes the run command for ReportTSI.
func RunReportTSI(cmd *cobra.Command, args []string) error {
	// set up log
	config := logger.NewConfig()
	config.Level = zapcore.InfoLevel
	log, err := config.New(os.Stderr)
	if err != nil {
		return err
	}

	// if path is unset, set to $HOME/.influxdbv2/engine"
	if reportTSIFlags.Path == "" {
		reportTSIFlags.Path = path.Join(os.Getenv("HOME"), ".influxdbv2/engine")
	}

	report := tsi1.NewReportCommand()
	report.DataPath = reportTSIFlags.Path
	report.Logger = log
	report.ByMeasurement = reportTSIFlags.ByMeasurement
	report.TopN = reportTSIFlags.TopN

	if reportTSIFlags.Org != "" {
		if orgID, err := influxdb.IDFromString(reportTSIFlags.Org); err != nil {
			return err
		} else {
			report.OrgID = orgID
		}
	}

	if reportTSIFlags.Bucket != "" {
		if bucketID, err := influxdb.IDFromString(reportTSIFlags.Bucket); err != nil {
			return err
		} else if report.OrgID == nil {
			return errors.New("org must be provided if filtering by bucket")
		} else {
			report.BucketID = bucketID
		}
	}

	// Run command with printing enabled
	_, err = report.Run(true)
	if err != nil {
		return err
	}
	return nil
}
