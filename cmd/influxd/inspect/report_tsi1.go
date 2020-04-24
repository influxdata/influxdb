package inspect

import (
	"errors"
	"io"
	"os"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/v1/tsdb/index/tsi1"
	"github.com/spf13/cobra"
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
	cmd := &cobra.Command{
		Use:   "report-tsi",
		Short: "Reports the cardinality of TSI files",
		Long: `This command will analyze TSI files within a storage engine directory, reporting 
		the cardinality of data within the files, divided into org and bucket cardinalities.
		
		For each report, the following is output:
		
			* All orgs and buckets in the index;
			* The series cardinality within each org and each bucket;
			* The time taken to read the index.
		
		Depending on the --measurements flag, series cardinality is segmented 
		in the following ways:
		
			* Series cardinality for each organization;
			* Series cardinality for each bucket;
			* Series cardinality for each measurement;`,
		RunE: RunReportTSI,
	}

	cmd.Flags().StringVar(&reportTSIFlags.Path, "path", os.Getenv("HOME")+"/.influxdbv2/engine/index", "Path to index. Defaults $HOME/.influxdbv2/engine/index")
	cmd.Flags().StringVar(&reportTSIFlags.SeriesFilePath, "series-file", os.Getenv("HOME")+"/.influxdbv2/engine/_series", "Optional path to series file. Defaults $HOME/.influxdbv2/engine/_series")
	cmd.Flags().BoolVarP(&reportTSIFlags.ByMeasurement, "measurements", "m", false, "Segment cardinality by measurements")
	cmd.Flags().IntVarP(&reportTSIFlags.TopN, "top", "t", 0, "Limit results to top n")
	cmd.Flags().StringVarP(&reportTSIFlags.Bucket, "bucket_id", "b", "", "If bucket is specified, org must be specified. A bucket id must be a base-16 string")
	cmd.Flags().StringVarP(&reportTSIFlags.Org, "org_id", "o", "", "Only specified org data will be reported. An org id must be a base-16 string")

	cmd.SetOutput(reportTSIFlags.Stdout)

	return cmd
}

// RunReportTSI executes the run command for ReportTSI.
func RunReportTSI(cmd *cobra.Command, args []string) error {
	report := tsi1.NewReportCommand()
	report.DataPath = reportTSIFlags.Path
	report.ByMeasurement = reportTSIFlags.ByMeasurement
	report.TopN = reportTSIFlags.TopN
	report.SeriesDirPath = reportTSIFlags.SeriesFilePath

	report.Stdout = os.Stdout
	report.Stderr = os.Stderr

	var err error
	if reportTSIFlags.Org != "" {
		if report.OrgID, err = influxdb.IDFromString(reportTSIFlags.Org); err != nil {
			return err
		}
	}

	if reportTSIFlags.Bucket != "" {
		if report.BucketID, err = influxdb.IDFromString(reportTSIFlags.Bucket); err != nil {
			return err
		} else if report.OrgID == nil {
			return errors.New("org must be provided if filtering by bucket")
		}
	}

	// Run command with printing enabled
	if _, err = report.Run(true); err != nil {
		return err
	}
	return nil
}
