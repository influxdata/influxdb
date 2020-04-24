package inspect

import (
	"fmt"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/kit/errors"
	"github.com/influxdata/influxdb/v2/v1/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
)

// reportTSMFlags defines the `report-tsm` Command.
var reportTSMFlags = struct {
	pattern  string
	exact    bool
	detailed bool

	orgID, bucketID string
	dataDir         string
}{}

func NewReportTSMCommand() *cobra.Command {

	reportTSMCommand := &cobra.Command{
		Use:   "report-tsm",
		Short: "Run TSM report",
		Long: `
This command will analyze TSM files within a storage engine directory, reporting 
the cardinality within the files as well as the time range that the point data 
covers.

This command only interrogates the index within each file, and does not read any
block data. To reduce heap requirements, by default report-tsm estimates the 
overall cardinality in the file set by using the HLL++ algorithm. Exact 
cardinalities can be determined by using the --exact flag.

For each file, the following is output:

	* The full filename;
	* The series cardinality within the file;
	* The number of series first encountered within the file;
	* The min and max timestamp associated with TSM data in the file; and
	* The time taken to load the TSM index and apply any tombstones.

The summary section then outputs the total time range and series cardinality for 
the fileset. Depending on the --detailed flag, series cardinality is segmented 
in the following ways:

	* Series cardinality for each organization;
	* Series cardinality for each bucket;
	* Series cardinality for each measurement;
	* Number of field keys for each measurement; and
	* Number of tag values for each tag key.`,
		RunE: inspectReportTSMF,
	}

	reportTSMCommand.Flags().StringVarP(&reportTSMFlags.pattern, "pattern", "", "", "only process TSM files containing pattern")
	reportTSMCommand.Flags().BoolVarP(&reportTSMFlags.exact, "exact", "", false, "calculate and exact cardinality count. Warning, may use significant memory...")
	reportTSMCommand.Flags().BoolVarP(&reportTSMFlags.detailed, "detailed", "", false, "emit series cardinality segmented by measurements, tag keys and fields. Warning, may take a while.")

	reportTSMCommand.Flags().StringVarP(&reportTSMFlags.orgID, "org-id", "", "", "process only data belonging to organization ID.")
	reportTSMCommand.Flags().StringVarP(&reportTSMFlags.bucketID, "bucket-id", "", "", "process only data belonging to bucket ID. Requires org flag to be set.")

	dir, err := fs.InfluxDir()
	if err != nil {
		panic(err)
	}
	dir = filepath.Join(dir, "engine/data")
	reportTSMCommand.Flags().StringVarP(&reportTSMFlags.dataDir, "data-dir", "", dir, fmt.Sprintf("use provided data directory (defaults to %s).", dir))

	return reportTSMCommand
}

// inspectReportTSMF runs the report-tsm tool.
func inspectReportTSMF(cmd *cobra.Command, args []string) error {
	report := &tsm1.Report{
		Stderr:   os.Stderr,
		Stdout:   os.Stdout,
		Dir:      reportTSMFlags.dataDir,
		Pattern:  reportTSMFlags.pattern,
		Detailed: reportTSMFlags.detailed,
		Exact:    reportTSMFlags.exact,
	}

	if reportTSMFlags.orgID == "" && reportTSMFlags.bucketID != "" {
		return errors.New("org-id must be set for non-empty bucket-id")
	}

	if reportTSMFlags.orgID != "" {
		orgID, err := influxdb.IDFromString(reportTSMFlags.orgID)
		if err != nil {
			return err
		}
		report.OrgID = orgID
	}

	if reportTSMFlags.bucketID != "" {
		bucketID, err := influxdb.IDFromString(reportTSMFlags.bucketID)
		if err != nil {
			return err
		}
		report.BucketID = bucketID
	}

	_, err := report.Run(true)
	return err
}
