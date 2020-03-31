package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/internal/fs"
	"github.com/influxdata/influxdb/tsdb/tsm1"
	"github.com/spf13/cobra"
)

var _ = debugCmd

func debugCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "debug",
		Short: "commands for debugging InfluxDB",
	}
	cmd.AddCommand(initInspectReportTSMCommand()) // Add report-tsm command

	return cmd
}

var inspectReportTSMFlags struct {
	pattern  string
	exact    bool
	detailed bool
	organization
	bucketID string
	dataDir  string
}

func initInspectReportTSMCommand() *cobra.Command {
	inspectReportTSMCommand := &cobra.Command{
		Use:   "report-tsm",
		Short: "Run a TSM report",
		Long: `This command will analyze TSM files within a storage engine
directory, reporting the cardinality within the files as well as the time range that 
the point data covers.

This command only interrogates the index within each file, and does not read any
block data. To reduce heap requirements, by default report-tsm estimates the overall
cardinality in the file set by using the HLL++ algorithm. Exact cardinalities can
be determined by using the --exact flag.

For each file, the following is output:

	* The full filename;
	* The series cardinality within the file;
	* The number of series first encountered within the file;
	* The minimum and maximum timestamp associated with any TSM data in the file; and
	* The time taken to load the TSM index and apply any tombstones.

The summary section then outputs the total time range and series cardinality for 
the fileset. Depending on the --detailed flag, series cardinality is segmented 
in the following ways:

	* Series cardinality for each organization;
	* Series cardinality for each bucket;
	* Series cardinality for each measurement;
	* Number of field keys for each measurement; and
	* Number of tag values for each tag key.
`,
		RunE: inspectReportTSMF,
	}

	inspectReportTSMCommand.Flags().StringVarP(&inspectReportTSMFlags.pattern, "pattern", "", "", "only process TSM files containing pattern")
	inspectReportTSMCommand.Flags().BoolVarP(&inspectReportTSMFlags.exact, "exact", "", false, "calculate and exact cardinality count. Warning, may use significant memory...")
	inspectReportTSMCommand.Flags().BoolVarP(&inspectReportTSMFlags.detailed, "detailed", "", false, "emit series cardinality segmented by measurements, tag keys and fields. Warning, may take a while.")

	inspectReportTSMFlags.organization.register(inspectReportTSMCommand, false)
	inspectReportTSMCommand.Flags().StringVarP(&inspectReportTSMFlags.bucketID, "bucket-id", "", "", "process only data belonging to bucket ID. Requires org flag to be set.")

	dir, err := fs.InfluxDir()
	if err != nil {
		panic(err)
	}
	inspectReportTSMCommand.Flags().StringVarP(&inspectReportTSMFlags.dataDir, "data-dir", "", "", fmt.Sprintf("use provided data directory (defaults to %s).", filepath.Join(dir, "engine/data")))
	return inspectReportTSMCommand
}

// inspectReportTSMF runs the report-tsm tool.
func inspectReportTSMF(cmd *cobra.Command, args []string) error {
	if err := inspectReportTSMFlags.organization.validOrgFlags(&flags); err != nil {
		return err
	}
	report := &tsm1.Report{
		Stderr:   os.Stderr,
		Stdout:   os.Stdout,
		Dir:      inspectReportTSMFlags.dataDir,
		Pattern:  inspectReportTSMFlags.pattern,
		Detailed: inspectReportTSMFlags.detailed,
		Exact:    inspectReportTSMFlags.exact,
	}

	if (inspectReportTSMFlags.organization.name == "" || inspectReportTSMFlags.organization.id == "") && inspectReportTSMFlags.bucketID != "" {
		return errors.New("org-id must be set for non-empty bucket-id")
	}

	orgSvc, err := newOrganizationService()
	if err != nil {
		return err
	}
	id, err := inspectReportTSMFlags.organization.getID(orgSvc)
	if err != nil {
		return err
	}
	report.OrgID = &id

	if inspectReportTSMFlags.bucketID != "" {
		bucketID, err := influxdb.IDFromString(inspectReportTSMFlags.bucketID)
		if err != nil {
			return err
		}
		report.BucketID = bucketID
	}

	_, err = report.Run(true)
	if err != nil {
		panic(err)
	}
	return err
}
