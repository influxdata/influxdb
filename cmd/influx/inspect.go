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

// InspectReportTSMFlags defines the `report-tsm` Command.
type InspectReportTSMFlags struct {
	pattern  string
	exact    bool
	detailed bool

	orgID, bucketID string
	dataDir         string
}

var inspectReportTSMFlags InspectReportTSMFlags

func initInspectReportTSMCommand() *cobra.Command {
	inspectReportTSMCommand := &cobra.Command{
		Use:   "report-tsm",
		Short: "Run TSM report",
		RunE:  inspectReportTSMF,
	}

	inspectReportTSMCommand.Flags().StringVarP(&inspectReportTSMFlags.pattern, "pattern", "", "", "only process TSM files containing pattern")
	inspectReportTSMCommand.Flags().BoolVarP(&inspectReportTSMFlags.exact, "exact", "", false, "calculate and exact cardinality count. Warning, may use significant memory...")
	inspectReportTSMCommand.Flags().BoolVarP(&inspectReportTSMFlags.detailed, "detailed", "", false, "emit series cardinality segmented by measurements, tag keys and fields. Warning, may take a while...")

	inspectReportTSMCommand.Flags().StringVarP(&inspectReportTSMFlags.orgID, "org-id", "", "", "process only data belonging to organization ID.")
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
	report := &tsm1.Report{
		Stderr:   os.Stderr,
		Stdout:   os.Stdout,
		Dir:      inspectReportTSMFlags.dataDir,
		Pattern:  inspectReportTSMFlags.pattern,
		Detailed: inspectReportTSMFlags.detailed,
		Exact:    inspectReportTSMFlags.exact,
	}

	if inspectReportTSMFlags.orgID == "" && inspectReportTSMFlags.bucketID != "" {
		return errors.New("org-id must be set for non-empty bucket-id")
	}

	if inspectReportTSMFlags.orgID != "" {
		orgID, err := influxdb.IDFromString(inspectReportTSMFlags.orgID)
		if err != nil {
			return err
		}
		report.OrgID = orgID
	}

	if inspectReportTSMFlags.bucketID != "" {
		bucketID, err := influxdb.IDFromString(inspectReportTSMFlags.bucketID)
		if err != nil {
			return err
		}
		report.BucketID = bucketID
	}

	err := report.Run()
	if err != nil {
		panic(err)
	}
	return err
}
