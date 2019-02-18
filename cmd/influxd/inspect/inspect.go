package inspect

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

// NewCommand creates the new command.
func NewCommand() *cobra.Command {
	base := &cobra.Command{
		Use:   "inspect",
		Short: "Commands for inspecting on-disk database data",
	}

	reportTSMCommand := &cobra.Command{
		Use:   "report-tsm",
		Short: "Run TSM report",
		Run:   inspectReportTSMF,
	}

	reportTSMCommand.Flags().StringVarP(&reportTSMFlags.pattern, "pattern", "", "", "only process TSM files containing pattern")
	reportTSMCommand.Flags().BoolVarP(&reportTSMFlags.exact, "exact", "", false, "calculate and exact cardinality count. Warning, may use significant memory...")
	reportTSMCommand.Flags().BoolVarP(&reportTSMFlags.detailed, "detailed", "", false, "emit series cardinality segmented by measurements, tag keys and fields. Warning, may take a while...")

	reportTSMCommand.Flags().StringVarP(&reportTSMFlags.orgName, "org", "o", "", "process only data belonging to organization")
	reportTSMCommand.Flags().StringVarP(&reportTSMFlags.bucketName, "bucket", "b", "", "process only data belonging to bucket. Requires org flag to be set.")

	dir, err := fs.InfluxDir()
	if err != nil {
		panic(err)
	}
	reportTSMCommand.Flags().StringVarP(&reportTSMFlags.dataDir, "data-dir", "", "", fmt.Sprintf("use provided data directory (defaults to %s).", filepath.Join(dir, "engine/data")))

	base.AddCommand(reportTSMCommand)
	return base
}

// reportTSMFlags defines the `report-tsm` Command.
type reportTSMFlags struct {
	pattern  string
	exact    bool
	detailed bool

	orgName, bucketName string
	dataDir             string
}

var reportTSMFlags = &reportTSMFlags{}

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

	err := report.Run()
	if err != nil {
		panic(err)
	}
	return err
}
