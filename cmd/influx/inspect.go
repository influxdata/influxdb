package main

import (
	"fmt"
	"path/filepath"

	"github.com/influxdata/influxdb/internal/fs"
	"github.com/spf13/cobra"
)

// Inspect Command
var inspectCmd = &cobra.Command{
	Use:   "inspect",
	Short: "commands for inspecting database file data",
	Run:   inspectF,
}

func inspectF(cmd *cobra.Command, args []string) {
	cmd.Usage()
}

// InspectReportTSMFlags defines the `report-tsm` Command.
type InspectReportTSMFlags struct {
	pattern  string
	exact    bool
	detailed bool

	orgName, bucketName string
	dataDir             string
}

var inspectReportTSMFlags InspectReportTSMFlags

func init() {
	inspectReportTSMCommand := &cobra.Command{
		Use:   "report-tsm",
		Short: "Run TSM report",
		Run:   inspectReportTSMF,
	}

	inspectReportTSMCommand.Flags().StringVarP(&inspectReportTSMFlags.pattern, "pattern", "", "", "only process TSM files containing pattern")
	inspectReportTSMCommand.Flags().BoolVarP(&inspectReportTSMFlags.exact, "exact", "", false, "calculate and exact cardinality count. Warning, may use significant memory...")
	inspectReportTSMCommand.Flags().BoolVarP(&inspectReportTSMFlags.detailed, "detailed", "", false, "emit series cardinality segmented by measurements, tag keys and fields. Warning, may take a while...")

	inspectReportTSMCommand.Flags().StringVarP(&inspectReportTSMFlags.orgName, "org", "o", "", "process only data belonging to organization")
	inspectReportTSMCommand.Flags().StringVarP(&inspectReportTSMFlags.bucketName, "bucket", "b", "", "process only data belonging to bucket. Requires org flag to be set.")

	dir, err := fs.InfluxDir()
	if err != nil {
		panic(err)
	}
	inspectReportTSMCommand.Flags().StringVarP(&inspectReportTSMFlags.dataDir, "data-dir", "", "", fmt.Sprintf("use provided data directory (defaults to %s).", filepath.Join(dir, "engine/data")))

	inspectCmd.AddCommand(inspectReportTSMCommand)
}

// inspectReportTSMF runs the report-tsm tool.
func inspectReportTSMF(cmd *cobra.Command, args []string) {}
