package inspect

import (
	"fmt"
	"path/filepath"

	"github.com/influxdata/influxdb/internal/fs"
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
		Run:   nil,
	}

	var reportTSMFlags = &reportTSMFlags{}
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
