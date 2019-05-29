package inspect

import (
	"github.com/influxdata/influxdb"
	"github.com/spf13/cobra"
)

// NewCommand creates the new command.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Commands for inspecting on-disk database data",
	}
	cmd.AddCommand(newReportTSMCommand())
	cmd.AddCommand(newVerifyTSMCommand())
	return cmd
}

// idFromStringIfExists parses s into a string if non-blank. Otherwise return 0.
func idFromStringIfExists(s string) (*influxdb.ID, error) {
	if s == "" {
		return nil, nil
	}
	return influxdb.IDFromString(s)
}
