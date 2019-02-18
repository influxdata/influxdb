package main

import (
	"github.com/spf13/cobra"
)

// Debug Command
var debugCmd = &cobra.Command{
	Use:   "debug",
	Short: "commands for debugging InfluxDB",
}

func debugF(cmd *cobra.Command, args []string) {
	cmd.Usage()
}

func init() {
	debugCmd.AddCommand(initInspectReportTSMCommand()) // Add report-tsm command
}
