package inspect

import (
	"github.com/influxdata/influxdb/v2/cmd/influxd/inspect/delete_tsm"
	"github.com/influxdata/influxdb/v2/cmd/influxd/inspect/dump_tsi"
	"github.com/influxdata/influxdb/v2/cmd/influxd/inspect/dump_tsm"
	"github.com/influxdata/influxdb/v2/cmd/influxd/inspect/export_index"
	"github.com/influxdata/influxdb/v2/cmd/influxd/inspect/export_lp"
	"github.com/influxdata/influxdb/v2/cmd/influxd/inspect/verify_seriesfile"
	"github.com/influxdata/influxdb/v2/cmd/influxd/inspect/verify_tombstone"
	"github.com/influxdata/influxdb/v2/cmd/influxd/inspect/verify_tsm"
	"github.com/influxdata/influxdb/v2/cmd/influxd/inspect/dump_wal"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// NewCommand creates the new command.
func NewCommand(v *viper.Viper) (*cobra.Command, error) {
	base := &cobra.Command{
		Use:   "inspect",
		Short: "Commands for inspecting on-disk database data",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			cmd.PrintErrf("See '%s -h' for help\n", cmd.CommandPath())
		},
	}

	exportLp, err := export_lp.NewExportLineProtocolCommand(v)
	if err != nil {
		return nil, err
	}
	base.AddCommand(exportLp)
	base.AddCommand(export_index.NewExportIndexCommand())
	base.AddCommand(verify_tsm.NewTSMVerifyCommand())
	base.AddCommand(verify_seriesfile.NewVerifySeriesfileCommand())
	base.AddCommand(verify_tombstone.NewVerifyTombstoneCommand())
	base.AddCommand(dump_tsm.NewDumpTSMCommand())
	base.AddCommand(dump_tsi.NewDumpTSICommand())
	base.AddCommand(delete_tsm.NewDeleteTSMCommand())
	base.AddCommand(dump_wal.NewDumpWALCommand())

	return base, nil
}
