package recovery

import (
	"github.com/influxdata/influxdb/v2/cmd/influxd/recovery/auth"
	"github.com/influxdata/influxdb/v2/cmd/influxd/recovery/organization"
	"github.com/influxdata/influxdb/v2/cmd/influxd/recovery/user"
	"github.com/spf13/cobra"
)

// NewCommand creates the new command.
func NewCommand() *cobra.Command {
	base := &cobra.Command{
		Use:   "recovery-cli",
		Short: "Subset of commands similar to influx cli but usable on a halted local database without authentication",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			cmd.PrintErrf("See '%s -h' for help\n", cmd.CommandPath())
		},
	}

	base.AddCommand(auth.NewAuthCommand())
	base.AddCommand(user.NewUserCommand())
	base.AddCommand(organization.NewOrgCommand())

	return base
}
