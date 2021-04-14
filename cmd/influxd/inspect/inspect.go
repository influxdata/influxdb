package inspect

import (
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

	exportLp, err := NewExportLineProtocolCommand(v)
	if err != nil {
		return nil, err
	}
	base.AddCommand(exportLp)
	base.AddCommand(NewExportIndexCommand())

	return base, nil
}
