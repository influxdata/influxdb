package inspect

import (
	"github.com/spf13/cobra"
)

// NewCommand creates the new command.
func NewCommand() *cobra.Command {
	base := &cobra.Command{
		Use:   "inspect",
		Short: "Commands for inspecting on-disk database data",
	}

	// List of available sub-commands
	// If a new sub-command is created, it must be added here
	subCommands := []*cobra.Command{
		NewExportBlocksCommand(),
		NewReportTSMCommand(),
		NewVerifyTSMCommand(),
		NewVerifyWALCommand(),
<<<<<<< HEAD
		NewReportTSICommand(),
=======
		NewVerifySeriesFileCommand(),
>>>>>>> 26899d169... adds verify-seriesfile to 2.x
	}

	for _, command := range subCommands {
		base.AddCommand(command)
	}

	return base
}
