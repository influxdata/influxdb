package verify

import (
	"github.com/spf13/cobra"
)

// NewCommand creates the new command.
func NewCommand() *cobra.Command {
	base := &cobra.Command{
		Use:   "verify",
		Short: "Commands for verifying on-disk database data",
	}

	// List of available sub-commands
	// If a new sub-command is created, it must be added here
	subCommands := []*cobra.Command{
		newTSMBlocksCommand(),
	}

	for _, command := range subCommands {
		base.AddCommand(command)
	}

	return base
}
