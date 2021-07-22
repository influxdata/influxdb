package main

import "github.com/spf13/cobra"

func cmdV1SubCommands(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := opt.newCmd("v1", nil, false)
	cmd.Short = "InfluxDB v1 management commands"
	cmd.Run = seeHelp

	cmd.AddCommand(
		cmdV1Auth(f, opt),
		cmdV1DBRP(f, opt),
	)

	return cmd
}
