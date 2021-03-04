package main

import "github.com/spf13/cobra"

func cmdV1SubCommands(f *globalFlags, opt genericCLIOpts) (*cobra.Command, error) {
	cmd := opt.newCmd("v1", nil, false)
	cmd.Short = "InfluxDB v1 management commands"
	cmd.Run = seeHelp

	builders := []func(*globalFlags, genericCLIOpts) (*cobra.Command, error){cmdV1Auth, cmdV1DBRP}
	for _, builder := range builders {
		subcmd, err := builder(f, opt)
		if err != nil {
			return nil, err
		}
		cmd.AddCommand(subcmd)
	}

	return cmd, nil
}
