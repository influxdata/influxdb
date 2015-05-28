package main

import "fmt"

// HelpCommand displays help for command-line sub-commands.
type HelpCommand struct {
}

// NewHelpCommand returns a new instance of HelpCommand.
func NewHelpCommand() *HelpCommand {
	return &HelpCommand{}
}

// Run executes the command.
func (cmd *HelpCommand) Run(args ...string) error {
	fmt.Println(`
Configure and start an InfluxDB server.

Usage:

	influxd [[command] [arguments]]

The commands are:

    config               display the default configuration
    join-cluster         create a new node that will join an existing cluster
    run                  run node with existing configuration
    version              displays the InfluxDB version

"run" is the default command.

Use "influxd help [command]" for more information about a command.
`)
	return nil
}
