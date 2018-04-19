// Package help is the help subcommand of the influxd command.
package help

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// Command displays help for command-line sub-commands.
type Command struct {
	Stdout io.Writer
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stdout: os.Stdout,
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	fmt.Fprintln(cmd.Stdout, strings.TrimSpace(usage))
	return nil
}

const usage = `
Tools for managing and querying InfluxDB data.

Usage: influx-tools command [arguments]

The commands are:

    export               downloads a snapshot of a data node and saves it to disk
    help                 display this help message

Use "influx-tools command -help" for more information about a command.
`
