// Package help contains the help for the influx_inspect command.
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
Usage: influx_inspect [[command] [arguments]]

The commands are:

    deletetsm            bulk measurement deletion of raw tsm file
    dumptsi              dumps low-level details about tsi1 files
    dumptsm              dumps low-level details about tsm1 files
    export               exports raw data from a shard to line protocol
    buildtsi             generates tsi1 indexes from tsm1 data
    help                 display this help message
    report               displays a shard level report
    verify               verifies integrity of TSM files
    verify-seriesfile    verifies integrity of the Series file

"help" is the default command.

Use "influx_inspect [command] -help" for more information about a command.
`
