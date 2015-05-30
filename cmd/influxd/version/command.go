package version

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

// These variables are populated via the Go linker.
var (
	version string = "0.9"
	commit  string
)

func init() {
	// If commit not set, make that clear.
	if commit == "" {
		commit = "unknown"
	}
}

// Command represents the command executed by "influxd version".
type Command struct {
	Stdout io.Writer
	Stderr io.Writer
}

// NewCommand return a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run prints the current version and commit info.
func (cmd *Command) Run(args ...string) error {
	// Parse flags in case -h is specified.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, strings.TrimSpace(usage)) }
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Print version info.
	fmt.Fprintf(cmd.Stdout, "InfluxDB v%s (git: %s)\n", version, commit)

	return nil
}

var usage = `
usage: version

	version displays the InfluxDB version and build git commit hash
`
