// Command influxd is the InfluxDB server.
package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/influxdata/influxdb/cmd"
	"github.com/influxdata/influxdb/cmd/influxd/backup"
	"github.com/influxdata/influxdb/cmd/influxd/help"
	"github.com/influxdata/influxdb/cmd/influxd/restore"
	"github.com/influxdata/influxdb/cmd/influxd/run"
)

// These variables are populated via the Go linker.
var (
	version string
	commit  string
	branch  string
)

func init() {
	// If commit, branch, or build time are not set, make that clear.
	if version == "" {
		version = "unknown"
	}
	if commit == "" {
		commit = "unknown"
	}
	if branch == "" {
		branch = "unknown"
	}
}

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	m := NewMain()
	if err := m.Run(os.Args[1:]...); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Main represents the program execution.
type Main struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewMain return a new instance of Main.
func NewMain() *Main {
	return &Main{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run determines and runs the command specified by the CLI args.
func (m *Main) Run(args ...string) error {
	name, args := cmd.ParseCommandName(args)

	// Extract name from args.
	switch name {
	case "", "run":
		cmd := run.NewCommand()

		// Tell the server the build details.
		cmd.Version = version
		cmd.Commit = commit
		cmd.Branch = branch

		if err := cmd.Run(args...); err != nil {
			return fmt.Errorf("run: %s", err)
		}

		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
		cmd.Logger.Info("Listening for signals")

		// Block until one of the signals above is received
		sig := <-signalCh
		cmd.Logger.Info("Signal received, initializing clean shutdown...")
		if sig == syscall.SIGTERM && cmd.Server.LogQueriesOnTermination() {
			cmd.Server.QueryExecutor.TaskManager.LogCurrentQueries(cmd.Logger.Info)
		}
		go cmd.Close()

		// Block again until another signal is received, a shutdown timeout elapses,
		// or the Command is gracefully closed
		cmd.Logger.Info("Waiting for clean shutdown...")
		select {
		case <-signalCh:
			cmd.Logger.Info("Second signal received, initializing hard shutdown")
		case <-time.After(time.Second * 30):
			cmd.Logger.Info("Time limit reached, initializing hard shutdown")
		case <-cmd.Closed:
			cmd.Logger.Info("Server shutdown completed")
		}

		// goodbye.

	case "backup":
		name := backup.NewCommand()
		if err := name.Run(args...); err != nil {
			return fmt.Errorf("backup: %s", err)
		}
	case "restore":
		name := restore.NewCommand()
		if err := name.Run(args...); err != nil {
			return fmt.Errorf("restore: %s", err)
		}
	case "config":
		if err := run.NewPrintConfigCommand().Run(args...); err != nil {
			return fmt.Errorf("config: %s", err)
		}
	case "version":
		if err := NewVersionCommand().Run(args...); err != nil {
			return fmt.Errorf("version: %s", err)
		}
	case "help":
		if err := help.NewCommand().Run(args...); err != nil {
			return fmt.Errorf("help: %s", err)
		}
	default:
		return fmt.Errorf(`unknown command "%s"`+"\n"+`Run 'influxd help' for usage`+"\n\n", name)
	}

	return nil
}

// VersionCommand represents the command executed by "influxd version".
type VersionCommand struct {
	Stdout io.Writer
	Stderr io.Writer
}

// NewVersionCommand return a new instance of VersionCommand.
func NewVersionCommand() *VersionCommand {
	return &VersionCommand{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run prints the current version and commit info.
func (cmd *VersionCommand) Run(args ...string) error {
	// Parse flags in case -h is specified.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, versionUsage) }
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Print version info.
	fmt.Fprintf(cmd.Stdout, "InfluxDB %s (git: %s %s)\n", version, branch, commit)

	return nil
}

var versionUsage = `Displays the InfluxDB version, build branch and git commit hash.

Usage: influxd version
`
