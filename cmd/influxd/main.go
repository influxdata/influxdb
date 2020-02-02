// Command influxd is the InfluxDB server.
package main

import (
	"context"
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
	rand.Seed(time.Now().UnixNano())

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

func (m *Main) run(args ...string) error {
	cmd := run.NewCommand()
	// Tell the server the build details.
	cmd.Version = version
	cmd.Commit = commit
	cmd.Branch = branch

	// set up signal handler
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	cmd.Logger.Info("Listening for signals")

	// start server in a new go routine
	cmd.Logger.Info("Starting services")
	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { errCh <- cmd.Run(ctx, args...) }()

	// wait for message on errCh or signalCh
	select {
	case err := <-errCh:
		// if we've received a value on our errCh at this point, then the
		// server has stopped and we should exit.
		return err

	case <-signalCh:
		// if we received a signal, lets cancel our services.
		cmd.Logger.Info("Signal received, initializing clean shutdown...")
		cancel()

		// we store the deadline duration so that we can print it later.
		deadline := time.Second * 30
		t := time.NewTicker(deadline)
		defer t.Stop() // clean up ticker when done

		select {
		case err := <-errCh:
			// cmd.Run() has completed.
			return err
		case <-signalCh:
			// we got another signal before cmd.Run() has gracefully shut down.
			return fmt.Errorf("got another signal; forcibly shutting down")
		case <-t.C:
			// timeout expired before we cleanly shutdown
			return fmt.Errorf("server failed to shutdown after %v", deadline)
		}
	}

	return nil
}

// Run determines and runs the command specified by the CLI args.
func (m *Main) Run(args ...string) error {
	name, args := cmd.ParseCommandName(args)

	// Extract name from args.
	switch name {
	case "", "run":
		if err := m.run(args...); err != nil {
			return fmt.Errorf("run: %s", err)
		}

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
	fmt.Fprintf(cmd.Stdout, "InfluxDB v%s (git: %s %s)\n", version, branch, commit)

	return nil
}

var versionUsage = `Displays the InfluxDB version, build branch and git commit hash.

Usage: influxd version
`
