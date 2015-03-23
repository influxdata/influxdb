package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
)

// BackupSuffix is a suffix added to the backup while it's in-process.
const BackupSuffix = ".pending"

// BackupCommand represents the program execution for "influxd backup".
type BackupCommand struct {
	// The logger passed to the ticker during execution.
	Logger *log.Logger

	// Standard input/output, overridden for testing.
	Stderr io.Writer
}

// NewBackupCommand returns a new instance of BackupCommand with default settings.
func NewBackupCommand() *BackupCommand {
	return &BackupCommand{
		Stderr: os.Stderr,
	}
}

// Run excutes the program.
func (cmd *BackupCommand) Run(args ...string) error {
	// Set up logger.
	cmd.Logger = log.New(cmd.Stderr, "", log.LstdFlags)
	cmd.Logger.Printf("influxdb backup, version %s, commit %s", version, commit)

	// Parse command line arguments.
	u, path, err := cmd.parseFlags(args)
	if err != nil {
		return err
	}

	// TODO: Check highest index from local version.

	// Determine temporary path to download to.
	tmppath := path + BackupSuffix

	// Retrieve snapshot.
	if err := cmd.download(u, tmppath); err != nil {
		return fmt.Errorf("download: %s", err)
	}

	// Rename temporary file to final path.
	if err := os.Rename(tmppath, path); err != nil {
		return fmt.Errorf("rename: %s", err)
	}

	// TODO: Check file integrity.

	// Notify user of completion.
	cmd.Logger.Println("backup complete")

	return nil
}

// parseFlags parses and validates the command line arguments.
func (cmd *BackupCommand) parseFlags(args []string) (url.URL, string, error) {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	host := fs.String("host", DefaultSnapshotURL.String(), "")
	fs.SetOutput(cmd.Stderr)
	fs.Usage = cmd.printUsage
	if err := fs.Parse(args); err != nil {
		return url.URL{}, "", err
	}

	// Parse host.
	u, err := url.Parse(*host)
	if err != nil {
		return url.URL{}, "", fmt.Errorf("parse host url: %s", err)
	}

	// Require output path.
	path := fs.Arg(0)
	if path == "" {
		return url.URL{}, "", fmt.Errorf("snapshot path required")
	}

	return *u, path, nil
}

// download downloads a snapshot from a host to a given path.
func (cmd *BackupCommand) download(u url.URL, path string) error {
	// Create local file to write to.
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("open temp file: %s", err)
	}
	defer f.Close()

	// Fetch the archive from the server.
	u.Path = "/snapshot"
	resp, err := http.Get(u.String())
	if err != nil {
		return fmt.Errorf("get: %s", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Check the status code.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("snapshot error: status=%d", resp.StatusCode)
	}

	// Write the archive to disk.
	if _, err := io.Copy(f, resp.Body); err != nil {
		fmt.Errorf("write snapshot: %s", err)
	}

	return nil
}

// printUsage prints the usage message to STDERR.
func (cmd *BackupCommand) printUsage() {
	fmt.Fprintf(cmd.Stderr, `usage: influxd backup [flags] PATH

backup downloads a snapshot of a data node and saves it to disk.

        -host <url>
                          The host to connect to snapshot.
                          Defaults to 127.0.0.1:8087.
`)
}
