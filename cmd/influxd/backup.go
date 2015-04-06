package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/influxdb/influxdb"
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

	// Retrieve snapshot from local file.
	ss, err := influxdb.ReadFileSnapshot(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read file snapshot: %s", err)
	}

	// Determine temporary path to download to.
	tmppath := path + BackupSuffix

	// Calculate path of next backup file.
	// This uses the path if it doesn't exist.
	// Otherwise it appends an autoincrementing number.
	path, err = cmd.nextPath(path)
	if err != nil {
		return fmt.Errorf("next path: %s", err)
	}

	// Retrieve snapshot.
	if err := cmd.download(u, ss, tmppath); err != nil {
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

// nextPath returns the next file to write to.
func (cmd *BackupCommand) nextPath(path string) (string, error) {
	// Use base path if it doesn't exist.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return path, nil
	} else if err != nil {
		return "", err
	}

	// Otherwise iterate through incremental files until one is available.
	for i := 0; ; i++ {
		s := fmt.Sprintf(path+".%d", i)
		if _, err := os.Stat(s); os.IsNotExist(err) {
			return s, nil
		} else if err != nil {
			return "", err
		}
	}
}

// download downloads a snapshot from a host to a given path.
func (cmd *BackupCommand) download(u url.URL, ss *influxdb.Snapshot, path string) error {
	// Create local file to write to.
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("open temp file: %s", err)
	}
	defer f.Close()

	// Encode snapshot.
	var buf bytes.Buffer
	if ss != nil {
		if err := json.NewEncoder(&buf).Encode(ss); err != nil {
			return fmt.Errorf("encode snapshot: %s", err)
		}
	}

	// Create request with existing snapshot as the body.
	u.Path = "/snapshot"
	req, err := http.NewRequest("GET", u.String(), &buf)
	if err != nil {
		return fmt.Errorf("new request: %s", err)
	}

	// Fetch the archive from the server.
	resp, err := http.DefaultClient.Do(req)
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
		return fmt.Errorf("write snapshot: %s", err)
	}

	return nil
}

// printUsage prints the usage message to STDERR.
func (cmd *BackupCommand) printUsage() {
	fmt.Fprintf(cmd.Stderr, `usage: influxd backup [flags] PATH

backup downloads a snapshot of a data node and saves it to disk.

        -host <url>
                          The host to connect to snapshot.
                          Defaults to http://127.0.0.1:8087.
`)
}
