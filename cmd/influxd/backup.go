package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
)

// BackupSuffix is a suffix added to the backup while it's in-process.
const BackupSuffix = ".pending"

func Backup(host, path string) {
	log.Printf("influxdb backup, version %s, commit %s", version, commit)

	// Parse host and generate URL.
	u, err := url.Parse(host)
	if err != nil {
		log.Fatalf("host url parse error: %s", err)
	}

	// TODO: Check highest index from local version.

	// Create local file to write to.
	tmppath := path + BackupSuffix
	f, err := os.Create(tmppath)
	if err != nil {
		log.Fatalf("create temp file: %s", err)
	}
	defer f.Close()

	// Download snapshot to temp location.
	if err := downloadBackup(*u, f); err != nil {
		log.Fatalf("download backup: %s", err)
	}

	// Rename the archive to its final location.
	f.Close()
	if err := os.Rename(tmppath, path); err != nil {
		log.Fatalf("rename: %s", err)
	}

	// Notify user of completion.
	log.Print("backup complete")
}

// downloadBackup downloads a snapshot from a host to a given path.
func downloadBackup(u url.URL, f *os.File) error {
	// Fetch the archive from the server.
	u.Path = "/backup"
	resp, err := http.Get(u.String())
	if err != nil {
		return fmt.Errorf("get backup: %s", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Check the status code.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("backup error: status=%d", resp.StatusCode)
	}

	// Write the archive to disk.
	if _, err := io.Copy(f, resp.Body); err != nil {
		fmt.Errorf("write backup: %s", err)
	}

	return nil
}

func printBackupUsage() {
	log.Printf(`usage: influxd backup [flags]

backup downloads a snapshot of a data node and saves it to disk.

        -host <url>
                          The host to connect to snapshot.

        -output <path>
                          Path to where the snapshot will be saved.
`)
}
