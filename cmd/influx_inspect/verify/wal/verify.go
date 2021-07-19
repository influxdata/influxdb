package wal

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// Command represents the program execution for "influx_inspect verify-wal".
type Command struct {
	Stderr io.Writer
	Stdout io.Writer
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

func (cmd *Command) Run(args ...string) error {
	var path string
	var v bool
	fs := flag.NewFlagSet("verify-wal", flag.ExitOnError)
	fs.StringVar(&path, "wal-dir", os.Getenv("HOME")+"/.influxdb", "Root storage path. [$HOME/.influxdb]")
	fs.BoolVar(&v, "verbose", false, "Enable verbose logging")

	fs.SetOutput(cmd.Stdout)
	fs.Usage = cmd.printUsage

	if err := fs.Parse(args); err != nil {
		return err
	}

	walPath := filepath.Join(path, "wal")
	tw := tabwriter.NewWriter(cmd.Stdout, 16, 8, 0, '\t', 0)
	err := cmd.verifyWAL(tw, walPath, v)
	tw.Flush()
	return err
}

func (cmd *Command) verifyWAL(tw *tabwriter.Writer, path string, v bool) error {
	// Verify valid directory
	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat %q: %w", path, err)
	} else if !fi.IsDir() {
		return fmt.Errorf("%q is not a directory", path)
	}

	// Find all WAL files in provided directory
	files, err := loadFiles(path)
	if err != nil {
		return fmt.Errorf("failed to search for WAL files in directory %s: %w", path, err)
	}
	if len(files) == 0 {
		return fmt.Errorf("no WAL files found in directory %s", path)
	}

	start := time.Now()
	var corruptFiles []string
	var totalEntriesScanned int

	// Scan each WAL file
	for _, fpath := range files {
		var entriesScanned int
		f, err := os.OpenFile(fpath, os.O_RDONLY, 0600)
		if err != nil {
			return fmt.Errorf("error opening file %s: %w. Exiting", fpath, err)
		}

		clean := true
		reader := tsm1.NewWALSegmentReader(f)

		// Check for corrupted entries
		for reader.Next() {
			entriesScanned++
			_, err := reader.Read()
			if err != nil {
				clean = false
				_, _ = fmt.Fprintf(cmd.Stderr, "%s: corrupt entry found at position %d\n", fpath, reader.Count())
				corruptFiles = append(corruptFiles, fpath)
				break
			}
		}

		if v {
			if entriesScanned == 0 {
				// No data found in file
				_, _ = fmt.Fprintf(cmd.Stderr, "%s: no WAL entries found\n", f.Name())
			} else if clean {
				// No corrupted entry found
				_, _ = fmt.Fprintf(cmd.Stderr, "%s: clean\n", fpath)
			}
		}
		totalEntriesScanned += entriesScanned
		_ = tw.Flush()
	}

	// Print Summary
	_, _ = fmt.Fprintf(tw, "Results:\n")
	_, _ = fmt.Fprintf(tw, "  Files checked: %d\n", len(files))
	_, _ = fmt.Fprintf(tw, "  Total entries checked: %d\n", totalEntriesScanned)
	_, _ = fmt.Fprintf(tw, "  Corrupt files found: ")
	if len(corruptFiles) == 0 {
		_, _ = fmt.Fprintf(tw, "None")
	} else {
		for _, name := range corruptFiles {
			_, _ = fmt.Fprintf(tw, "\n    %s", name)
		}
	}

	_, _ = fmt.Fprintf(tw, "\nCompleted in %v\n", time.Since(start))
	_ = tw.Flush()

	return nil
}

func loadFiles(dir string) (files []string, err error) {
	err = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == "."+tsm1.WALFileExtension {
			files = append(files, path)
		}
		return nil
	})
	return
}

type walParams struct {
	dir     string
	verbose bool
}

func (cmd *Command) printUsage() {
	usage := fmt.Sprintf(`This command will analyze the WAL (Write-Ahead Log) in a storage directory to 
 check if there are any corrupt files. If any corrupt files are found, the names
 of said corrupt files will be reported. The tool will also count the total number
 of entries in the scanned WAL files, in case this is of interest.
 For each file, the following is output:
 	* The file name;
 	* "clean" (if the file is clean) OR 
       The first position of any corruption that is found
 In the summary section, the following is printed:
	* The number of WAL files scanned;
 	* The number of WAL entries scanned;
 	* A list of files found to be corrupt

Usage: influx_inspect verify-wal [flags]

	-wal-dir <path>
		The root WAL storage path.
		Must be changed if you are using a non-default storage directory.
            Defaults to "%[1]s/.influxdb".

	-verbose
		Enable verbose logging
`, os.Getenv("HOME"))

	fmt.Fprintf(cmd.Stdout, usage)
}
