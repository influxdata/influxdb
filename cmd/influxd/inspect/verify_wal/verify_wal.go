package verify_wal

import (
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
)

type args struct {
	dir     string
	verbose bool
}

func NewVerifyWALCommand() *cobra.Command {
	var arguments args
	cmd := &cobra.Command{
		Use:   `verify-wal`,
		Short: "Check for WAL corruption",
		Long: `
This command will analyze the WAL (Write-Ahead Log) in a storage directory to 
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
	* A list of files found to be corrupt`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return arguments.Run(cmd)
		},
	}

	dir, err := fs.InfluxDir()
	if err != nil {
		panic(err)
	}
	dir = filepath.Join(dir, "engine/wal")
	cmd.Flags().StringVar(&arguments.dir, "wal-dir", dir, "use provided WAL directory.")
	cmd.Flags().BoolVarP(&arguments.verbose, "verbose", "v", false, "enable verbose logging")
	return cmd
}

func (a args) Run(cmd *cobra.Command) error {
	// Verify valid directory
	fi, err := os.Stat(a.dir)
	if err != nil {
		return fmt.Errorf("failed to stat %q: %w", a.dir, err)
	} else if !fi.IsDir() {
		return fmt.Errorf("%q is not a directory", a.dir)
	}

	// Find all WAL files in provided directory
	files, err := loadFiles(a.dir)
	if err != nil {
		return fmt.Errorf("failed to search for WAL files in directory %s: %w", a.dir, err)
	}
	if len(files) == 0 {
		return fmt.Errorf("no WAL files found in directory %s", a.dir)
	}

	start := time.Now()
	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 8, 2, 1, ' ', 0)

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
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "%s: corrupt entry found at position %d\n", fpath, reader.Count())
				corruptFiles = append(corruptFiles, fpath)
				break
			}
		}

		if entriesScanned == 0 {
			// No data found in file
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "no WAL entries found for file %s, skipping", f.Name())
		} else if clean && a.verbose {
			// No corrupted entry found
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "%s: clean\n", fpath)
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
