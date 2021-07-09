package verify_wal

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
)

type args struct {
	dir string
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
	cmd.Flags().StringVarP(&arguments.dir, "data-dir", "", dir, fmt.Sprintf("use provided data directory (defaults to %s).", dir))
	return cmd
}

func (a args) Run(cmd *cobra.Command) error {
	// Verify valid directory
	dir, err := os.Stat(a.dir)
	if err != nil {
		return fmt.Errorf("failed to get directory from %s", a.dir)
	} else if !dir.IsDir() {
		return errors.New("invalid data directory")
	}

	// Find all WAL files in provided directory
	files, err := filepath.Glob(path.Join(a.dir, "*."+tsm1.WALFileExtension))
	if err != nil {
		return fmt.Errorf("failed to find WAL files in directory %s: %w", a.dir, err)
	}

	start := time.Now()
	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 8, 2, 1, ' ', 0)

	var corruptFiles []string
	var entriesScanned int

	// Scan each WAL file
	for _, fpath := range files {
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
				_, _ = fmt.Fprintf(tw, "%s: corrupt entry found at position %d\n", fpath, reader.Count())
				corruptFiles = append(corruptFiles, fpath)
				break
			}

		}

		// No corrupted entry found
		if clean {
			_, _ = fmt.Fprintf(tw, "%s: clean\n", fpath)
		}
	}

	// Print Summary
	_, _ = fmt.Fprintf(tw, "Results:\n")
	_, _ = fmt.Fprintf(tw, "  Files checked: %d\n", len(files))
	_, _ = fmt.Fprintf(tw, "  Total entries checked: %d\n", entriesScanned)
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
