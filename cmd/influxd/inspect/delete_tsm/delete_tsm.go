package delete_tsm

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
)

type args struct {
	measurement string // measurement to delete
	sanitize    bool   // remove all keys with non-printable unicode
	verbose     bool   // verbose logging
}

func NewDeleteTSMCommand() *cobra.Command {
	var arguments args
	cmd := &cobra.Command{
		Use:   "delete-tsm",
		Short: "Deletes a measurement from a raw tsm file.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Validate measurement or sanitize flag.
			if arguments.measurement == "" && !arguments.sanitize {
				return fmt.Errorf("--measurement or --sanitize flag required")
			}

			// Process each TSM file.
			for _, path := range args {
				if arguments.verbose {
					cmd.Printf("processing: %s", path)
				}
				if err := arguments.process(cmd, path); err != nil {
					return err
				}
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&arguments.measurement, "measurement", "",
		"The name of the measurement to remove")
	cmd.Flags().BoolVar(&arguments.sanitize, "sanitize", false,
		"Remove all keys with non-printable unicode characters")
	cmd.Flags().BoolVarP(&arguments.verbose, "verbose", "v", false,
		"Enable verbose logging")

	return cmd
}

func (a *args) process(cmd *cobra.Command, path string) error {
	// Open TSM reader.
	input, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file %q: %w", path, err)
	}
	defer input.Close()

	// Check if path is a directory
	fi, err := input.Stat()
	if err != nil {
		return fmt.Errorf("failed to read FileInfo of file %s: %w", path, err)
	}
	if fi.IsDir() {
		return fmt.Errorf("%s is a directory", path)
	}

	// Check if file is a TSM file
	if filepath.Ext(path) != "."+tsm1.TSMFileExtension {
		return fmt.Errorf("%s is not a TSM file", path)
	}

	r, err := tsm1.NewTSMReader(input)
	if err != nil {
		return fmt.Errorf("unable to read TSM file %q: %w", path, err)
	}
	defer r.Close()

	// Remove previous temporary files.
	outputPath := path + ".rewriting.tmp"
	if err := os.RemoveAll(outputPath); err != nil {
		return fmt.Errorf("failed to remove existing temp file at %q: %w", outputPath, err)
	} else if err := os.RemoveAll(outputPath + ".idx.tmp"); err != nil {
		return fmt.Errorf("failed to remove existing temp file at %q: %w", outputPath+".idx.tmp", err)
	}

	// Create TSMWriter to temporary location.
	output, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create temporary file at %q: %w", outputPath, err)
	}
	defer output.Close()

	w, err := tsm1.NewTSMWriter(output)
	if err != nil {
		return fmt.Errorf("failed to create TSM Reader for file %q: %w", output.Name(), err)
	}
	defer w.Close()

	// Iterate over the input blocks.
	hasData := false
	itr := r.BlockIterator()
	for itr.Next() {
		// Read key & time range.
		key, minTime, maxTime, _, _, block, err := itr.Read()
		if err != nil {
			return fmt.Errorf("failed to read block: %w", err)
		}

		// Skip block if this is the measurement and time range we are deleting.
		series, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
		measurement, tags := models.ParseKey(series)
		if measurement == a.measurement || (a.sanitize && !models.ValidKeyTokens(measurement, tags)) {
			if a.verbose {
				cmd.Printf("deleting block: %s (%s-%s) sz=%d",
					key,
					time.Unix(0, minTime).UTC().Format(time.RFC3339Nano),
					time.Unix(0, maxTime).UTC().Format(time.RFC3339Nano),
					len(block),
				)
			}
			continue
		}

		if err := w.WriteBlock(key, minTime, maxTime, block); err != nil {
			return fmt.Errorf("failed to write block %q: %w", block, err)
		}
		hasData = true
	}

	// Write index & close.
	if hasData {
		if err := w.WriteIndex(); err != nil {
			return fmt.Errorf("failed to write index to TSM file: %w", err)
		}
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close TSM Writer: %w", err)
	}

	// Replace original file with new file.
	if err := os.Rename(outputPath, path); err != nil {
		return fmt.Errorf("failed to update TSM file %q: %w", path, err)
	}
	if !hasData {
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("failed to remove empty TSM file %q: %w", path, err)
		}
	}
	return nil
}
