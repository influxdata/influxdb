package delete_tsm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/influxdata/influxdb/v2/models"
	errors2 "github.com/influxdata/influxdb/v2/pkg/errors"
	"github.com/influxdata/influxdb/v2/pkg/file"
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

func (a *args) process(cmd *cobra.Command, path string) (retErr error) {
	// Remove previous temporary files.
	outputPath := path + ".rewriting.tmp"

	// Open TSM reader.
	input, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file %q: %w", path, err)
	}

	// Check if path is a directory
	fi, err := input.Stat()
	if err != nil {
		_ = input.Close()
		return fmt.Errorf("failed to read FileInfo of file %s: %w", path, err)
	}
	if fi.IsDir() {
		_ = input.Close()
		return fmt.Errorf("%s is a directory", path)
	}

	// Check if file is a TSM file
	if filepath.Ext(path) != "."+tsm1.TSMFileExtension {
		_ = input.Close()
		return fmt.Errorf("%s is not a TSM file", path)
	}

	r, err := tsm1.NewTSMReader(input)
	if err != nil {
		// close the input file on error creating the TSMReader
		_ = input.Close()
		return fmt.Errorf("unable to read TSM file %q: %w", path, err)
	}

	// Nested function to ensure all deferred close operations happen before final deletion or rename
	size, err := func() (size uint32, fRetErr error) {
		// This will close the input file
		defer errors2.Capture(&retErr, r.Close)()

		if err := os.RemoveAll(outputPath); err != nil {
			return 0, fmt.Errorf("failed to remove existing temp file at %q: %w", outputPath, err)
		} else if err := os.RemoveAll(outputPath + ".idx.tmp"); err != nil {
			return 0, fmt.Errorf("failed to remove existing temp file at %q: %w", outputPath+".idx.tmp", err)
		}

		// Create TSMWriter to temporary location.
		output, err := os.Create(outputPath)
		if err != nil {
			return 0, fmt.Errorf("failed to create temporary file at %q: %w", outputPath, err)
		}

		w, err := tsm1.NewTSMWriter(output)
		if err != nil {
			// close the output file on error creating the TSMWriter
			_ = output.Close()
			return 0, fmt.Errorf("failed to create TSM Writer for file %q: %w", output.Name(), err)
		}

		// This will close the output file
		defer errors2.Capture(&fRetErr, w.Close)()

		// Iterate over the input blocks.
		hasData := false
		itr := r.BlockIterator()
		for itr.Next() {
			// Read key & time range.
			key, minTime, maxTime, _, _, block, err := itr.Read()
			if err != nil {
				return 0, fmt.Errorf("failed to read block: %w", err)
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
				return 0, fmt.Errorf("failed to write block: %w", err)
			}
			hasData = true
		}

		// Write index & close.
		// It is okay to have no index values if no block was written
		if err := w.WriteIndex(); err != nil && !(hasData || errors.Is(err, tsm1.ErrNoValues)) {
			return 0, fmt.Errorf("failed to write index to TSM file: %w", err)
		}
		return w.Size(), nil
	}()
	if err != nil {
		return err
	}

	if size > 0 {
		// Replace original file with new file.
		if err := file.RenameFile(outputPath, path); err != nil {
			return fmt.Errorf("failed to update TSM file %q: %w", path, err)
		}
	} else {
		// Empty TSM file, remove both original and temp
		if err = os.RemoveAll(path); err != nil {
			err = fmt.Errorf("failed to remove empty TSM file %q: %w", path, err)
		}
		if err2 := os.RemoveAll(outputPath); err2 != nil && err == nil {
			return fmt.Errorf("failed to remove temporary file %q: %w", outputPath, err2)
		}
		return err
	}
	return nil
}
