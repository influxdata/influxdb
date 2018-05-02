// Package deletetsm bulk deletes a measurement from a raw tsm file.
package deletetsm

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// Command represents the program execution for "influxd deletetsm".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	measurement string // measurement to delete
	sanitize    bool   // remove all keys with non-printable unicode
	verbose     bool   // verbose logging
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) (err error) {
	fs := flag.NewFlagSet("deletetsm", flag.ExitOnError)
	fs.StringVar(&cmd.measurement, "measurement", "", "")
	fs.BoolVar(&cmd.sanitize, "sanitize", false, "")
	fs.BoolVar(&cmd.verbose, "v", false, "")
	fs.SetOutput(cmd.Stdout)
	fs.Usage = cmd.printUsage
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 {
		fmt.Printf("path required\n\n")
		fs.Usage()
		return nil
	}

	if !cmd.verbose {
		log.SetOutput(ioutil.Discard)
	}

	// Validate measurement or sanitize flag.
	if cmd.measurement == "" && !cmd.sanitize {
		return fmt.Errorf("-measurement or -sanitize flag required")
	}

	// Process each TSM file.
	for _, path := range fs.Args() {
		log.Printf("processing: %s", path)
		if err := cmd.process(path); err != nil {
			return err
		}
	}

	return nil
}

func (cmd *Command) process(path string) error {
	// Open TSM reader.
	input, err := os.Open(path)
	if err != nil {
		return err
	}
	defer input.Close()

	r, err := tsm1.NewTSMReader(input)
	if err != nil {
		return fmt.Errorf("unable to read %s: %s", path, err)
	}
	defer r.Close()

	// Remove previous temporary files.
	outputPath := path + ".rewriting.tmp"
	if err := os.RemoveAll(outputPath); err != nil {
		return err
	} else if err := os.RemoveAll(outputPath + ".idx.tmp"); err != nil {
		return err
	}

	// Create TSMWriter to temporary location.
	output, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer output.Close()

	w, err := tsm1.NewTSMWriter(output)
	if err != nil {
		return err
	}
	defer w.Close()

	// Iterate over the input blocks.
	itr := r.BlockIterator()
	for itr.Next() {
		// Read key & time range.
		key, minTime, maxTime, _, _, block, err := itr.Read()
		if err != nil {
			return err
		}

		// Skip block if this is the measurement and time range we are deleting.
		series, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
		measurement, tags := models.ParseKey(series)
		if string(measurement) == cmd.measurement || (cmd.sanitize && !models.ValidKeyTokens(measurement, tags)) {
			log.Printf("deleting block: %s (%s-%s) sz=%d",
				key,
				time.Unix(0, minTime).UTC().Format(time.RFC3339Nano),
				time.Unix(0, maxTime).UTC().Format(time.RFC3339Nano),
				len(block),
			)
			continue
		}

		if err := w.WriteBlock(key, minTime, maxTime, block); err != nil {
			return err
		}
	}

	// Write index & close.
	if err := w.WriteIndex(); err != nil {
		return err
	} else if err := w.Close(); err != nil {
		return err
	}

	// Replace original file with new file.
	return os.Rename(outputPath, path)
}

func (cmd *Command) printUsage() {
	fmt.Print(`Deletes a measurement from a raw tsm file.

Usage: influx_inspect deletetsm [flags] path...

    -measurement NAME
            The name of the measurement to remove.
    -sanitize
            Remove all keys with non-printable unicode characters.
    -v
            Enable verbose logging.`)
}
