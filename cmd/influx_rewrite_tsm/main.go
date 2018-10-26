package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

func main() {
	if err := run(os.Args[1:]); err == flag.ErrHelp {
		os.Exit(1)
	} else if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("influx_rewrite_tsm", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 {
		return errors.New("path required")
	}

	for _, filename := range fs.Args() {
		if err := process(filename); err != nil {
			return err
		}
	}
	return nil
}

func process(filename string) error {
	in, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer in.Close()

	r, err := tsm1.NewTSMReader(in)
	if err != nil {
		return err
	}
	defer r.Close()

	tempFilename := filename + ".tmp"
	out, err := os.Create(tempFilename)
	if err != nil {
		return err
	}
	defer out.Close()

	w, err := tsm1.NewTSMWriter(out)
	if err != nil {
		return err
	}
	defer w.Close()

	// Iterate over blocks and rewrite block & index.
	itr := r.BlockIterator()
	for itr.Next() {
		// Read next block.
		key, oldMinTime, oldMaxTime, _, _, buf, err := itr.Read()
		if err != nil {
			return fmt.Errorf("block read error: %s", err)
		}

		// Determine min & max directly from the values.
		values, err := tsm1.DecodeBlock(buf, nil)
		if err != nil {
			return fmt.Errorf("block decode error: %s", err)
		}
		minTime, maxTime := tsm1.Values(values).MinTime(), tsm1.Values(values).MaxTime()

		// Report index changes.
		if minTime != oldMinTime || maxTime != oldMaxTime {
			log.Printf("[diff] index time updated: (%d,%d) → (%d,%d) %s", oldMinTime, oldMaxTime, minTime, maxTime, key)
		}

		// Rewrite block.
		if err := w.WriteBlock(key, minTime, maxTime, buf); err != nil {
			return fmt.Errorf("block write error: %s", err)
		}
	}

	// Write index & close.
	if err := w.WriteIndex(); err != nil {
		return err
	} else if err := w.Close(); err != nil {
		return err
	}

	// Replace original file with new file.
	return os.Rename(tempFilename, filename)
}
