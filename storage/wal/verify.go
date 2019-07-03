package wal

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"text/tabwriter"
	"time"
)

type Verifier struct {
	Stderr io.Writer
	Stdout io.Writer
	Dir    string
}

type VerificationSummary struct {
	EntryCount   int
	FileCount    int
	CorruptFiles []string
}

func (v *Verifier) Run(print bool) (*VerificationSummary, error) {
	if v.Stderr == nil {
		v.Stderr = os.Stderr
	}

	if v.Stdout == nil {
		v.Stdout = os.Stdout
	}

	if !print {
		v.Stderr, v.Stdout = ioutil.Discard, ioutil.Discard
	}

	dir, err := os.Stat(v.Dir)
	if err != nil {
		return nil, err
	} else if !dir.IsDir() {
		return nil, errors.New("invalid data directory")
	}

	files, err := filepath.Glob(path.Join(v.Dir, "*.wal"))

	if err != nil {
		panic(err)
	}

	start := time.Now()
	tw := tabwriter.NewWriter(v.Stdout, 8, 2, 1, ' ', 0)

	var corruptFiles []string
	var entriesScanned int

	for _, fpath := range files {
		f, err := os.OpenFile(fpath, os.O_RDONLY, 0600)
		if err != nil {
			fmt.Fprintf(v.Stderr, "error opening file %s: %v. Exiting", fpath, err)
		}

		clean := true
		reader := NewWALSegmentReader(f)
		for reader.Next() {
			entriesScanned++
			_, err := reader.Read()
			if err != nil {
				clean = false
				fmt.Fprintf(tw, "%s: corrupt entry found at position %d\n", fpath, reader.Count())
				corruptFiles = append(corruptFiles, fpath)
				break
			}

		}

		if clean {
			fmt.Fprintf(tw, "%s: clean\n", fpath)
		}
	}

	fmt.Fprintf(tw, "Results:\n")
	fmt.Fprintf(tw, "  Files checked: %d\n", len(files))
	fmt.Fprintf(tw, "  Total entries checked: %d\n", entriesScanned)
	fmt.Fprintf(tw, "  Corrupt files found: ")
	if len(corruptFiles) == 0 {
		fmt.Fprintf(tw, "None")
	} else {
		for _, name := range corruptFiles {
			fmt.Fprintf(tw, "\n    %s", name)
		}
	}

	fmt.Fprintf(tw, "\nCompleted in %v\n", time.Since(start))

	summary := &VerificationSummary{
		EntryCount:   entriesScanned,
		CorruptFiles: corruptFiles,
		FileCount:    len(files),
	}

	return summary, nil
}
