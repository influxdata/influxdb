package wal

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"
)

type Verifier struct {
	Stderr io.Writer
	Stdout io.Writer
	Dir    string
}

type VerificationSummary struct {
	EntryCount int
	FileCount int
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

	for _, file := range files {
		f, err := os.OpenFile(file, os.O_RDONLY, 0600)
		if err != nil {
			fmt.Fprintf(v.Stderr, "error %s: %v. Exiting", file, err)
		}

		clean := true
		reader := NewWALSegmentReader(f)
		for reader.Next() {
			entriesScanned++
			_, err := reader.Read()
			if err != nil {
				clean = false
				fmt.Fprintf(tw,"%s: corrupt entry found at position %d\n", file, reader.Count())
				break
			}

		}

		if clean {
			fmt.Fprintf(tw, "%s: clean\n", file)
		}

		if !clean {
			corruptFiles = append(corruptFiles, file)
		}
	}

	fmt.Fprintf(tw, "Statistics:\n")
	fmt.Fprintf(tw, "Files checked: %d\n", len(files))
	fmt.Fprintf(tw, "Corrupt files found: %s\n", strings.Join(corruptFiles, ","))
	fmt.Fprintf(tw, "Total entries checked: %d\n", entriesScanned);
	fmt.Fprintf(tw, "Time Elapsed %v\n", time.Since(start))

	summary := &VerificationSummary{
		EntryCount: entriesScanned,
		CorruptFiles: corruptFiles,
		FileCount: len(files),
	}

	return summary, nil
}
