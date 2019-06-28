// Package dumptsmwal dumps all data from a WAL file.
package wal

import (
	"fmt"
	"github.com/influxdata/influxdb/tsdb/value"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"text/tabwriter"
	"time"
)

// Command represents the program execution for "influxd dumptsmwal".
type Dump struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer
	Files []string

	FindDuplicates bool
}

type DumpReport struct {
	File string
	DuplicateKeys []string
}

// Run executes the command.
func (w *Dump) Run(print bool) ([]*DumpReport, error) {
	if w.Stderr == nil {
		w.Stderr = os.Stderr
	}

	if w.Stdout == nil {
		w.Stdout = os.Stdout
	}

	if !print {
		w.Stdout, w.Stderr = ioutil.Discard, ioutil.Discard
	}

	tw := tabwriter.NewWriter(w.Stdout, 8, 2, 1, ' ', 0)

	// Process each TSM WAL file.

	var reports []*DumpReport
	for _, path := range w.Files {
		duplicateKeys, err := w.process(tw, path)
		if err != nil {
			return nil, err
		}

		r := &DumpReport{
			File: path,
			DuplicateKeys: duplicateKeys,
		}
		reports = append(reports, r)
	}

	return reports, nil
}

func (w *Dump) process(out io.Writer, path string) ([]string, error) {
	if filepath.Ext(path) != "."+WALFileExtension {
		fmt.Fprintf(out,"invalid wal filename, skipping %s", path)
		return nil, nil
	}

	// Track the earliest timestamp for each key and a set of keys with out-of-order points.
	minTimestampByKey := make(map[string]int64)
	duplicateKeys := make(map[string]struct{})

	// Open WAL reader.
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := NewWALSegmentReader(f)

	// Iterate over the WAL entries.
	for r.Next() {
		entry, err := r.Read()
		if err != nil {
			return nil, fmt.Errorf("cannot read entry: %s", err)
		}

		switch entry := entry.(type) {
		case *WriteWALEntry:
			if !w.FindDuplicates {
				fmt.Fprintf(out,"[write] sz=%d\n", entry.MarshalSize())
			}

			keys := make([]string, 0, len(entry.Values))
			for k := range entry.Values {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			fmt.Println("keys: ", keys)

			for _, k := range keys {
				fmt.Println("key: ", k)
				time.Sleep(2 * time.Second)
				for _, v := range entry.Values[k] {
					t := v.UnixNano()

					// Check for duplicate/out of order keys.
					if min, ok := minTimestampByKey[k]; ok && t <= min {
						duplicateKeys[k] = struct{}{}
					}
					minTimestampByKey[k] = t

					// Skip printing if we are only showing duplicate keys.
					if w.FindDuplicates {
						continue
					}

					switch v := v.(type) {
					case value.IntegerValue:
						fmt.Fprintf(out,"%s %vi %d\n", k, v.Value(), t)
					case value.UnsignedValue:
						fmt.Fprintf(out,"%s %vu %d\n", k, v.Value(), t)
					case value.FloatValue:
						fmt.Fprintf(out,"%s %v %d\n", k, v.Value(), t)
					case value.BooleanValue:
						fmt.Fprintf(out,"%s %v %d\n", k, v.Value(), t)
					case value.StringValue:
						fmt.Fprintf(out,"%s %q %d\n", k, v.Value(), t)
					default:
						fmt.Fprintf(out,"%s EMPTY\n", k)
					}
				}
			}
		case *DeleteBucketRangeWALEntry:
			bucketID := entry.BucketID.GoString()
			orgID := entry.OrgID.GoString()
			fmt.Fprintf(out,"[delete-bucket-range] org=%s bucket=%s min=%d max=%d sz=%d\n", orgID, bucketID, entry.Min, entry.Max, entry.MarshalSize())
		default:
			return nil, fmt.Errorf("invalid wal entry: %#v", entry)
		}
	}

	// Print keys with duplicate or out-of-order points, if requested.
	if w.FindDuplicates {
		keys := make([]string, 0, len(duplicateKeys))
		for k := range duplicateKeys {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		fmt.Fprintln(out, "Duplicate/out of order keys:")
		for _, k := range keys {
			fmt.Fprintf(out, "  %s\n", k)
		}
		return keys, nil
	}

	return nil, nil
}