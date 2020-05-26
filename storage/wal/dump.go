package wal

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"text/tabwriter"

	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/value"
)

// Command represents the program execution for "influxd inspect dumpmwal
// This command will dump all entries from a given list WAL filepath globs

type Dump struct {
	// Standard input/output
	Stderr io.Writer
	Stdout io.Writer

	// A list of files to dump
	FileGlobs []string

	// Whether or not to check for duplicate/out of order entries
	FindDuplicates bool
}

type DumpReport struct {
	// The file this report corresponds to
	File string
	// Any keys found to be duplicated/out of order
	DuplicateKeys []string
	// A list of all the write wal entries from this file
	Writes []*WriteWALEntry
	// A list of all the delete wal entries from this file
	Deletes []*DeleteBucketRangeWALEntry
}

// Run executes the dumpwal command, generating a list of DumpReports
// for each requested file. The `print` flag indicates whether or not
// the command should log output during execution. If the command is run
// as a cli, Run(true) should be used, and if the tool is run programmatically,
// output should likely be suppressed with Run(false).
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

	twOut := tabwriter.NewWriter(w.Stdout, 8, 2, 1, ' ', 0)
	twErr := tabwriter.NewWriter(w.Stderr, 8, 2, 1, ' ', 0)

	// Process each WAL file.
	paths, err := globAndDedupe(w.FileGlobs)
	if err != nil {
		return nil, err
	}

	var reports []*DumpReport
	for _, path := range paths {
		r, err := w.process(path, twOut, twErr)
		if err != nil {
			return nil, err
		}

		reports = append(reports, r)
	}

	return reports, nil
}

func globAndDedupe(globs []string) ([]string, error) {
	files := make(map[string]struct{})
	for _, filePattern := range globs {
		matches, err := filepath.Glob(filePattern)
		if err != nil {
			return nil, err
		}

		for _, match := range matches {
			files[match] = struct{}{}
		}
	}

	return sortKeys(files), nil
}

func sortKeys(m map[string]struct{}) []string {
	s := make([]string, 0, len(m))
	for k := range m {
		s = append(s, k)
	}
	sort.Strings(s)

	return s
}

func (w *Dump) process(path string, stdout, stderr io.Writer) (*DumpReport, error) {
	if filepath.Ext(path) != "."+WALFileExtension {
		fmt.Fprintf(stderr, "invalid wal filename, skipping %s", path)
		return nil, fmt.Errorf("invalid wal filename: %s", path)
	}

	report := &DumpReport{
		File: path,
	}

	fmt.Fprintf(stdout, "File: %s\n", path)

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

	// Iterate over the WAL entries
	for r.Next() {
		entry, err := r.Read()
		if err != nil {
			fmt.Fprintf(stdout, "Error: cannot read entry: %v ", err)
			return nil, fmt.Errorf("cannot read entry: %v", err)
		}

		switch entry := entry.(type) {
		case *WriteWALEntry:
			// MarshalSize must always be called to make sure the size of the entry is set
			sz := entry.MarshalSize()
			if !w.FindDuplicates {
				fmt.Fprintf(stdout, "[write] sz=%d\n", sz)
			}
			report.Writes = append(report.Writes, entry)

			keys := make([]string, 0, len(entry.Values))
			for k := range entry.Values {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, k := range keys {
				fmtKey, err := formatKeyOrgBucket(k)
				// if key cannot be properly formatted with org and bucket, skip printing
				if err != nil {
					fmt.Fprintf(stderr, "Invalid key: %v\n", err)
					return nil, fmt.Errorf("invalid key: %v", err)
				}

				for _, v := range entry.Values[k] {
					t := v.UnixNano()

					// Skip printing if we are only showing duplicate keys.
					if w.FindDuplicates {
						// Check for duplicate/out of order keys.
						if min, ok := minTimestampByKey[k]; ok && t <= min {
							duplicateKeys[k] = struct{}{}
						}
						minTimestampByKey[k] = t
						continue
					}

					switch v := v.(type) {
					case value.IntegerValue:
						fmt.Fprintf(stdout, "%s %vi %d\n", fmtKey, v.Value(), t)
					case value.UnsignedValue:
						fmt.Fprintf(stdout, "%s %vu %d\n", fmtKey, v.Value(), t)
					case value.FloatValue:
						fmt.Fprintf(stdout, "%s %v %d\n", fmtKey, v.Value(), t)
					case value.BooleanValue:
						fmt.Fprintf(stdout, "%s %v %d\n", fmtKey, v.Value(), t)
					case value.StringValue:
						fmt.Fprintf(stdout, "%s %q %d\n", fmtKey, v.Value(), t)
					default:
						fmt.Fprintf(stdout, "%s EMPTY\n", fmtKey)
					}
				}
			}
		case *DeleteBucketRangeWALEntry:
			bucketID := entry.BucketID.String()
			orgID := entry.OrgID.String()

			// MarshalSize must always be called to make sure the size of the entry is set
			sz := entry.MarshalSize()
			if !w.FindDuplicates {
				pred := new(datatypes.Predicate)
				if len(entry.Predicate) > 0 {
					if err := pred.Unmarshal(entry.Predicate[1:]); err != nil {
						return nil, fmt.Errorf("invalid predicate on wal entry: %#v\nerr: %v", entry, err)
					}
				}
				fmt.Fprintf(stdout, "[delete-bucket-range] org=%s bucket=%s min=%d max=%d sz=%d pred=%s\n", orgID, bucketID, entry.Min, entry.Max, sz, pred.String())
			}
			report.Deletes = append(report.Deletes, entry)
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

		fmt.Fprintln(stdout, "Duplicate/out of order keys:")
		for _, k := range keys {
			fmtKey, err := formatKeyOrgBucket(k)
			// don't print keys that cannot be formatted with org/bucket
			if err != nil {
				fmt.Fprintf(stderr, "Error: %v\n", err)
				continue
			}
			fmt.Fprintf(stdout, "  %s\n", fmtKey)
		}
		report.DuplicateKeys = keys
	}

	return report, nil
}

// removes the first 16 bytes of the key, formats as org and bucket id (hex),
// and re-appends to the key so that it can be pretty printed
func formatKeyOrgBucket(key string) (string, error) {
	b := []byte(key)
	if len(b) < 16 {
		return "", fmt.Errorf("key too short to format with org and bucket")
	}

	var a [16]byte
	copy(a[:], b[:16])

	org, bucket := tsdb.DecodeName(a)

	s := fmt.Sprintf("%s%s", org.String(), bucket.String())
	k := s + string(b[16:])

	return k, nil
}
