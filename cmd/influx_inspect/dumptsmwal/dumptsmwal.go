// Package dumptsmwal dumps all data from a WAL file.
package dumptsmwal

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// Command represents the program execution for "influxd dumptsmwal".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	showDuplicates bool
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
	fs := flag.NewFlagSet("dumptsmwal", flag.ExitOnError)
	fs.SetOutput(cmd.Stdout)
	fs.BoolVar(&cmd.showDuplicates, "show-duplicates", false, "prints keys with out-of-order or duplicate values")
	fs.Usage = cmd.printUsage
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 {
		fmt.Printf("path required\n\n")
		fs.Usage()
		return nil
	}

	// Process each TSM WAL file.
	for _, path := range fs.Args() {
		if err := cmd.process(path); err != nil {
			return err
		}
	}
	return nil
}

func (cmd *Command) process(path string) error {
	if filepath.Ext(path) != "."+tsm1.WALFileExtension {
		log.Printf("invalid wal filename, skipping %s", path)
		return nil
	}

	// Track the earliest timestamp for each key and a set of keys with out-of-order points.
	minTimestampByKey := make(map[string]int64)
	duplicateKeys := make(map[string]struct{})

	// Open WAL reader.
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := tsm1.NewWALSegmentReader(f)

	// Iterate over the WAL entries.
	for r.Next() {
		entry, err := r.Read()
		if err != nil {
			return fmt.Errorf("cannot read entry: %s", err)
		}

		switch entry := entry.(type) {
		case *tsm1.WriteWALEntry:
			if !cmd.showDuplicates {
				fmt.Printf("[write] sz=%d\n", entry.MarshalSize())
			}

			keys := make([]string, 0, len(entry.Values))
			for k := range entry.Values {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, k := range keys {
				for _, v := range entry.Values[k] {
					t := v.UnixNano()

					// Check for duplicate/out of order keys.
					if min, ok := minTimestampByKey[k]; ok && t <= min {
						duplicateKeys[k] = struct{}{}
					}
					minTimestampByKey[k] = t

					// Skip printing if we are only showing duplicate keys.
					if cmd.showDuplicates {
						continue
					}

					switch v := v.(type) {
					case tsm1.IntegerValue:
						fmt.Printf("%s %vi %d\n", k, v.Value(), t)
					case tsm1.UnsignedValue:
						fmt.Printf("%s %vu %d\n", k, v.Value(), t)
					case tsm1.FloatValue:
						fmt.Printf("%s %v %d\n", k, v.Value(), t)
					case tsm1.BooleanValue:
						fmt.Printf("%s %v %d\n", k, v.Value(), t)
					case tsm1.StringValue:
						fmt.Printf("%s %q %d\n", k, v.Value(), t)
					default:
						fmt.Printf("%s EMPTY\n", k)
					}
				}
			}

		case *tsm1.DeleteWALEntry:
			fmt.Printf("[delete] sz=%d\n", entry.MarshalSize())
			for _, k := range entry.Keys {
				fmt.Printf("%s\n", string(k))
			}

		case *tsm1.DeleteRangeWALEntry:
			fmt.Printf("[delete-range] min=%d max=%d sz=%d\n", entry.Min, entry.Max, entry.MarshalSize())
			for _, k := range entry.Keys {
				fmt.Printf("%s\n", string(k))
			}

		default:
			return fmt.Errorf("invalid wal entry: %#v", entry)
		}
	}

	// Print keys with duplicate or out-of-order points, if requested.
	if cmd.showDuplicates {
		keys := make([]string, 0, len(duplicateKeys))
		for k := range duplicateKeys {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			fmt.Println(k)
		}
	}

	return nil
}

func (cmd *Command) printUsage() {
	fmt.Print(`Dumps all entries from one or more TSM WAL files.

Usage: influx_inspect dumptsmwal path...`)
}
