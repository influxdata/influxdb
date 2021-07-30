package dump_wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
)

type dumpWALCommand struct {
	findDuplicates bool
}

func NewDumpWALCommand() *cobra.Command {
	var dumpWAL dumpWALCommand
	cmd := &cobra.Command{
		Use:   "dump-wal",
		Short: "Dumps TSM data from WAL files",
		Long: `
This tool dumps data from WAL files for debugging purposes. Given at least one WAL file path as an argument, the tool will parse and print out the entries in each file. 
It has two modes of operation, depending on the --find-duplicates flag.
--find-duplicates=false (default): for each file, the following is printed:
	* The file name
	* for each entry,
		* The type of the entry (either [write] or [delete-bucket-range]);
		* The formatted entry contents
--find-duplicates=true: for each file, the following is printed:
	* The file name
	* A list of keys in the file that have duplicate or out of order timestamps
`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return dumpWAL.run(*cmd, args)
		},
	}

	cmd.Flags().BoolVarP(&dumpWAL.findDuplicates, "find-duplicates", "", false,
		"ignore dumping entries; only report keys in the WAL files that are duplicates or out of order (default false)")

	return cmd
}

func (dumpWAL *dumpWALCommand) run(cmd cobra.Command, args []string) error {

	// Process each WAL file.
	for _, path := range args {
		if err := dumpWAL.processWALFile(cmd, path); err != nil {
			return err
		}
	}
	return nil
}

func (dumpWAL *dumpWALCommand) processWALFile(cmd cobra.Command, path string) error {
	if filepath.Ext(path) != "."+tsm1.WALFileExtension {
		cmd.Printf("invalid wal file path, skipping %s\n", path)
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
	defer r.Close()

	// Iterate over the WAL entries.
	for r.Next() {
		entry, err := r.Read()
		if err != nil {
			return fmt.Errorf("failed to read entry from %q: %w", path, err)
		}

		switch entry := entry.(type) {
		case *tsm1.WriteWALEntry:
			if !dumpWAL.findDuplicates {
				cmd.Printf("[write] sz=%d\n", entry.MarshalSize())
			}

			keys := make([]string, 0, len(entry.Values))
			for k := range entry.Values {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, k := range keys {
				for _, v := range entry.Values[k] {
					t := v.UnixNano()

					if dumpWAL.findDuplicates {
						// Check for duplicate/out of order keys.
						if min, ok := minTimestampByKey[k]; ok && t <= min {
							duplicateKeys[k] = struct{}{}
						}
						minTimestampByKey[k] = t

						// Skip printing if we are only showing duplicate keys.
						continue
					}

					switch v := v.(type) {
					case tsm1.IntegerValue:
						cmd.Printf("%s %vi %d\n", k, v.Value(), t)
					case tsm1.UnsignedValue:
						cmd.Printf("%s %vu %d\n", k, v.Value(), t)
					case tsm1.FloatValue:
						cmd.Printf("%s %v %d\n", k, v.Value(), t)
					case tsm1.BooleanValue:
						cmd.Printf("%s %v %d\n", k, v.Value(), t)
					case tsm1.StringValue:
						cmd.Printf("%s %q %d\n", k, v.Value(), t)
					default:
						cmd.Printf("%s EMPTY\n", k)
					}
				}
			}

		case *tsm1.DeleteWALEntry:
			cmd.Printf("[delete] sz=%d\n", entry.MarshalSize())
			for _, k := range entry.Keys {
				cmd.Printf("%s\n", string(k))
			}

		case *tsm1.DeleteRangeWALEntry:
			cmd.Printf("[delete-range] min=%d max=%d sz=%d\n", entry.Min, entry.Max, entry.MarshalSize())
			for _, k := range entry.Keys {
				cmd.Printf("%s\n", string(k))
			}

		default:
			return fmt.Errorf("invalid wal entry: %#v", entry)
		}
	}

	// Print keys with duplicate or out-of-order points, if requested.
	if dumpWAL.findDuplicates {
		keys := make([]string, 0, len(duplicateKeys))

		if len(duplicateKeys) == 0 {
			cmd.Println("No duplicates or out of order timestamps found")
			return nil
		}

		for k := range duplicateKeys {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			cmd.Println(k)
		}
	}

	return nil
}
