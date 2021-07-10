package dump_wal

import (
	"fmt"
	"github.com/influxdata/influxdb/v2/kit/errors"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"sort"
)

type dumpWALCommand struct {
	findDuplicates bool
}

func NewDumpWALCommand() *cobra.Command {
	var  dumpWAL dumpWALCommand
	cmd := &cobra.Command{
		Use:   "dump-wal",
		Short: "Dump TSM data from WAL files",
		Long: `
This tool dumps data from WAL files for debugging purposes. Given a list of filepath globs 
(patterns which match to .wal file paths), the tool will parse and print out the entries in each file. 
It has two modes of operation, depending on the --find-duplicates flag.
--find-duplicates=false (default): for each file, the following is printed:
	* The file name
	* for each entry,
		* The type of the entry (either [write] or [delete-bucket-range]);
		* The formatted entry contents
--find-duplicates=true: for each file, the following is printed:
	* The file name
	* A list of keys in the file that have out of order timestamps
`,
		RunE: func(cmd *cobra.Command, args []string) error{
			if len(args) == 0{
				return errors.New("no files provided. aborting")
			}
			args = cmd.ValidArgs
			return dumpWAL.run(args)
		},
	}

	cmd.Flags().BoolVarP(&dumpWAL.findDuplicates, "find-duplicates", "", false,
		"ignore dumping entries; only report keys in the WAL that are out of order")

	return cmd
}

func (dumpWAL *dumpWALCommand) run(args []string) error {

	// Process each TSM WAL file.
	for _, path := range args {
		if err := dumpWAL.process(path); err != nil {
			return err
		}
	}
	return nil
}

func (dumpWAL *dumpWALCommand) process(path string) error {
	if filepath.Ext(path) != "."+tsm1.WALFileExtension {
		fmt.Printf("invalid wal filename, skipping %s", path)
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
			if !dumpWAL.findDuplicates {
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
					if dumpWAL.findDuplicates {
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
	if dumpWAL.findDuplicates {
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
