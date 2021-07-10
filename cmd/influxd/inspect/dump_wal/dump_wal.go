package dump_wal

import (
	"fmt"
	"github.com/influxdata/influxdb/v2/kit/errors"
"github.com/spf13/cobra"
)

var dumpWALFlags = struct {
	findDuplicates bool
}{}

func NewDumpWALCommand() *cobra.Command {
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
		RunE: inspectDumpWAL,
	}

	cmd.Flags().BoolVarP(
		&dumpWALFlags.findDuplicates,
		"find-duplicates", "", false, "ignore dumping entries; only report keys in the WAL that are out of order")

	return cmd
}

func inspectDumpWAL(cmd *cobra.Command, args []string) error {

	if len(args) == 0 {
		return errors.New("no files provided. aborting")
	}

	fmt.Println("Hi, this is the inspect dump-wal command")

	return nil
}
