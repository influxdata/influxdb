package inspect

import (
	"github.com/influxdata/influxdb/kit/errors"
	"github.com/influxdata/influxdb/storage/wal"
	"github.com/spf13/cobra"
	"os"
)

var dumpTSMWALFlags = struct {
	findDuplicates bool
}{}

func NewDumpTSMWALCommand() *cobra.Command {
	dumpTSMWALCommand := &cobra.Command{
		Use:   "dumptsmwal",
		Short: "Dump TSM data from WAL files",
		Long: `
This tool dumps data from WAL files for debugging purposes. Given a list of .wal files,
the tool will parse and print out the entries in each file. For each file, the following is printed:
	* The file name
	* for each entry,
		* The type of the entry (either [write] or [delete-bucket-range]);
		* The formatted entry contents
If the tool is run with --find-duplicates=true, A list of all keys with duplicate or out of order timestamps will 
be printed.
`,
		RunE: inspectDumpTSMWAL,
	}

	dumpTSMWALCommand.Flags().BoolVarP(&dumpTSMWALFlags.findDuplicates, "find-duplicates", "", false, "report keys with out of order points")

	return dumpTSMWALCommand
}

func inspectDumpTSMWAL(cmd *cobra.Command, args []string) error {
	dumper := &wal.Dump{
		Stdout:         os.Stdout,
		Stderr:         os.Stderr,
		Files:          args,
		FindDuplicates: dumpTSMWALFlags.findDuplicates,
	}

	if len(args) == 0 {
		return errors.New("no files provided. aborting")
	}

	_, err := dumper.Run(true)
	return err
}
