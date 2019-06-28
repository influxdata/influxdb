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
update....`,
		RunE: inspectDumpTSMWAL,
	}

	dumpTSMWALCommand.Flags().BoolVarP(&dumpTSMWALFlags.findDuplicates, "duplicates", "", false, "report keys with out of order points")

	return dumpTSMWALCommand
}

func inspectDumpTSMWAL(cmd *cobra.Command, args []string) error {
	dumper := &wal.Dump{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		Files:  args,
	}

	if len(args) == 0 {
		return errors.New("no files provided. aborting")
	}

	_, err := dumper.Run(true)
	return err
}
