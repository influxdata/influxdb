package inspect

import (
	"fmt"
	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/storage/wal"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
)

func NewVerifyWALCommand() *cobra.Command {
	verifyWALCommand := &cobra.Command{
		Use:   `verify-wal`,
		Short: "Check for WAL corruption",
		Long: `
This command will analyze the WAL (Write-Ahead Log) in a storage directory to 
check if there are any corrupt files. If any corrupt files are found, the names
of said corrupt files will be reported. The tool will also count the total number
of entries in the scanned WAL files, in case this is of interest.

For each file, the following is output:
	* The file name;
	* "clean" (if the file is clean) OR 
      The first position of any corruption that is found
In the summary section, the following is printed:
	* The number of WAL files scanned;
	* The number of WAL entries scanned;
	* A list of files found to be corrupt`,
		RunE: inspectVerifyWAL,
	}

	dir, err := fs.InfluxDir()
	if err != nil {
		panic(err)
	}
	dir = filepath.Join(dir, "engine/wal")
	verifyWALCommand.Flags().StringVarP(&verifyWALFlags.dataDir, "data-dir", "", dir, fmt.Sprintf("use provided data directory (defaults to %s).", dir))

	return verifyWALCommand
}

var verifyWALFlags = struct {
	dataDir string
}{}

// inspectReportTSMF runs the report-tsm tool.
func inspectVerifyWAL(cmd *cobra.Command, args []string) error {
	report := &wal.Verifier{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
		Dir:    verifyWALFlags.dataDir,
	}

	_, err := report.Run(true)
	return err
}
