package inspect

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/influxdata/influxdb/v2/tsdb/tsm1"
	"github.com/spf13/cobra"
)

// verifyTSMFlags defines the `verify-tsm` Command.
var verifyTSMFlags = struct {
	cli.OrgBucket
	path string
}{}

func NewVerifyTSMCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "verify-tsm <pathspec>...",
		Short: "Checks the consistency of TSM files",
		Long: `
This command will analyze a set of TSM files for inconsistencies between the
TSM index and the blocks.

The checks performed by this command are:

* CRC-32 checksums match for each block
* TSM index min and max timestamps match decoded data

OPTIONS

   <pathspec>...
      A list of files or directories to search for TSM files.

An optional organization or organization and bucket may be specified to limit
the analysis.
`,
		RunE: verifyTSMF,
	}

	verifyTSMFlags.AddFlags(cmd)

	return cmd
}

func verifyTSMF(cmd *cobra.Command, args []string) error {
	verify := tsm1.VerifyTSM{
		Stdout:   os.Stdout,
		OrgID:    verifyTSMFlags.Org,
		BucketID: verifyTSMFlags.Bucket,
	}

	// resolve all pathspecs
	for _, arg := range args {
		fi, err := os.Stat(arg)
		if err != nil {
			fmt.Printf("Error processing path %q: %v", arg, err)
			continue
		}

		if fi.IsDir() {
			files, _ := filepath.Glob(filepath.Join(arg, "*."+tsm1.TSMFileExtension))
			verify.Paths = append(verify.Paths, files...)
		} else {
			verify.Paths = append(verify.Paths, arg)
		}
	}

	return verify.Run()
}
