package inspect

import (
	"errors"
	"path/filepath"

	"github.com/influxdata/influxdb/internal/fs"
	"github.com/influxdata/influxdb/tsdb/tsm1"
	"github.com/spf13/cobra"
)

func newVerifyTSMCommand() *cobra.Command {
	var pattern string
	var orgID, bucketID string

	influxDir, err := fs.InfluxDir()
	if err != nil {
		panic(err)
	}

	cmd := &cobra.Command{
		Use:   "verify-tsm",
		Short: "Verify TSM file consistency",
		Long: `
This command will verify checksums of every block in the specified TSM files.
If a directory is specified then it is recursively searched for any .tsm files.
The data directory will be searched if no paths are specified.

For each block, an error will be reported if the block cannot be read or the
checksum does not match the computed checksum for the block. If a file is
successfully read without any errors then it will list the file as "healthy".`,
	}

	cmd.Flags().StringVarP(&pattern, "pattern", "", `\.tsm$`, "only process TSM files containing pattern")
	cmd.Flags().StringVarP(&orgID, "org-id", "", "", "process only data belonging to organization ID.")
	cmd.Flags().StringVarP(&bucketID, "bucket-id", "", "", "process only data belonging to bucket ID. Requires org flag to be set.")

	cmd.RunE = func(cmd *cobra.Command, args []string) (err error) {
		if orgID == "" && bucketID != "" {
			return errors.New("org-id must be set for non-empty bucket-id")
		}

		// Default to data directory if no paths specified.
		paths := args
		if len(paths) == 0 {
			paths = []string{filepath.Join(influxDir, "engine/data")}
		}

		report := tsm1.NewVerificationReport()
		if report.Paths, err = fs.ExpandFiles(args, pattern); err != nil {
			return err
		} else if report.OrgID, err = idFromStringIfExists(orgID); err != nil {
			return err
		} else if report.BucketID, err = idFromStringIfExists(bucketID); err != nil {
			return err
		}

		_, err = report.Run()
		return err
	}

	return cmd
}
