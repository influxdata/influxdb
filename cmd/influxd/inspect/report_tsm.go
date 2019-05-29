package inspect

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/internal/fs"
	"github.com/influxdata/influxdb/tsdb/tsm1"
	"github.com/spf13/cobra"
)

func newReportTSMCommand() *cobra.Command {
	var pattern string
	var exact bool
	var detailed bool
	var orgID, bucketID string
	var dataDir string

	influxDir, err := fs.InfluxDir()
	if err != nil {
		panic(err)
	}

	cmd := &cobra.Command{
		Use:   "report-tsm",
		Short: "Run TSM report",
		Long: `
This command will analyze TSM files within a storage engine directory, reporting 
the cardinality within the files as well as the time range that the point data 
covers.

This command only interrogates the index within each file, and does not read any
block data. To reduce heap requirements, by default report-tsm estimates the 
overall cardinality in the file set by using the HLL++ algorithm. Exact 
cardinalities can be determined by using the --exact flag.

For each file, the following is output:

	* The full filename;
	* The series cardinality within the file;
	* The number of series first encountered within the file;
	* The min and max timestamp associated with TSM data in the file; and
	* The time taken to load the TSM index and apply any tombstones.

The summary section then outputs the total time range and series cardinality for 
the fileset. Depending on the --detailed flag, series cardinality is segmented 
in the following ways:

	* Series cardinality for each organization;
	* Series cardinality for each bucket;
	* Series cardinality for each measurement;
	* Number of field keys for each measurement; and
	* Number of tag values for each tag key.`,
	}

	cmd.Flags().StringVarP(&pattern, "pattern", "", "", "only process TSM files containing pattern")
	cmd.Flags().BoolVarP(&exact, "exact", "", false, "calculate and exact cardinality count. Warning, may use significant memory...")
	cmd.Flags().BoolVarP(&detailed, "detailed", "", false, "emit series cardinality segmented by measurements, tag keys and fields. Warning, may take a while.")
	cmd.Flags().StringVarP(&orgID, "org-id", "", "", "process only data belonging to organization ID.")
	cmd.Flags().StringVarP(&bucketID, "bucket-id", "", "", "process only data belonging to bucket ID. Requires org flag to be set.")

	dir := filepath.Join(influxDir, "engine/data")
	cmd.Flags().StringVarP(&dataDir, "data-dir", "", dir, fmt.Sprintf("use provided data directory (defaults to %s).", dir))

	cmd.RunE = func(cmd *cobra.Command, args []string) (err error) {
		report := &tsm1.Report{
			Stderr:   os.Stderr,
			Stdout:   os.Stdout,
			Dir:      dataDir,
			Pattern:  pattern,
			Detailed: detailed,
			Exact:    exact,
		}

		if orgID == "" && bucketID != "" {
			return errors.New("org-id must be set for non-empty bucket-id")
		}
		if orgID != "" {
			if report.OrgID, err = influxdb.IDFromString(orgID); err != nil {
				return err
			}
		}
		if bucketID != "" {
			if report.BucketID, err = influxdb.IDFromString(bucketID); err != nil {
				return err
			}
		}

		_, err = report.Run(true)
		return err
	}

	return cmd
}
