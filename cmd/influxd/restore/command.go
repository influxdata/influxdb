package restore

import (
	"fmt"
	"path/filepath"

	"github.com/influxdata/influxdb/bolt"
	"github.com/influxdata/influxdb/internal/fs"
	"github.com/influxdata/influxdb/kit/cli"
	"github.com/spf13/cobra"
)

var Command = &cobra.Command{
	Use:   "restore",
	Short: "Restore data and metadata from a backup",
	Long: `
This command restores data and metadata from a backup fileset.



NOTES:

* The influxd server should not be running when using the restore tool
  as it replaces all data and metadata.
`,
	Args: cobra.ExactArgs(0),
	RunE: restoreE,
}

var flags struct {
	boltPath   string
	enginePath string
}

func init() {
	dir, err := fs.InfluxDir()
	if err != nil {
		panic(fmt.Errorf("failed to determine influx directory: %v", err))
	}

	Command.Flags().SortFlags = false

	pfs := Command.PersistentFlags()
	pfs.SortFlags = false

	opts := []cli.Opt{
		{
			DestP:   &flags.boltPath,
			Flag:    "bolt-path",
			Default: filepath.Join(dir, bolt.DefaultFilename),
			Desc:    "path to boltdb database",
		},
		{
			DestP:   &flags.enginePath,
			Flag:    "engine-path",
			Default: filepath.Join(dir, "engine"),
			Desc:    "path to persistent engine files",
		},
	}

	cli.BindOptions(Command, opts)
}

func restoreE(cmd *cobra.Command, args []string) error {
	panic("implement me")
}
