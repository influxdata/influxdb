package inspect

import (
	"os"

	"github.com/influxdata/influxdb/v2/tsdb/tsm1"
	"github.com/spf13/cobra"
)

func NewExportBlocksCommand() *cobra.Command {
	return &cobra.Command{
		Use:   `export-blocks`,
		Short: "Exports block data",
		Long: `
This command will export all blocks in one or more TSM1 files to
another format for easier inspection and debugging.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			e := tsm1.NewSQLBlockExporter(os.Stdout)
			for _, arg := range args {
				if err := e.ExportFile(arg); err != nil {
					return err
				}
			}
			if err := e.Close(); err != nil {
				return err
			}
			return nil
		},
	}
}
