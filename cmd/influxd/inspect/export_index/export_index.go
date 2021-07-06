package export_index

import (
	"bufio"
	"os"

	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/index/tsi1"
	"github.com/spf13/cobra"
)

func NewExportIndexCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   `export-index`,
		Short: "Exports TSI index data",
		Long: `
This command will export all series in a TSI index to
SQL format for easier inspection and debugging.`,
		Args: cobra.NoArgs,
	}

	var seriesFilePath, dataPath string
	cmd.Flags().StringVar(&seriesFilePath, "series-path", "", "Path to series file")
	cmd.Flags().StringVar(&dataPath, "index-path", "", "Path to the index directory of the data engine")
	_ = cmd.MarkFlagRequired("series-path")
	_ = cmd.MarkFlagRequired("index-path")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		// Initialize series file.
		sfile := tsdb.NewSeriesFile(seriesFilePath)
		if err := sfile.Open(); err != nil {
			return err
		}
		defer sfile.Close()

		// Open index.
		idx := tsi1.NewIndex(sfile, "", tsi1.WithPath(dataPath), tsi1.DisableCompactions())
		if err := idx.Open(); err != nil {
			return err
		}
		defer idx.Close()

		// Dump out index data.
		w := bufio.NewWriter(os.Stdout)
		e := tsi1.NewSQLIndexExporter(w)
		if err := e.ExportIndex(idx); err != nil {
			return err
		} else if err := e.Close(); err != nil {
			return err
		} else if err := w.Flush(); err != nil {
			return err
		}
		return nil
	}

	return cmd
}
