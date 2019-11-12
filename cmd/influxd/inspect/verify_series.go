package inspect

import (
	"encoding/hex"
	"os"
	"runtime"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"
)

// NewVerifySeriesFileCommand returns a new instance of verifySeriesCommand
// for execution of "influx_inspect verify-seriesfile".
func NewVerifySeriesFileCommand() *cobra.Command {
	verifySeriesCommand := &cobra.Command{
		Use:   "verify-seriesfile",
		Short: "Verifies the integrity of Series files",
		Long: `This command verifies the integrity and correctness of the Series
File segments and index.

It is possible to verify only data belonging to a single bucket or org using the 
--measurement-prefix flag.

Example for a single bucket: --measurement-prefix="0231f2dbca673232000000000000000a"

Example for all buckets in an org: --measurement-prefix="0231f2dbca673232"

Partitions are verified concurrently (unless -c = 1). The tool proceeds as follows:

  - Each segment in the partition is initialised in order.
  - All entries within the segment are read.
  - The series ID of each entry is checked to ensure it is monotonically increasing.
  - Upon reading an insertion entry the tool verifies there was not a previous deletion of the ID.
  - Each series key is parsed within an insertion entry.
  - Upon reading a deletion entry the tool verifies it was previously inserted.
  - The tool checks for invalid entries.
  - Once all segments in a partition are verified, the index for the partition is verified.
  - For each entry from the segment the tool verifies it exists correctly in the index.`,
		RunE: verifySeriesRun,
	}

	verifySeriesCommand.Flags().StringVar(&VerifySeriesFlags.seriesFile, "series-file", os.Getenv("HOME")+"/.influxdbv2/engine/_series", "Path to a series file. This defaults to "+os.Getenv("HOME")+"/.influxdbv2/engine/_series")
	verifySeriesCommand.Flags().BoolVarP(&VerifySeriesFlags.verbose, "v", "v", false, "verbose output")
	verifySeriesCommand.Flags().BoolVarP(&VerifySeriesFlags._recover, "recover", "r", true, "recover from a panic")
	verifySeriesCommand.Flags().BoolVarP(&VerifySeriesFlags.continueError, "continue-on-error", "", false, "continue verification after encountering an error")
	verifySeriesCommand.Flags().IntVarP(&VerifySeriesFlags.concurrent, "c", "c", runtime.GOMAXPROCS(0), "number of concurrent workers to use")
	verifySeriesCommand.Flags().StringVar(&VerifySeriesFlags.measurementPrefixHex, "measurement-prefix", "", "filter measurements by provided base-16 prefix")
	return verifySeriesCommand
}

var VerifySeriesFlags = struct {
	seriesFile           string
	verbose              bool
	continueError        bool
	_recover             bool
	concurrent           int
	measurementPrefixHex string
}{}

// verifySeriesRun executes the command.
func verifySeriesRun(cmd *cobra.Command, args []string) error {
	config := logger.NewConfig()
	config.Level = zapcore.WarnLevel
	if VerifySeriesFlags.verbose {
		config.Level = zapcore.InfoLevel
	}
	logger, err := config.New(os.Stderr)
	if err != nil {
		return err
	}

	v := tsdb.NewVerify()
	v.Logger = logger
	v.Recover = VerifySeriesFlags._recover
	v.Concurrent = VerifySeriesFlags.concurrent
	v.ContinueOnError = VerifySeriesFlags.continueError
	v.SeriesFilePath = VerifySeriesFlags.seriesFile

	if VerifySeriesFlags.measurementPrefixHex != "" {
		namePrefix, err := hex.DecodeString(VerifySeriesFlags.measurementPrefixHex)
		if err != nil {
			return err
		}
		v.MeasurementPrefix = namePrefix
	}
	_, err = v.VerifySeriesFile()
	return err
}
