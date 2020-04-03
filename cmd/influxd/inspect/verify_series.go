package inspect

import (
	"os"
	"runtime"

	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/tsdb/seriesfile"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"
)

// NewVerifySeriesFileCommand returns a new instance of verifySeriesCommand
// for execution of "influx_inspect verify-seriesfile".
func NewVerifySeriesFileCommand() *cobra.Command {
	verifySeriesCommand := &cobra.Command{
		Use:   "verify-seriesfile",
		Short: "Verifies the integrity of Series files",
		Long: `Verifies the integrity of Series files.
		Usage: influx_inspect verify-seriesfile [flags]
			--series-file <path>
					Path to a series file. This defaults to ` + os.Getenv("HOME") + `/.influxdbv2/engine/_series.
			--v
					Enable verbose logging.
			--c
					How many concurrent workers to run.
					Defaults to "` + string(runtime.GOMAXPROCS(0)) + `" on this machine.`,
		RunE: verifySeriesRun,
	}

	verifySeriesCommand.Flags().StringVar(&VerifySeriesFlags.seriesFile, "series-file", os.Getenv("HOME")+"/.influxdbv2/engine/_series",
		"Path to a series file. This defaults to "+os.Getenv("HOME")+"/.influxdbv2/engine/_series")
	verifySeriesCommand.Flags().BoolVarP(&VerifySeriesFlags.verbose, "v", "v", false,
		"Verbose output.")
	verifySeriesCommand.Flags().IntVarP(&VerifySeriesFlags.concurrent, "c", "c", runtime.GOMAXPROCS(0),
		"How many concurrent workers to run.")

	return verifySeriesCommand
}

var VerifySeriesFlags = struct {
	seriesFile string
	verbose    bool
	concurrent int
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

	v := seriesfile.NewVerify()
	v.Logger = logger
	v.Concurrent = VerifySeriesFlags.concurrent

	if VerifySeriesFlags.seriesFile != "" {
		_, err := v.VerifySeriesFile(VerifySeriesFlags.seriesFile)
		return err
	}

	return nil
}
