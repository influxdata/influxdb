// Package verify_seriesfile verifies integrity of series files.
package verify_seriesfile

import (
	"flag"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/influxdata/influxdb/logger"
	"go.uber.org/zap/zapcore"
)

// Command represents the program execution for "influx_inspect verify-seriesfile".
type Command struct {
	Stdout io.Writer
	Stderr io.Writer
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	fs := flag.NewFlagSet("verify-seriesfile", flag.ExitOnError)
	fs.SetOutput(cmd.Stdout)

	dataDir := fs.String("dir", filepath.Join(os.Getenv("HOME"), ".influxdb", "data"), "Data directory.")
	dbName := fs.String("db", "", "Only use this database inside of the data directory.")
	seriesFile := fs.String("series-file", "", "Path to a series file. This overrides -db and -dir.")
	verbose := fs.Bool("v", false, "Verbose output.")

	if err := fs.Parse(args); err != nil {
		return err
	}

	config := logger.NewConfig()
	if *verbose {
		config.Level = zapcore.DebugLevel
	}
	logger, err := config.New(cmd.Stderr)
	if err != nil {
		return err
	}

	if *seriesFile != "" {
		_, err := VerifySeriesFile(logger, *seriesFile)
		return err
	}

	if *dbName != "" {
		_, err := VerifySeriesFile(logger, filepath.Join(*dataDir, *dbName, "_series"))
		return err
	}

	dbs, err := ioutil.ReadDir(*dataDir)
	if err != nil {
		return err
	}

	for _, db := range dbs {
		if !db.IsDir() {
			continue
		}
		_, err := VerifySeriesFile(logger, filepath.Join(*dataDir, db.Name(), "_series"))
		if err != nil {
			return err
		}
	}

	return nil
}
