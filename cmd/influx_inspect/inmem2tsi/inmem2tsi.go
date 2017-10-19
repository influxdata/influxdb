// Package inmem2tsi reads an in-memory index and exports it as a TSI index.
package inmem2tsi

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/influxdata/influxdb/diagnostic"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/index/inmem"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
	tsi1diag "github.com/influxdata/influxdb/tsdb/index/tsi1/diagnostic"
	"go.uber.org/zap"
)

// Command represents the program execution for "influx_inspect inmem2tsi".
type Command struct {
	Stderr     io.Writer
	Stdout     io.Writer
	Diagnostic *diagnostic.Service
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	fs := flag.NewFlagSet("inmem2tsi", flag.ExitOnError)
	path := fs.String("path", "", "data path")
	walPath := fs.String("wal-path", "", "WAL path")
	verbose := fs.Bool("v", false, "verbose")
	fs.SetOutput(cmd.Stdout)
	fs.Usage = cmd.printUsage
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() > 0 || *path == "" || *walPath == "" {
		cmd.printUsage()
		return flag.ErrHelp
	}

	if *verbose {
		cmd.Diagnostic = diagnostic.New(os.Stderr)
	}

	return cmd.run(*path, *walPath, *verbose)
}

func (cmd *Command) run(path, walPath string, verbose bool) error {
	// Check if shard already has a TSI index.
	indexPath := filepath.Join(path, "index")
	cmd.Logger().Info("checking index path", zap.String("path", indexPath))
	if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
		return errors.New("tsi1 index already exists")
	}

	cmd.Logger().Info("opening shard", zap.String("path", path), zap.String("wal-path", walPath))

	// Open shard at path.
	sh := tsdb.NewShard(0, path, walPath, tsdb.EngineOptions{
		EngineVersion: tsdb.DefaultEngine,
		IndexVersion:  inmem.IndexName,
		InmemIndex:    inmem.NewIndex(""),
	})
	if err := sh.Open(); err != nil {
		return err
	}
	defer sh.CloseFast()

	cmd.Logger().Info("reading in-memory index")

	// Retrieve in-memory index reference.
	inmemIndex, ok := sh.Index().(*inmem.ShardIndex)
	if !ok {
		return fmt.Errorf("invalid source index type: %T", sh.Index())
	}

	// Remove temporary index files if this is being re-run.
	tmpPath := filepath.Join(path, ".index")
	cmd.Logger().Info("cleaning up partial index from previous run, if any")
	if err := os.RemoveAll(tmpPath); err != nil {
		return err
	}

	// Open TSI index in temporary path.
	tsiIndex := tsi1.NewIndex()
	tsiIndex.Path = tmpPath
	if ctx, ok := cmd.Diagnostic.StoreHandler().(interface {
		TSI1Handler() tsi1diag.Handler
	}); ok {
		tsiIndex.WithDiagnosticHandler(ctx.TSI1Handler())
	}
	cmd.Logger().Info("opening tsi index in temporary location", zap.String("path", tmpPath))
	if err := tsiIndex.Open(); err != nil {
		return err
	}
	defer tsiIndex.Close()

	cmd.Logger().Info("iterating over measurements")

	// Iterate over each series & insert into new index.
	if err := inmemIndex.ForEachMeasurementName(func(name []byte) error {
		cmd.Logger().Info("processing measurement", zap.String("name", string(name)))

		mm, err := inmemIndex.Measurement(name)
		if err != nil {
			return err
		} else if mm == nil {
			return nil
		}

		if err := mm.ForEachSeriesByExpr(nil, func(tags models.Tags) error {
			cmd.Logger().Info("series", zap.String("name", string(name)), zap.String("tags", tags.String()))
			if err := tsiIndex.CreateSeriesIfNotExists(nil, name, tags); err != nil {
				return fmt.Errorf("cannot create series: %s %s (%s)", name, tags.String(), err)
			}
			return nil
		}); err != nil {
			return err
		}
		return nil

	}); err != nil {
		return err
	}

	cmd.Logger().Info("compacting index")

	// Attempt to compact the index & wait for all compactions to complete.
	tsiIndex.Compact()
	tsiIndex.Wait()

	cmd.Logger().Info("closing tsi index")

	// Close TSI index.
	if err := tsiIndex.Close(); err != nil {
		return err
	}

	cmd.Logger().Info("moving tsi to permanent location")

	// Rename TSI to standard path.
	if err := os.Rename(tmpPath, indexPath); err != nil {
		return err
	}

	return nil
}

func (cmd *Command) printUsage() {
	usage := `Converts a shard from an in-memory index to a TSI index.

Usage: influx_inspect inmem2tsi -path DATA_PATH -wal-path WAL_PATH
`

	fmt.Fprintf(cmd.Stdout, usage)
}

func (cmd *Command) Logger() *zap.Logger {
	return cmd.Diagnostic.Logger()
}
