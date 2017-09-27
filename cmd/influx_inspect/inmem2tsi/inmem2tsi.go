// Package inmem2tsi reads an in-memory index and exports it as a TSI index.
package inmem2tsi

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
	"github.com/uber-go/zap"
)

// Command represents the program execution for "influx_inspect inmem2tsi".
type Command struct {
	Stderr io.Writer
	Stdout io.Writer

	Verbose bool
	Logger  zap.Logger
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
		Logger: zap.New(zap.NullEncoder()),
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	fs := flag.NewFlagSet("inmem2tsi", flag.ExitOnError)
	dataDir := fs.String("datadir", "", "shard data directory")
	walDir := fs.String("waldir", "", "shard WAL directory")
	fs.BoolVar(&cmd.Verbose, "v", false, "verbose")
	fs.SetOutput(cmd.Stdout)
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() > 0 || *dataDir == "" || *walDir == "" {
		return flag.ErrHelp
	}

	cmd.Logger = zap.New(
		zap.NewTextEncoder(),
		zap.Output(os.Stderr),
	)

	return cmd.run(*dataDir, *walDir)
}

func (cmd *Command) run(dataDir, walDir string) error {
	// Check if shard already has a TSI index.
	indexPath := filepath.Join(dataDir, "index")
	cmd.Logger.Info("checking index path", zap.String("path", indexPath))
	if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
		return errors.New("tsi1 index already exists")
	}

	cmd.Logger.Info("opening shard",
		zap.String("datadir", dataDir),
		zap.String("waldir", walDir),
	)

	// Find shard files.
	tsmPaths, err := cmd.collectTSMFiles(dataDir)
	if err != nil {
		return err
	}
	walPaths, err := cmd.collectWALFiles(walDir)
	if err != nil {
		return err
	}

	// Remove temporary index files if this is being re-run.
	tmpPath := filepath.Join(dataDir, ".index")
	cmd.Logger.Info("cleaning up partial index from previous run, if any")
	if err := os.RemoveAll(tmpPath); err != nil {
		return err
	}

	// Open TSI index in temporary path.
	tsiIndex := tsi1.NewIndex(
		tsi1.WithPath(tmpPath),
		tsi1.WithLogger(cmd.Logger),
	)
	cmd.Logger.Info("opening tsi index in temporary location", zap.String("path", tmpPath))
	if err := tsiIndex.Open(); err != nil {
		return err
	}
	defer tsiIndex.Close()

	// Write out tsm1 files.
	cmd.Logger.Info("iterating over tsm files")
	for _, path := range tsmPaths {
		cmd.Logger.Info("processing tsm file", zap.String("path", path))
		if err := cmd.processTSMFile(tsiIndex, path); err != nil {
			return err
		}
	}

	// Write out wal files.
	cmd.Logger.Info("building cache from wal files")
	cache := tsm1.NewCache(tsdb.DefaultCacheMaxMemorySize, "")
	loader := tsm1.NewCacheLoader(walPaths)
	loader.WithLogger(cmd.Logger)
	if err := loader.Load(cache); err != nil {
		return err
	}

	cmd.Logger.Info("iterating over cache")
	for _, key := range cache.Keys() {
		seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
		name, tags := models.ParseKey(seriesKey)

		if cmd.Verbose {
			cmd.Logger.Info("series", zap.String("name", string(name)), zap.String("tags", tags.String()))
		}

		if err := tsiIndex.CreateSeriesIfNotExists(nil, []byte(name), tags); err != nil {
			return fmt.Errorf("cannot create series: %s %s (%s)", name, tags.String(), err)
		}
	}

	// Attempt to compact the index & wait for all compactions to complete.
	cmd.Logger.Info("compacting index")
	tsiIndex.Compact()
	tsiIndex.Wait()

	// Close TSI index.
	cmd.Logger.Info("closing tsi index")
	if err := tsiIndex.Close(); err != nil {
		return err
	}

	// Rename TSI to standard path.
	cmd.Logger.Info("moving tsi to permanent location")
	if err := os.Rename(tmpPath, indexPath); err != nil {
		return err
	}

	return nil
}

func (cmd *Command) processTSMFile(index *tsi1.Index, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		cmd.Logger.Warn("unable to read, skipping", zap.String("path", path), zap.Error(err))
		return nil
	}
	defer r.Close()

	for i := 0; i < r.KeyCount(); i++ {
		key, _ := r.KeyAt(i)
		seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
		name, tags := models.ParseKey(seriesKey)

		if cmd.Verbose {
			cmd.Logger.Info("series", zap.String("name", string(name)), zap.String("tags", tags.String()))
		}

		if err := index.CreateSeriesIfNotExists(nil, []byte(name), tags); err != nil {
			return fmt.Errorf("cannot create series: %s %s (%s)", name, tags.String(), err)
		}
	}
	return nil
}

func (cmd *Command) collectTSMFiles(path string) ([]string, error) {
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var paths []string
	for _, fi := range fis {
		if filepath.Ext(fi.Name()) != "."+tsm1.TSMFileExtension {
			continue
		}
		paths = append(paths, filepath.Join(path, fi.Name()))
	}
	return paths, nil
}

func (cmd *Command) collectWALFiles(path string) ([]string, error) {
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var paths []string
	for _, fi := range fis {
		if filepath.Ext(fi.Name()) != "."+tsm1.WALFileExtension {
			continue
		}
		paths = append(paths, filepath.Join(path, fi.Name()))
	}
	return paths, nil
}

func (cmd *Command) printUsage() {
	usage := `Converts a shard from an in-memory index to a TSI index.

Usage: influx_inspect inmem2tsi [-v] -datadir DATADIR -waldir WALDIR
`

	fmt.Fprintf(cmd.Stdout, usage)
}
