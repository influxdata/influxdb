// Package buildtsi reads an in-memory index and exports it as a TSI index.
package buildtsi

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
	"go.uber.org/zap"
)

const defaultBatchSize = 10000

// Command represents the program execution for "influx_inspect buildtsi".
type Command struct {
	Stderr  io.Writer
	Stdout  io.Writer
	Verbose bool
	Logger  *zap.Logger

	databaseFilter  string
	retentionFilter string
	shardFilter     string
	maxLogFileSize  int64
	batchSize       int
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr:    os.Stderr,
		Stdout:    os.Stdout,
		Logger:    zap.NewNop(),
		batchSize: defaultBatchSize,
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	fs := flag.NewFlagSet("buildtsi", flag.ExitOnError)
	dataDir := fs.String("datadir", "", "data directory")
	walDir := fs.String("waldir", "", "WAL directory")
	fs.StringVar(&cmd.databaseFilter, "database", "", "optional: database name")
	fs.StringVar(&cmd.retentionFilter, "retention", "", "optional: retention policy")
	fs.StringVar(&cmd.shardFilter, "shard", "", "optional: shard id")
	fs.Int64Var(&cmd.maxLogFileSize, "max-log-file-size", tsdb.DefaultMaxIndexLogFileSize, "optional: maximum log file size")
	fs.IntVar(&cmd.batchSize, "batch-size", defaultBatchSize, "optional: set the size of the batches we write to the index. Setting this can have adverse affects on performance and heap requirements")
	fs.BoolVar(&cmd.Verbose, "v", false, "verbose")
	fs.SetOutput(cmd.Stdout)
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() > 0 || *dataDir == "" || *walDir == "" {
		fs.Usage()
		return nil
	}
	cmd.Logger = logger.New(cmd.Stderr)

	finish := startProfiles()
	defer finish()
	return cmd.run(*dataDir, *walDir)
}

func startProfiles() func() {
	runtime.MemProfileRate = 100 // Sample 1% of allocations.

	paths := []string{"/tmp/buildtsi.mem.pprof", "/tmp/buildtsi.cpu.pprof"}
	var files []*os.File
	for _, pth := range paths {
		f, err := os.Create(pth)
		if err != nil {
			log.Fatalf("memprofile: %v", err)
		}
		log.Printf("writing profile to: %s\n", pth)
		files = append(files, f)

	}

	closeFn := func() {
		// Write the memory profile
		if err := pprof.Lookup("heap").WriteTo(files[0], 0); err != nil {
			panic(err)
		}

		// Stop the CPU profile.
		pprof.StopCPUProfile()

		for _, fd := range files {
			if err := fd.Close(); err != nil {
				panic(err)
			}
		}
	}

	if err := pprof.StartCPUProfile(files[1]); err != nil {
		panic(err)
	}
	return closeFn
}

func (cmd *Command) run(dataDir, walDir string) error {
	// Verify the user actually wants to run as root.
	if isRoot() {
		fmt.Println("You are currently running as root. This will build your")
		fmt.Println("index files with root ownership and will be inaccessible")
		fmt.Println("if you run influxd as a non-root user. You should run")
		fmt.Println("buildtsi as the same user you are running influxd.")
		fmt.Print("Are you sure you want to continue? (y/N): ")
		var answer string
		if fmt.Scanln(&answer); !strings.HasPrefix(strings.TrimSpace(strings.ToLower(answer)), "y") {
			return fmt.Errorf("operation aborted")
		}
	}

	fis, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		name := fi.Name()
		if !fi.IsDir() {
			continue
		} else if cmd.databaseFilter != "" && name != cmd.databaseFilter {
			continue
		}

		if err := cmd.processDatabase(name, filepath.Join(dataDir, name), filepath.Join(walDir, name)); err != nil {
			return err
		}
	}

	return nil
}

func (cmd *Command) processDatabase(dbName, dataDir, walDir string) error {
	cmd.Logger.Info("Rebuilding database", zap.String("name", dbName))

	sfile := tsdb.NewSeriesFile(filepath.Join(dataDir, tsdb.SeriesFileDirectory))
	sfile.Logger = cmd.Logger
	if err := sfile.Open(); err != nil {
		return err
	}
	defer sfile.Close()

	fis, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		rpName := fi.Name()
		if !fi.IsDir() {
			continue
		} else if rpName == tsdb.SeriesFileDirectory {
			continue
		} else if cmd.retentionFilter != "" && rpName != cmd.retentionFilter {
			continue
		}

		if err := cmd.processRetentionPolicy(sfile, dbName, rpName, filepath.Join(dataDir, rpName), filepath.Join(walDir, rpName)); err != nil {
			return err
		}
	}

	return nil
}

func (cmd *Command) processRetentionPolicy(sfile *tsdb.SeriesFile, dbName, rpName, dataDir, walDir string) error {
	cmd.Logger.Info("Rebuilding retention policy", logger.Database(dbName), logger.RetentionPolicy(rpName))

	fis, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		if !fi.IsDir() {
			continue
		} else if cmd.shardFilter != "" && fi.Name() != cmd.shardFilter {
			continue
		}

		shardID, err := strconv.ParseUint(fi.Name(), 10, 64)
		if err != nil {
			continue
		}

		if err := cmd.processShard(sfile, dbName, rpName, shardID, filepath.Join(dataDir, fi.Name()), filepath.Join(walDir, fi.Name())); err != nil {
			return err
		}
	}
	return nil
}

func (cmd *Command) processShard(sfile *tsdb.SeriesFile, dbName, rpName string, shardID uint64, dataDir, walDir string) error {
	cmd.Logger.Info("Rebuilding shard", logger.Database(dbName), logger.RetentionPolicy(rpName), logger.Shard(shardID))

	// Check if shard already has a TSI index.
	indexPath := filepath.Join(dataDir, "index")
	cmd.Logger.Info("Checking index path", zap.String("path", indexPath))
	if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
		cmd.Logger.Info("tsi1 index already exists, skipping", zap.String("path", indexPath))
		return nil
	}

	cmd.Logger.Info("Opening shard")

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
	cmd.Logger.Info("Cleaning up partial index from previous run, if any")
	if err := os.RemoveAll(tmpPath); err != nil {
		return err
	}

	// Open TSI index in temporary path.
	tsiIndex := tsi1.NewIndex(sfile, dbName,
		tsi1.WithPath(tmpPath),
		tsi1.WithMaximumLogFileSize(cmd.maxLogFileSize),
		tsi1.DisableFsync())

	tsiIndex.WithLogger(cmd.Logger)

	cmd.Logger.Info("Opening tsi index in temporary location", zap.String("path", tmpPath))
	if err := tsiIndex.Open(); err != nil {
		return err
	}
	defer tsiIndex.Close()

	// Write out tsm1 files.
	cmd.Logger.Info("Iterating over tsm files")
	for _, path := range tsmPaths {
		cmd.Logger.Info("Processing tsm file", zap.String("path", path))
		if err := cmd.processTSMFile(tsiIndex, path); err != nil {
			return err
		}
	}

	// Write out wal files.
	cmd.Logger.Info("Building cache from wal files")
	cache := tsm1.NewCache(tsdb.DefaultCacheMaxMemorySize)
	loader := tsm1.NewCacheLoader(walPaths)
	loader.WithLogger(cmd.Logger)
	if err := loader.Load(cache); err != nil {
		return err
	}

	cmd.Logger.Info("Iterating over cache")
	keysBatch := make([][]byte, 0, cmd.batchSize)
	namesBatch := make([][]byte, 0, cmd.batchSize)
	tagsBatch := make([]models.Tags, 0, cmd.batchSize)

	for _, key := range cache.Keys() {
		seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
		name, tags := models.ParseKey(seriesKey)

		if cmd.Verbose {
			cmd.Logger.Info("Series", zap.String("name", string(name)), zap.String("tags", tags.String()))
		}

		keysBatch = append(keysBatch, seriesKey)
		namesBatch = append(namesBatch, []byte(name))
		tagsBatch = append(tagsBatch, tags)

		// Flush batch?
		if len(keysBatch) == cmd.batchSize {
			if err := tsiIndex.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch); err != nil {
				return fmt.Errorf("problem creating series: (%s)", err)
			}
			keysBatch = keysBatch[:0]
			namesBatch = namesBatch[:0]
			tagsBatch = tagsBatch[:0]
		}
	}

	// Flush any remaining series in the batches
	if len(keysBatch) > 0 {
		if err := tsiIndex.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch); err != nil {
			return fmt.Errorf("problem creating series: (%s)", err)
		}
		keysBatch = nil
		namesBatch = nil
		tagsBatch = nil
	}

	// Attempt to compact the index & wait for all compactions to complete.
	cmd.Logger.Info("compacting index")
	tsiIndex.Compact()
	tsiIndex.Wait()

	// Close TSI index.
	cmd.Logger.Info("Closing tsi index")
	if err := tsiIndex.Close(); err != nil {
		return err
	}

	// Rename TSI to standard path.
	cmd.Logger.Info("Moving tsi to permanent location")
	return os.Rename(tmpPath, indexPath)
}

func (cmd *Command) processTSMFile(index *tsi1.Index, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		cmd.Logger.Warn("Unable to read, skipping", zap.String("path", path), zap.Error(err))
		return nil
	}
	defer r.Close()

	keysBatch := make([][]byte, 0, cmd.batchSize)
	namesBatch := make([][]byte, 0, cmd.batchSize)
	tagsBatch := make([]models.Tags, 0, cmd.batchSize)

	for i := 0; i < r.KeyCount(); i++ {
		key, _ := r.KeyAt(i)
		seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
		name, tags := models.ParseKey(seriesKey)

		if cmd.Verbose {
			cmd.Logger.Info("Series", zap.String("name", string(name)), zap.String("tags", tags.String()))
		}

		keysBatch = append(keysBatch, seriesKey)
		namesBatch = append(namesBatch, []byte(name))
		tagsBatch = append(tagsBatch, tags)

		// Flush batch?
		if len(keysBatch) == cmd.batchSize {
			if err := index.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch); err != nil {
				return fmt.Errorf("problem creating series: (%s)", err)
			}
			keysBatch = keysBatch[:0]
			namesBatch = namesBatch[:0]
			tagsBatch = tagsBatch[:0]
		}
	}

	// Flush any remaining series in the batches
	if len(keysBatch) > 0 {
		if err := index.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch); err != nil {
			return fmt.Errorf("problem creating series: (%s)", err)
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

func isRoot() bool {
	user, _ := user.Current()
	return user != nil && user.Username == "root"
}
