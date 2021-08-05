// Package buildtsi reads an in-memory index and exports it as a TSI index.
package buildtsi

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/file"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const defaultBatchSize = 10000

// Command represents the program execution for "influx_inspect buildtsi".
type Command struct {
	Stderr  io.Writer
	Stdout  io.Writer
	Verbose bool
	Logger  *zap.Logger

	concurrency       int // Number of goroutines to dedicate to shard index building.
	databaseFilter    string
	retentionFilter   string
	shardFilter       string
	compactSeriesFile bool
	maxLogFileSize    int64
	maxCacheSize      uint64
	batchSize         int
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr:      os.Stderr,
		Stdout:      os.Stdout,
		Logger:      zap.NewNop(),
		batchSize:   defaultBatchSize,
		concurrency: runtime.GOMAXPROCS(0),
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	fs := flag.NewFlagSet("buildtsi", flag.ExitOnError)
	dataDir := fs.String("datadir", "", "data directory")
	walDir := fs.String("waldir", "", "WAL directory")
	fs.IntVar(&cmd.concurrency, "concurrency", runtime.GOMAXPROCS(0), "Number of workers to dedicate to shard index building. Defaults to GOMAXPROCS")
	fs.StringVar(&cmd.databaseFilter, "database", "", "optional: database name")
	fs.StringVar(&cmd.retentionFilter, "retention", "", "optional: retention policy")
	fs.StringVar(&cmd.shardFilter, "shard", "", "optional: shard id")
	fs.BoolVar(&cmd.compactSeriesFile, "compact-series-file", false, "optional: compact existing series file. Do not rebuilt index.")
	fs.Int64Var(&cmd.maxLogFileSize, "max-log-file-size", tsdb.DefaultMaxIndexLogFileSize, "optional: maximum log file size")
	fs.Uint64Var(&cmd.maxCacheSize, "max-cache-size", tsdb.DefaultCacheMaxMemorySize, "optional: maximum cache size")
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

	return cmd.run(*dataDir, *walDir)
}

func (cmd *Command) run(dataDir, walDir string) error {
	// Verify the user actually wants to run as root.
	if isRoot() {
		fmt.Fprintln(cmd.Stdout, "You are currently running as root. This will build your")
		fmt.Fprintln(cmd.Stdout, "index files with root ownership and will be inaccessible")
		fmt.Fprintln(cmd.Stdout, "if you run influxd as a non-root user. You should run")
		fmt.Fprintln(cmd.Stdout, "buildtsi as the same user you are running influxd.")
		fmt.Fprint(cmd.Stdout, "Are you sure you want to continue? (y/N): ")
		var answer string
		if fmt.Scanln(&answer); !strings.HasPrefix(strings.TrimSpace(strings.ToLower(answer)), "y") {
			return fmt.Errorf("operation aborted")
		}
	}

	if cmd.compactSeriesFile {
		if cmd.retentionFilter != "" {
			return errors.New("cannot specify retention policy when compacting series file")
		} else if cmd.shardFilter != "" {
			return errors.New("cannot specify shard ID when compacting series file")
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

		if cmd.compactSeriesFile {
			if err := cmd.compactDatabaseSeriesFile(name, filepath.Join(dataDir, name)); err != nil {
				return err
			}
			continue
		}

		if err := cmd.processDatabase(name, filepath.Join(dataDir, name), filepath.Join(walDir, name)); err != nil {
			return err
		}
	}

	return nil
}

// compactDatabaseSeriesFile compacts the series file segments associated with
// the series file for the provided database.
func (cmd *Command) compactDatabaseSeriesFile(dbName, path string) error {
	sfilePath := filepath.Join(path, tsdb.SeriesFileDirectory)
	paths, err := cmd.seriesFilePartitionPaths(sfilePath)
	if err != nil {
		return err
	}

	// Build input channel.
	pathCh := make(chan string, len(paths))
	for _, path := range paths {
		pathCh <- path
	}
	close(pathCh)

	// Concurrently process each partition in the series file
	var g errgroup.Group
	for i := 0; i < cmd.concurrency; i++ {
		g.Go(func() error {
			for path := range pathCh {
				if err := cmd.compactSeriesFilePartition(path); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Build new series file indexes
	sfile := tsdb.NewSeriesFile(sfilePath)
	if err = sfile.Open(); err != nil {
		return err
	}

	compactor := tsdb.NewSeriesPartitionCompactor()
	for _, partition := range sfile.Partitions() {
		if err = compactor.Compact(partition); err != nil {
			return err
		}
		fmt.Fprintln(cmd.Stdout, "compacted ", partition.Path())
	}
	return nil
}

func (cmd *Command) compactSeriesFilePartition(path string) error {
	const tmpExt = ".tmp"

	fmt.Fprintf(cmd.Stdout, "processing partition for %q\n", path)

	// Open partition so index can recover from entries not in the snapshot.
	partitionID, err := strconv.Atoi(filepath.Base(path))
	if err != nil {
		return fmt.Errorf("cannot parse partition id from path: %s", path)
	}
	p := tsdb.NewSeriesPartition(partitionID, path, nil)
	if err := p.Open(); err != nil {
		return fmt.Errorf("cannot open partition: path=%s err=%s", path, err)
	}
	defer p.Close()

	// Loop over segments and compact.
	indexPath := p.IndexPath()
	var segmentPaths []string
	for _, segment := range p.Segments() {
		fmt.Fprintf(cmd.Stdout, "processing segment %q %d\n", segment.Path(), segment.ID())

		if err := segment.CompactToPath(segment.Path()+tmpExt, p.Index()); err != nil {
			return err
		}
		segmentPaths = append(segmentPaths, segment.Path())
	}

	// Close partition.
	if err := p.Close(); err != nil {
		return err
	}

	// Remove the old segment files and replace with new ones.
	for _, dst := range segmentPaths {
		src := dst + tmpExt

		fmt.Fprintf(cmd.Stdout, "renaming new segment %q to %q\n", src, dst)
		if err = file.RenameFile(src, dst); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("serious failure. Please rebuild index and series file: %v", err)
		}
	}

	// Remove index file so it will be rebuilt when reopened.
	fmt.Fprintln(cmd.Stdout, "removing index file", indexPath)
	if err = os.Remove(indexPath); err != nil && !os.IsNotExist(err) { // index won't exist for low cardinality
		return err
	}

	return nil
}

// seriesFilePartitionPaths returns the paths to each partition in the series file.
func (cmd *Command) seriesFilePartitionPaths(path string) ([]string, error) {
	sfile := tsdb.NewSeriesFile(path)
	sfile.Logger = cmd.Logger
	if err := sfile.Open(); err != nil {
		return nil, err
	}

	var paths []string
	for _, partition := range sfile.Partitions() {
		paths = append(paths, partition.Path())
	}
	if err := sfile.Close(); err != nil {
		return nil, err
	}
	return paths, nil
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

	type shard struct {
		ID   uint64
		Path string
	}

	var shards []shard

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

		shards = append(shards, shard{shardID, fi.Name()})
	}

	errC := make(chan error, len(shards))
	var maxi uint32 // index of maximum shard being worked on.
	for k := 0; k < cmd.concurrency; k++ {
		go func() {
			for {
				i := int(atomic.AddUint32(&maxi, 1) - 1) // Get next partition to work on.
				if i >= len(shards) {
					return // No more work.
				}

				id, name := shards[i].ID, shards[i].Path
				log := cmd.Logger.With(logger.Database(dbName), logger.RetentionPolicy(rpName), logger.Shard(id))
				errC <- IndexShard(sfile, filepath.Join(dataDir, name), filepath.Join(walDir, name), cmd.maxLogFileSize, cmd.maxCacheSize, cmd.batchSize, log, cmd.Verbose)
			}
		}()
	}

	// Check for error
	for i := 0; i < cap(errC); i++ {
		if err := <-errC; err != nil {
			return err
		}
	}
	return nil
}

func IndexShard(sfile *tsdb.SeriesFile, dataDir, walDir string, maxLogFileSize int64, maxCacheSize uint64, batchSize int, log *zap.Logger, verboseLogging bool) error {
	log.Info("Rebuilding shard")

	// Check if shard already has a TSI index.
	indexPath := filepath.Join(dataDir, "index")
	log.Info("Checking index path", zap.String("path", indexPath))
	if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
		log.Info("tsi1 index already exists, skipping", zap.String("path", indexPath))
		return nil
	}

	log.Info("Opening shard")

	// Remove temporary index files if this is being re-run.
	tmpPath := filepath.Join(dataDir, ".index")
	log.Info("Cleaning up partial index from previous run, if any")
	if err := os.RemoveAll(tmpPath); err != nil {
		return err
	}

	// Open TSI index in temporary path.
	tsiIndex := tsi1.NewIndex(sfile, "",
		tsi1.WithPath(tmpPath),
		tsi1.WithMaximumLogFileSize(maxLogFileSize),
		tsi1.DisableFsync(),
		// Each new series entry in a log file is ~12 bytes so this should
		// roughly equate to one flush to the file for every batch.
		tsi1.WithLogFileBufferSize(12*batchSize),
	)

	tsiIndex.WithLogger(log)

	log.Info("Opening tsi index in temporary location", zap.String("path", tmpPath))
	if err := tsiIndex.Open(); err != nil {
		return err
	}
	defer tsiIndex.Close()

	// Write out tsm1 files.
	// Find shard files.
	tsmPaths, err := collectTSMFiles(dataDir)
	if err != nil {
		return err
	}

	log.Info("Iterating over tsm files")
	for _, path := range tsmPaths {
		log.Info("Processing tsm file", zap.String("path", path))
		if err := IndexTSMFile(tsiIndex, path, batchSize, log, verboseLogging); err != nil {
			return err
		}
	}

	// Write out wal files.
	walPaths, err := collectWALFiles(walDir)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}

	} else {
		log.Info("Building cache from wal files")
		cache := tsm1.NewCache(maxCacheSize)
		loader := tsm1.NewCacheLoader(walPaths)
		loader.WithLogger(log)
		if err := loader.Load(cache); err != nil {
			return err
		}

		log.Info("Iterating over cache")
		keysBatch := make([][]byte, 0, batchSize)
		namesBatch := make([][]byte, 0, batchSize)
		tagsBatch := make([]models.Tags, 0, batchSize)

		for _, key := range cache.Keys() {
			seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
			name, tags := models.ParseKeyBytes(seriesKey)

			if verboseLogging {
				log.Info("Series", zap.String("name", string(name)), zap.String("tags", tags.String()))
			}

			keysBatch = append(keysBatch, seriesKey)
			namesBatch = append(namesBatch, name)
			tagsBatch = append(tagsBatch, tags)

			// Flush batch?
			if len(keysBatch) == batchSize {
				if err := tsiIndex.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch, tsdb.NoopStatsTracker()); err != nil {
					return fmt.Errorf("problem creating series: (%s)", err)
				}
				keysBatch = keysBatch[:0]
				namesBatch = namesBatch[:0]
				tagsBatch = tagsBatch[:0]
			}
		}

		// Flush any remaining series in the batches
		if len(keysBatch) > 0 {
			if err := tsiIndex.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch, tsdb.NoopStatsTracker()); err != nil {
				return fmt.Errorf("problem creating series: (%s)", err)
			}
			keysBatch = nil
			namesBatch = nil
			tagsBatch = nil
		}
	}

	// Attempt to compact the index & wait for all compactions to complete.
	log.Info("compacting index")
	tsiIndex.Compact()
	tsiIndex.Wait()

	// Close TSI index.
	log.Info("Closing tsi index")
	if err := tsiIndex.Close(); err != nil {
		return err
	}

	log.Info("Reopening TSI index with max-index-log-file-size=1 to fully compact log files")
	compactingIndex := tsi1.NewIndex(sfile, "",
		tsi1.WithPath(tmpPath),
		tsi1.WithMaximumLogFileSize(1),
	)
	if err := compactingIndex.Open(); err != nil {
		return err
	}
	compactingIndex.Compact()
	compactingIndex.Wait()
	// Close TSI index.
	log.Info("re-closing tsi index")
	if err := compactingIndex.Close(); err != nil {
		return err
	}

	// Rename TSI to standard path.
	log.Info("Moving tsi to permanent location")
	return os.Rename(tmpPath, indexPath)
}

func IndexTSMFile(index *tsi1.Index, path string, batchSize int, log *zap.Logger, verboseLogging bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		log.Warn("Unable to read, skipping", zap.String("path", path), zap.Error(err))
		return nil
	}
	defer r.Close()

	keysBatch := make([][]byte, 0, batchSize)
	namesBatch := make([][]byte, 0, batchSize)
	tagsBatch := make([]models.Tags, batchSize)
	var ti int
	for i := 0; i < r.KeyCount(); i++ {
		key, _ := r.KeyAt(i)
		seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
		var name []byte
		name, tagsBatch[ti] = models.ParseKeyBytesWithTags(seriesKey, tagsBatch[ti])

		if verboseLogging {
			log.Info("Series", zap.String("name", string(name)), zap.String("tags", tagsBatch[ti].String()))
		}

		keysBatch = append(keysBatch, seriesKey)
		namesBatch = append(namesBatch, name)
		ti++

		// Flush batch?
		if len(keysBatch) == batchSize {
			if err := index.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch[:ti], tsdb.NoopStatsTracker()); err != nil {
				return fmt.Errorf("problem creating series: (%s)", err)
			}
			keysBatch = keysBatch[:0]
			namesBatch = namesBatch[:0]
			ti = 0 // Reset tags.
		}
	}

	// Flush any remaining series in the batches
	if len(keysBatch) > 0 {
		if err := index.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch[:ti], tsdb.NoopStatsTracker()); err != nil {
			return fmt.Errorf("problem creating series: (%s)", err)
		}
	}
	return nil
}

func collectTSMFiles(path string) ([]string, error) {
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

func collectWALFiles(path string) ([]string, error) {
	if path == "" {
		return nil, os.ErrNotExist
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, err
	}
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
