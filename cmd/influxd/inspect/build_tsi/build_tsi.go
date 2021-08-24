package build_tsi

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"

	"go.uber.org/zap/zapcore"

	"github.com/influxdata/influx-cli/v2/clients"
	"github.com/influxdata/influx-cli/v2/pkg/stdio"
	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/file"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/influxdata/influxdb/v2/tsdb/index/tsi1"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const defaultBatchSize = 10000

type buildTSI struct {
	// Data path options
	dataPath string // optional. Defaults to <engine_path>/engine/data
	walPath  string // optional. Defaults to <engine_path>/engine/wal
	bucketID string // optional. Defaults to all buckets
	shardID  string // optional. Defaults to all shards

	batchSize         int    // optional. Defaults to 10000
	maxLogFileSize    int64  // optional. Defaults to tsdb.DefaultMaxIndexLogFileSize
	maxCacheSize      uint64 // optional. Defaults to tsdb.DefaultCacheMaxMemorySize
	compactSeriesFile bool   // optional. Defaults to false
	concurrency       int    // optional. Defaults to GOMAXPROCS(0)

	verbose bool // optional. Defaults to false
	Logger  *zap.Logger
}

// NewBuildTSICommand returns a new instance of Command with default settings applied.
func NewBuildTSICommand() *cobra.Command {
	var buildTSICmd buildTSI

	cmd := &cobra.Command{
		Use:   "build-tsi",
		Short: "Rebuilds the TSI index and (where necessary) the Series File.",
		Long: `This command will rebuild the TSI index and if needed the Series File.

The index is built by reading all of the TSM indexes in the TSM data 
directory, and all of the WAL entries in the WAL data directory. If the 
Series File directory is missing, then the series file will be rebuilt.
If the TSI index directory already exists, then this tool will fail.
Performance of the tool can be tweaked by adjusting the max log file size,
max cache file size and the batch size.
		
max-log-file-size determines how big in-memory parts of the index have to
get before they're compacted into memory-mappable index files. 
Consider decreasing this from the default if you find the heap 
requirements of your TSI index are too much.

max-cache-size refers to the maximum cache size allowed. If there are WAL
files to index, then they need to be replayed into a tsm1.Cache first
by this tool. If the maximum cache size isn't large enough then there
will be an error and this tool will fail. Increase max-cache-size to
address this.

batch-size refers to the size of the batches written into the index. 
Increasing this can improve performance but can result in much more
memory usage.
		`,
		Args: cobra.MaximumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			if buildTSICmd.shardID != "" && buildTSICmd.bucketID == "" {
				return errors.New("if shard-id is specified, bucket-id must also be specified")
			}

			config := logger.NewConfig()

			// Set logger level based on verbose flag
			if buildTSICmd.verbose {
				config.Level = zapcore.DebugLevel
			} else {
				config.Level = zapcore.InfoLevel
			}

			newLogger, err := config.New(cmd.OutOrStdout())
			if err != nil {
				return err
			}
			buildTSICmd.Logger = newLogger

			return buildTSICmd.run()
		},
	}

	defaultPath := filepath.Join(os.Getenv("HOME"), "/.influxdbv2/engine/")
	defaultDataPath := filepath.Join(defaultPath, "data")
	defaultWALPath := filepath.Join(defaultPath, "wal")

	cmd.Flags().StringVar(&buildTSICmd.dataPath, "data-path", defaultDataPath, "Path to the TSM data directory.")
	cmd.Flags().StringVar(&buildTSICmd.walPath, "wal-path", defaultWALPath, "Path to the WAL data directory.")
	cmd.Flags().StringVar(&buildTSICmd.bucketID, "bucket-id", "", "Bucket ID")
	cmd.Flags().StringVar(&buildTSICmd.shardID, "shard-id", "", "Shard ID, if this is specified a bucket-id must also be specified")
	cmd.Flags().BoolVar(&buildTSICmd.compactSeriesFile, "compact-series-file", false, "Compact existing series file. Does not rebuilt index.")
	cmd.Flags().IntVarP(&buildTSICmd.concurrency, "concurrency", "c", runtime.GOMAXPROCS(0), "Number of workers to dedicate to shard index building.")
	cmd.Flags().Int64Var(&buildTSICmd.maxLogFileSize, "max-log-file-size", tsdb.DefaultMaxIndexLogFileSize, "Maximum log file size")
	cmd.Flags().Uint64Var(&buildTSICmd.maxCacheSize, "max-cache-size", tsdb.DefaultCacheMaxMemorySize, "Maximum cache size")
	cmd.Flags().BoolVarP(&buildTSICmd.verbose, "verbose", "v", false, "Verbose output, includes debug-level logs")
	cmd.Flags().IntVar(&buildTSICmd.batchSize, "batch-size", defaultBatchSize, "Set the size of the batches we write to the index. Setting this can have adverse affects on performance and heap requirements")

	return cmd
}

// Run executes the run command for BuildTSI.
func (buildTSICmd *buildTSI) run() error {
	// Verify the user actually wants to run as root.
	if isRoot() {
		cli := clients.CLI{StdIO: stdio.TerminalStdio}
		if confirmed := cli.StdIO.GetConfirm(`
You are currently running as root. This will build your
index files with root ownership and will be inaccessible
if you run influxd as a non-root user. You should run
build-tsi as the same user you are running influxd.
Are you sure you want to continue?`); !confirmed {
			return errors.New("operation aborted")
		}
	}

	if buildTSICmd.compactSeriesFile {
		if buildTSICmd.shardID != "" {
			return errors.New("cannot specify shard ID when compacting series file")
		}
	}

	fis, err := os.ReadDir(buildTSICmd.dataPath)
	if err != nil {
		return err
	}
	for _, fi := range fis {
		name := fi.Name()
		if !fi.IsDir() {
			continue
		} else if buildTSICmd.bucketID != "" && name != buildTSICmd.bucketID {
			continue
		}

		if buildTSICmd.compactSeriesFile {
			if err := buildTSICmd.compactBucketSeriesFile(filepath.Join(buildTSICmd.dataPath, name)); err != nil {
				return err
			}
			continue
		}

		if err := buildTSICmd.processBucket(name, filepath.Join(buildTSICmd.dataPath, name), filepath.Join(buildTSICmd.walPath, name)); err != nil {
			return err
		}
	}

	return nil

}

// compactBucketSeriesFile compacts the series file segments associated with
// the series file for the provided bucket.
func (buildTSICmd *buildTSI) compactBucketSeriesFile(path string) error {
	sfilePath := filepath.Join(path, tsdb.SeriesFileDirectory)
	paths, err := buildTSICmd.seriesFilePartitionPaths(sfilePath)
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
	for i := 0; i < buildTSICmd.concurrency; i++ {
		g.Go(func() error {
			for path := range pathCh {
				if err := buildTSICmd.compactSeriesFilePartition(path); err != nil {
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
	err = sfile.Open()
	defer sfile.Close()

	if err != nil {
		return err
	}

	compactor := tsdb.NewSeriesPartitionCompactor()
	for _, partition := range sfile.Partitions() {
		if err = compactor.Compact(partition); err != nil {
			return err
		}
		buildTSICmd.Logger.Debug("Compacted", zap.String("path", partition.Path()))
	}
	return nil
}

func (buildTSICmd *buildTSI) compactSeriesFilePartition(path string) error {
	const tmpExt = ".tmp"
	buildTSICmd.Logger.Info("Processing partition", zap.String("path", path))

	// Open partition so index can recover from entries not in the snapshot.
	partitionID, err := strconv.Atoi(filepath.Base(path))
	if err != nil {
		return fmt.Errorf("cannot parse partition id from path: %s", path)
	}
	p := tsdb.NewSeriesPartition(partitionID, path, nil)
	if err := p.Open(); err != nil {
		return fmt.Errorf("cannot open partition: path=%s err=%w", path, err)
	}
	defer p.Close()

	// Loop over segments and compact.
	indexPath := p.IndexPath()
	var segmentPaths []string
	for _, segment := range p.Segments() {
		buildTSICmd.Logger.Debug("Processing segment", zap.String("path", segment.Path()), zap.Uint16("segment-id", segment.ID()))

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

		buildTSICmd.Logger.Debug("Renaming new segment", zap.String("prev", src), zap.String("new", dst))
		if err = file.RenameFile(src, dst); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("serious failure. Please rebuild index and series file: %w", err)
		}
	}

	// Remove index file so it will be rebuilt when reopened.
	buildTSICmd.Logger.Debug("Removing index file", zap.String("path", indexPath))

	if err = os.Remove(indexPath); err != nil && !os.IsNotExist(err) { // index won't exist for low cardinality
		return err
	}

	return nil
}

// seriesFilePartitionPaths returns the paths to each partition in the series file.
func (buildTSICmd *buildTSI) seriesFilePartitionPaths(path string) ([]string, error) {
	sfile := tsdb.NewSeriesFile(path)
	sfile.Logger = buildTSICmd.Logger
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

func (buildTSICmd *buildTSI) processBucket(bucketID, dataDir, walDir string) error {
	buildTSICmd.Logger.Info("Rebuilding bucket", zap.String("name", bucketID))

	sfile := tsdb.NewSeriesFile(filepath.Join(dataDir, tsdb.SeriesFileDirectory))
	sfile.Logger = buildTSICmd.Logger
	if err := sfile.Open(); err != nil {
		return err
	}
	defer sfile.Close()

	fis, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		rpName := fi.Name()
		if !fi.IsDir() {
			continue
		} else if rpName == tsdb.SeriesFileDirectory {
			continue
		}

		if err := buildTSICmd.processRetentionPolicy(sfile, bucketID, rpName, filepath.Join(dataDir, rpName), filepath.Join(walDir, rpName)); err != nil {
			return err
		}
	}

	return nil
}

func (buildTSICmd *buildTSI) processRetentionPolicy(sfile *tsdb.SeriesFile, bucketID, rpName, dataDir, walDir string) error {
	buildTSICmd.Logger.Info("Rebuilding retention policy", logger.Database(bucketID), logger.RetentionPolicy(rpName))

	fis, err := os.ReadDir(dataDir)
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
		} else if buildTSICmd.shardID != "" && fi.Name() != buildTSICmd.shardID {
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
	for k := 0; k < buildTSICmd.concurrency; k++ {
		go func() {
			for {
				i := int(atomic.AddUint32(&maxi, 1) - 1) // Get next partition to work on.
				if i >= len(shards) {
					return // No more work.
				}

				id, name := shards[i].ID, shards[i].Path
				log := buildTSICmd.Logger.With(logger.Database(bucketID), logger.RetentionPolicy(rpName), logger.Shard(id))
				errC <- IndexShard(sfile, filepath.Join(dataDir, name), filepath.Join(walDir, name), buildTSICmd.maxLogFileSize, buildTSICmd.maxCacheSize, buildTSICmd.batchSize, log)
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

func IndexShard(sfile *tsdb.SeriesFile, dataDir, walDir string, maxLogFileSize int64, maxCacheSize uint64, batchSize int, log *zap.Logger) error {
	log.Debug("Rebuilding shard")

	// Check if shard already has a TSI index.
	indexPath := filepath.Join(dataDir, "index")
	log.Debug("Checking index path", zap.String("path", indexPath))
	if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
		log.Warn("tsi1 index already exists, skipping", zap.String("path", indexPath))
		return nil
	}

	log.Debug("Opening shard")

	// Remove temporary index files if this is being re-run.
	tmpPath := filepath.Join(dataDir, ".index")
	log.Debug("Cleaning up partial index from previous run, if any")
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

	log.Debug("Opening tsi index in temporary location", zap.String("path", tmpPath))
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

	log.Debug("Iterating over tsm files")
	for _, path := range tsmPaths {
		log.Debug("Processing tsm file", zap.String("path", path))
		if err := IndexTSMFile(tsiIndex, path, batchSize, log); err != nil {
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
		log.Debug("Building cache from wal files")
		cache := tsm1.NewCache(maxCacheSize)
		loader := tsm1.NewCacheLoader(walPaths)
		loader.WithLogger(log)
		if err := loader.Load(cache); err != nil {
			return err
		}

		log.Debug("Iterating over cache")
		keysBatch := make([][]byte, 0, batchSize)
		namesBatch := make([][]byte, 0, batchSize)
		tagsBatch := make([]models.Tags, 0, batchSize)

		for _, key := range cache.Keys() {
			seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
			name, tags := models.ParseKeyBytes(seriesKey)

			log.Debug("Series", zap.String("name", string(name)), zap.String("tags", tags.String()))

			keysBatch = append(keysBatch, seriesKey)
			namesBatch = append(namesBatch, name)
			tagsBatch = append(tagsBatch, tags)

			// Flush batch?
			if len(keysBatch) == batchSize {
				if err := tsiIndex.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch); err != nil {
					return fmt.Errorf("problem creating series: %w", err)
				}
				keysBatch = keysBatch[:0]
				namesBatch = namesBatch[:0]
				tagsBatch = tagsBatch[:0]
			}
		}

		// Flush any remaining series in the batches
		if len(keysBatch) > 0 {
			if err := tsiIndex.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch); err != nil {
				return fmt.Errorf("problem creating series: %w", err)
			}
			keysBatch = nil
			namesBatch = nil
			tagsBatch = nil
		}
	}

	// Attempt to compact the index & wait for all compactions to complete.
	log.Debug("compacting index")
	tsiIndex.Compact()
	tsiIndex.Wait()

	// Close TSI index.
	log.Debug("Closing tsi index")
	if err := tsiIndex.Close(); err != nil {
		return err
	}

	// Rename TSI to standard path.
	log.Debug("Moving tsi to permanent location")
	return os.Rename(tmpPath, indexPath)
}

func IndexTSMFile(index *tsi1.Index, path string, batchSize int, log *zap.Logger) error {
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

		log.Debug("Series", zap.String("name", string(name)), zap.String("tags", tagsBatch[ti].String()))

		keysBatch = append(keysBatch, seriesKey)
		namesBatch = append(namesBatch, name)
		ti++

		// Flush batch?
		if len(keysBatch) == batchSize {
			if err := index.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch[:ti]); err != nil {
				return fmt.Errorf("problem creating series: %w", err)
			}
			keysBatch = keysBatch[:0]
			namesBatch = namesBatch[:0]
			ti = 0 // Reset tags.
		}
	}

	// Flush any remaining series in the batches
	if len(keysBatch) > 0 {
		if err := index.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch[:ti]); err != nil {
			return fmt.Errorf("problem creating series: %w", err)
		}
	}
	return nil
}

func collectTSMFiles(path string) ([]string, error) {
	fis, err := os.ReadDir(path)
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
	fis, err := os.ReadDir(path)
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
	currUser, _ := user.Current()
	return currUser != nil && currUser.Username == "root"
}
