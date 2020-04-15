// Package buildtsi reads an in-memory index and exports it as a TSI index.
package buildtsi

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/fs"
	"github.com/influxdata/influxdb/v2/storage/wal"
	"github.com/influxdata/influxdb/v2/toml"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/seriesfile"
	"github.com/influxdata/influxdb/v2/tsdb/tsi1"
	"github.com/influxdata/influxdb/v2/tsdb/tsm1"
	"go.uber.org/zap"
)

func IndexShard(sfile *seriesfile.SeriesFile, indexPath, dataDir, walDir string, maxLogFileSize int64, maxCacheSize uint64, batchSize int, log *zap.Logger, verboseLogging bool) error {
	log.Info("Rebuilding shard")

	// Check if shard already has a TSI index.
	log.Info("Checking index path", zap.String("path", indexPath))
	if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
		log.Info("TSI1 index already exists, skipping", zap.String("path", indexPath))
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
	c := tsi1.NewConfig()
	c.MaxIndexLogFileSize = toml.Size(maxLogFileSize)

	tsiIndex := tsi1.NewIndex(sfile, c,
		tsi1.WithPath(tmpPath),
		tsi1.DisableFsync(),
		// Each new series entry in a log file is ~12 bytes so this should
		// roughly equate to one flush to the file for every batch.
		tsi1.WithLogFileBufferSize(12*batchSize),
		tsi1.DisableMetrics(), // Disable metrics when rebuilding an index
	)
	tsiIndex.WithLogger(log)

	log.Info("Opening tsi index in temporary location", zap.String("path", tmpPath))
	if err := tsiIndex.Open(context.Background()); err != nil {
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
		cache := tsm1.NewCache(uint64(tsm1.DefaultCacheMaxMemorySize))
		loader := tsm1.NewCacheLoader(walPaths)
		loader.WithLogger(log)
		if err := loader.Load(cache); err != nil {
			return err
		}

		log.Info("Iterating over cache")
		collection := &tsdb.SeriesCollection{
			Keys:  make([][]byte, 0, batchSize),
			Names: make([][]byte, 0, batchSize),
			Tags:  make([]models.Tags, 0, batchSize),
			Types: make([]models.FieldType, 0, batchSize),
		}

		for _, key := range cache.Keys() {
			seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
			name, tags := models.ParseKeyBytes(seriesKey)
			typ, _ := cache.Type(key)

			if verboseLogging {
				log.Info("Series", zap.String("name", string(name)), zap.String("tags", tags.String()))
			}

			collection.Keys = append(collection.Keys, seriesKey)
			collection.Names = append(collection.Names, name)
			collection.Tags = append(collection.Tags, tags)
			collection.Types = append(collection.Types, typ)

			// Flush batch?
			if collection.Length() == batchSize {
				if err := tsiIndex.CreateSeriesListIfNotExists(collection); err != nil {
					return fmt.Errorf("problem creating series: (%s)", err)
				}
				collection.Truncate(0)
			}
		}

		// Flush any remaining series in the batches
		if collection.Length() > 0 {
			if err := tsiIndex.CreateSeriesListIfNotExists(collection); err != nil {
				return fmt.Errorf("problem creating series: (%s)", err)
			}
			collection = nil
		}
	}

	// Attempt to compact the index & wait for all compactions to complete.
	log.Info("Compacting index")
	tsiIndex.Compact()
	tsiIndex.Wait()

	// Close TSI index.
	log.Info("Closing tsi index")
	if err := tsiIndex.Close(); err != nil {
		return err
	}

	// Rename TSI to standard path.
	log.Info("Moving tsi to permanent location")
	return fs.RenameFile(tmpPath, indexPath)
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

	collection := &tsdb.SeriesCollection{
		Keys:  make([][]byte, 0, batchSize),
		Names: make([][]byte, 0, batchSize),
		Tags:  make([]models.Tags, batchSize),
		Types: make([]models.FieldType, 0, batchSize),
	}
	var ti int
	iter := r.Iterator(nil)
	for iter.Next() {
		key := iter.Key()
		seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
		var name []byte
		name, collection.Tags[ti] = models.ParseKeyBytesWithTags(seriesKey, collection.Tags[ti])
		typ := iter.Type()

		if verboseLogging {
			log.Info("Series", zap.String("name", string(name)), zap.String("tags", collection.Tags[ti].String()))
		}

		collection.Keys = append(collection.Keys, seriesKey)
		collection.Names = append(collection.Names, name)
		collection.Types = append(collection.Types, modelsFieldType(typ))
		ti++

		// Flush batch?
		if len(collection.Keys) == batchSize {
			collection.Truncate(ti)
			if err := index.CreateSeriesListIfNotExists(collection); err != nil {
				return fmt.Errorf("problem creating series: (%s)", err)
			}
			collection.Truncate(0)
			collection.Tags = collection.Tags[:batchSize]
			ti = 0 // Reset tags.
		}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("problem creating series: (%s)", err)
	}

	// Flush any remaining series in the batches
	if len(collection.Keys) > 0 {
		collection.Truncate(ti)
		if err := index.CreateSeriesListIfNotExists(collection); err != nil {
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
		if filepath.Ext(fi.Name()) != "."+wal.WALFileExtension {
			continue
		}
		paths = append(paths, filepath.Join(path, fi.Name()))
	}
	return paths, nil
}

func modelsFieldType(block byte) models.FieldType {
	switch block {
	case tsm1.BlockFloat64:
		return models.Float
	case tsm1.BlockInteger:
		return models.Integer
	case tsm1.BlockBoolean:
		return models.Boolean
	case tsm1.BlockString:
		return models.String
	case tsm1.BlockUnsigned:
		return models.Unsigned
	default:
		return models.Empty
	}
}
