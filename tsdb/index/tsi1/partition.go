package tsi1

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/uber-go/zap"
)

// Version is the current version of the TSI index.
const Version = 1

// DefaultMaxLogFileSize is the default compaction threshold.
const DefaultMaxLogFileSize = 5 * 1024 * 1024

// File extensions.
const (
	LogFileExt   = ".tsl"
	IndexFileExt = ".tsi"

	CompactingExt = ".compacting"
)

// ManifestFileName is the name of the index manifest file.
const ManifestFileName = "MANIFEST"

// SeriesFileName is the name of the series file.
const SeriesFileName = "series"

// Partition represents a collection of layered index files and WAL.
type Partition struct {
	mu     sync.RWMutex
	opened bool

	sfile         *SeriesFile // series lookup file
	activeLogFile *LogFile    // current log file
	fileSet       *FileSet    // current file set
	seq           int         // file id sequence

	// Compaction management
	levels          []CompactionLevel // compaction levels
	levelCompacting []bool            // level compaction status

	// Close management.
	once    sync.Once
	closing chan struct{}
	wg      sync.WaitGroup

	// Fieldset shared with engine.
	fieldset *tsdb.MeasurementFieldSet

	// Name of database.
	Database string

	// Directory of the Partition's index files.
	path string

	// Log file compaction thresholds.
	MaxLogFileSize int64

	// Frequency of compaction checks.
	compactionsDisabled       bool
	compactionMonitorInterval time.Duration

	logger zap.Logger

	// Index's version.
	version int
}

// NewPartition returns a new instance of Partition.
func NewPartition(path string) *Partition {
	return &Partition{
		closing: make(chan struct{}),

		path: path,

		// Default compaction thresholds.
		MaxLogFileSize: DefaultMaxLogFileSize,
		// compactionEnabled: true,

		logger:  zap.New(zap.NullEncoder()),
		version: Version,
	}
}

// ErrIncompatibleVersion is returned when attempting to read from an
// incompatible tsi1 manifest file.
var ErrIncompatibleVersion = errors.New("incompatible tsi1 index MANIFEST")

// Open opens the partition.
func (i *Partition) Open() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.opened {
		return errors.New("index partition already open")
	}

	// Create directory if it doesn't exist.
	if err := os.MkdirAll(i.path, 0777); err != nil {
		return err
	}

	// Open series file.
	sfile := NewSeriesFile(i.SeriesFilePath())
	if err := sfile.Open(); err != nil {
		return err
	}
	i.sfile = sfile

	// Read manifest file.
	m, err := ReadManifestFile(filepath.Join(i.path, ManifestFileName))
	if os.IsNotExist(err) {
		m = NewManifest()
	} else if err != nil {
		return err
	}

	// Check to see if the MANIFEST file is compatible with the current Index.
	if err := m.Validate(); err != nil {
		return err
	}

	// Copy compaction levels to the index.
	i.levels = make([]CompactionLevel, len(m.Levels))
	copy(i.levels, m.Levels)

	// Set up flags to track whether a level is compacting.
	i.levelCompacting = make([]bool, len(i.levels))

	// Open each file in the manifest.
	var files []File
	for _, filename := range m.Files {
		switch filepath.Ext(filename) {
		case LogFileExt:
			f, err := i.openLogFile(filepath.Join(i.path, filename))
			if err != nil {
				return err
			}
			files = append(files, f)

			// Make first log file active, if within threshold.
			sz, _ := f.Stat()
			if i.activeLogFile == nil && sz < i.MaxLogFileSize {
				i.activeLogFile = f
			}

		case IndexFileExt:
			f, err := i.openIndexFile(filepath.Join(i.path, filename))
			if err != nil {
				return err
			}
			files = append(files, f)
		}
	}
	fs, err := NewFileSet(i.levels, i.sfile, files)
	if err != nil {
		return err
	}
	i.fileSet = fs

	// Set initial sequnce number.
	i.seq = i.fileSet.MaxID()

	// Delete any files not in the manifest.
	if err := i.deleteNonManifestFiles(m); err != nil {
		return err
	}

	// Ensure a log file exists.
	if i.activeLogFile == nil {
		if err := i.prependActiveLogFile(); err != nil {
			return err
		}
	}

	// Mark opened.
	i.opened = true

	// Send a compaction request on start up.
	i.compact()

	return nil
}

// openLogFile opens a log file and appends it to the index.
func (i *Partition) openLogFile(path string) (*LogFile, error) {
	f := NewLogFile(i.sfile, path)
	if err := f.Open(); err != nil {
		return nil, err
	}
	return f, nil
}

// openIndexFile opens a log file and appends it to the index.
func (i *Partition) openIndexFile(path string) (*IndexFile, error) {
	f := NewIndexFile(i.sfile)
	f.SetPath(path)
	if err := f.Open(); err != nil {
		return nil, err
	}
	return f, nil
}

// deleteNonManifestFiles removes all files not in the manifest.
func (i *Partition) deleteNonManifestFiles(m *Manifest) error {
	dir, err := os.Open(i.path)
	if err != nil {
		return err
	}
	defer dir.Close()

	fis, err := dir.Readdir(-1)
	if err != nil {
		return err
	}

	// Loop over all files and remove any not in the manifest.
	for _, fi := range fis {
		filename := filepath.Base(fi.Name())
		if filename == ManifestFileName || m.HasFile(filename) {
			continue
		}

		if err := os.RemoveAll(filename); err != nil {
			return err
		}
	}

	return nil
}

// Wait returns once outstanding compactions have finished.
func (i *Partition) Wait() {
	i.wg.Wait()
}

// Close closes the index.
func (i *Partition) Close() error {
	// Wait for goroutines to finish outstanding compactions.
	i.once.Do(func() { close(i.closing) })
	i.wg.Wait()

	// Lock index and close remaining
	i.mu.Lock()
	defer i.mu.Unlock()

	// Close log files.
	for _, f := range i.fileSet.files {
		f.Close()
	}
	i.fileSet.files = nil

	return nil
}

// NextSequence returns the next file identifier.
func (i *Partition) NextSequence() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.nextSequence()
}

func (i *Partition) nextSequence() int {
	i.seq++
	return i.seq
}

// ManifestPath returns the path to the index's manifest file.
func (i *Partition) ManifestPath() string {
	return filepath.Join(i.path, ManifestFileName)
}

// SeriesFilePath returns the path to the index's series file.
func (i *Partition) SeriesFilePath() string {
	return filepath.Join(i.path, SeriesFileName)
}

// Manifest returns a manifest for the index.
func (i *Partition) Manifest() *Manifest {
	m := &Manifest{
		Levels:  i.levels,
		Files:   make([]string, len(i.fileSet.files)),
		Version: i.version,
	}

	for j, f := range i.fileSet.files {
		m.Files[j] = filepath.Base(f.Path())
	}

	return m
}

// writeManifestFile writes the manifest to the appropriate file path.
func (i *Partition) writeManifestFile() error {
	return WriteManifestFile(i.ManifestPath(), i.Manifest())
}

// WithLogger sets the logger for the index.
func (i *Partition) WithLogger(logger zap.Logger) {
	i.logger = logger.With(zap.String("index", "tsi"))
}

// SetFieldSet sets a shared field set from the engine.
func (i *Partition) SetFieldSet(fs *tsdb.MeasurementFieldSet) {
	i.mu.Lock()
	i.fieldset = fs
	i.mu.Unlock()
}

// RetainFileSet returns the current fileset and adds a reference count.
func (i *Partition) RetainFileSet() *FileSet {
	i.mu.RLock()
	fs := i.retainFileSet()
	i.mu.RUnlock()
	return fs
}

func (i *Partition) retainFileSet() *FileSet {
	fs := i.fileSet
	fs.Retain()
	return fs
}

// FileN returns the active files in the file set.
func (i *Partition) FileN() int { return len(i.fileSet.files) }

// prependActiveLogFile adds a new log file so that the current log file can be compacted.
func (i *Partition) prependActiveLogFile() error {
	// Open file and insert it into the first position.
	f, err := i.openLogFile(filepath.Join(i.path, FormatLogFileName(i.nextSequence())))
	if err != nil {
		return err
	}
	i.activeLogFile = f

	// Prepend and generate new fileset.
	i.fileSet = i.fileSet.PrependLogFile(f)

	// Write new manifest.
	if err := i.writeManifestFile(); err != nil {
		// TODO: Close index if write fails.
		return err
	}

	return nil
}

// ForEachMeasurementName iterates over all measurement names in the index.
func (i *Partition) ForEachMeasurementName(fn func(name []byte) error) error {
	fs := i.RetainFileSet()
	defer fs.Release()

	itr := fs.MeasurementIterator()
	if itr == nil {
		return nil
	}

	for e := itr.Next(); e != nil; e = itr.Next() {
		if err := fn(e.Name()); err != nil {
			return err
		}
	}

	return nil
}

// MeasurementExists returns true if a measurement exists.
func (i *Partition) MeasurementExists(name []byte) (bool, error) {
	fs := i.RetainFileSet()
	defer fs.Release()
	m := fs.Measurement(name)
	return m != nil && !m.Deleted(), nil
}

func (i *Partition) MeasurementNamesByExpr(expr influxql.Expr) ([][]byte, error) {
	fs := i.RetainFileSet()
	defer fs.Release()
	return fs.MeasurementNamesByExpr(expr)
}

func (i *Partition) MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error) {
	fs := i.RetainFileSet()
	defer fs.Release()

	itr := fs.MeasurementIterator()
	var a [][]byte
	for e := itr.Next(); e != nil; e = itr.Next() {
		if re.Match(e.Name()) {
			a = append(a, e.Name())
		}
	}
	return a, nil
}

// DropMeasurement deletes a measurement from the index.
func (i *Partition) DropMeasurement(name []byte) error {
	fs := i.RetainFileSet()
	defer fs.Release()

	// Delete all keys and values.
	if kitr := fs.TagKeyIterator(name); kitr != nil {
		for k := kitr.Next(); k != nil; k = kitr.Next() {
			// Delete key if not already deleted.
			if !k.Deleted() {
				if err := func() error {
					i.mu.RLock()
					defer i.mu.RUnlock()
					return i.activeLogFile.DeleteTagKey(name, k.Key())
				}(); err != nil {
					return err
				}
			}

			// Delete each value in key.
			if vitr := k.TagValueIterator(); vitr != nil {
				for v := vitr.Next(); v != nil; v = vitr.Next() {
					if !v.Deleted() {
						if err := func() error {
							i.mu.RLock()
							defer i.mu.RUnlock()
							return i.activeLogFile.DeleteTagValue(name, k.Key(), v.Value())
						}(); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	// Delete all series in measurement.
	if sitr := fs.MeasurementSeriesIDIterator(name); sitr != nil {
		for s := sitr.Next(); s.SeriesID != 0; s = sitr.Next() {
			if !s.Deleted {
				if err := func() error {
					i.mu.RLock()
					defer i.mu.RUnlock()
					return i.activeLogFile.DeleteSeriesID(s.SeriesID)
				}(); err != nil {
					return err
				}
			}
		}
	}

	// Mark measurement as deleted.
	if err := func() error {
		i.mu.RLock()
		defer i.mu.RUnlock()
		return i.activeLogFile.DeleteMeasurement(name)
	}(); err != nil {
		return err
	}

	// Check if the log file needs to be swapped.
	if err := i.CheckLogFile(); err != nil {
		return err
	}

	return nil
}

// CreateSeriesListIfNotExists creates a list of series if they doesn't exist in bulk.
func (i *Partition) CreateSeriesListIfNotExists(_, names [][]byte, tagsSlice []models.Tags) error {
	// All slices must be of equal length.
	if len(names) != len(tagsSlice) {
		return errors.New("names/tags length mismatch")
	}

	// Maintain reference count on files in file set.
	fs := i.RetainFileSet()
	defer fs.Release()

	// Ensure fileset cannot change during insert.
	i.mu.RLock()
	// Insert series into log file.
	if err := i.activeLogFile.AddSeriesList(names, tagsSlice); err != nil {
		i.mu.RUnlock()
		return err
	}
	i.mu.RUnlock()

	return i.CheckLogFile()
}

// InitializeSeries is a no-op. This only applies to the in-memory index.
func (i *Partition) InitializeSeries(key, name []byte, tags models.Tags) error {
	return nil
}

// CreateSeriesIfNotExists creates a series if it doesn't exist or is deleted.
func (i *Partition) CreateSeriesIfNotExists(key, name []byte, tags models.Tags) error {
	return i.CreateSeriesListIfNotExists(nil, [][]byte{name}, []models.Tags{tags})
}

func (i *Partition) DropSeries(key []byte) error {
	if err := func() error {
		i.mu.RLock()
		defer i.mu.RUnlock()

		name, tags := models.ParseKey(key)

		mname := []byte(name)
		seriesID := i.sfile.Offset(mname, tags, nil)

		if err := i.activeLogFile.DeleteSeriesID(seriesID); err != nil {
			return err
		}

		// Obtain file set after deletion because that may add a new log file.
		fs := i.retainFileSet()
		defer fs.Release()

		// Check if that was the last series for the measurement in the entire index.
		itr := fs.MeasurementSeriesIDIterator(mname)
		if itr == nil {
			return nil
		} else if e := itr.Next(); e.SeriesID != 0 {
			return nil
		}

		// If no more series exist in the measurement then delete the measurement.
		if err := i.activeLogFile.DeleteMeasurement(mname); err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return err
	}

	// Swap log file, if necesssary.
	if err := i.CheckLogFile(); err != nil {
		return err
	}
	return nil
}

// MeasurementsSketches returns the two sketches for the index by merging all
// instances of the type sketch types in all the index files.
func (i *Partition) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	fs := i.RetainFileSet()
	defer fs.Release()
	return fs.MeasurementsSketches()
}

// SeriesN returns the number of unique non-tombstoned series in the index.
// Since indexes are not shared across shards, the count returned by SeriesN
// cannot be combined with other shard's results. If you need to count series
// across indexes then use SeriesSketches and merge the results from other
// indexes.
func (i *Partition) SeriesN() int64 {
	return int64(i.sfile.SeriesCount())
}

// HasTagKey returns true if tag key exists.
func (i *Partition) HasTagKey(name, key []byte) (bool, error) {
	fs := i.RetainFileSet()
	defer fs.Release()
	return fs.HasTagKey(name, key), nil
}

// MeasurementTagKeysByExpr extracts the tag keys wanted by the expression.
func (i *Partition) MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) {
	fs := i.RetainFileSet()
	defer fs.Release()
	return fs.MeasurementTagKeysByExpr(name, expr)
}

// MeasurementTagKeyValuesByExpr returns a set of tag values filtered by an expression.
//
// See tsm1.Engine.MeasurementTagKeyValuesByExpr for a fuller description of this
// method.
func (i *Partition) MeasurementTagKeyValuesByExpr(name []byte, keys []string, expr influxql.Expr, keysSorted bool) ([][]string, error) {
	fs := i.RetainFileSet()
	defer fs.Release()

	if len(keys) == 0 {
		return nil, nil
	}

	results := make([][]string, len(keys))
	// If we haven't been provided sorted keys, then we need to sort them.
	if !keysSorted {
		sort.Sort(sort.StringSlice(keys))
	}

	// No expression means that the values shouldn't be filtered, so we can
	// fetch them all.
	if expr == nil {
		for ki, key := range keys {
			itr := fs.TagValueIterator(name, []byte(key))
			for val := itr.Next(); val != nil; val = itr.Next() {
				results[ki] = append(results[ki], string(val.Value()))
			}
		}
		return results, nil
	}

	// This is the case where we have filtered series by some WHERE condition.
	// We only care about the tag values for the keys given the
	// filtered set of series ids.
	resultSet, err := fs.tagValuesByKeyAndExpr(name, keys, expr, i.fieldset)
	if err != nil {
		return nil, err
	}

	// Convert result sets into []string
	for i, s := range resultSet {
		values := make([]string, 0, len(s))
		for v := range s {
			values = append(values, v)
		}
		sort.Sort(sort.StringSlice(values))
		results[i] = values
	}
	return results, nil
}

// ForEachMeasurementTagKey iterates over all tag keys in a measurement.
func (i *Partition) ForEachMeasurementTagKey(name []byte, fn func(key []byte) error) error {
	fs := i.RetainFileSet()
	defer fs.Release()

	itr := fs.TagKeyIterator(name)
	if itr == nil {
		return nil
	}

	for e := itr.Next(); e != nil; e = itr.Next() {
		if err := fn(e.Key()); err != nil {
			return err
		}
	}

	return nil
}

// TagKeyCardinality always returns zero.
// It is not possible to determine cardinality of tags across index files.
func (i *Partition) TagKeyCardinality(name, key []byte) int {
	return 0
}

// MeasurementSeriesKeysByExpr returns a list of series keys matching expr.
func (i *Partition) MeasurementSeriesKeysByExpr(name []byte, expr influxql.Expr) ([][]byte, error) {
	fs := i.RetainFileSet()
	defer fs.Release()
	return fs.MeasurementSeriesKeysByExpr(name, expr, i.fieldset)
}

// TagSets returns an ordered list of tag sets for a measurement by dimension
// and filtered by an optional conditional expression.
func (i *Partition) TagSets(name []byte, opt query.IteratorOptions) ([]*query.TagSet, error) {
	fs := i.RetainFileSet()
	defer fs.Release()

	itr, err := fs.MeasurementSeriesByExprIterator(name, opt.Condition, i.fieldset)
	if err != nil {
		return nil, err
	} else if itr == nil {
		return nil, nil
	}

	// For every series, get the tag values for the requested tag keys i.e.
	// dimensions. This is the TagSet for that series. Series with the same
	// TagSet are then grouped together, because for the purpose of GROUP BY
	// they are part of the same composite series.
	tagSets := make(map[string]*query.TagSet, 64)

	if itr != nil {
		for e := itr.Next(); e.SeriesID != 0; e = itr.Next() {
			_, tags := ParseSeriesKey(i.sfile.SeriesKey(e.SeriesID))
			if opt.Authorizer != nil && !opt.Authorizer.AuthorizeSeriesRead(i.Database, name, tags) {
				continue
			}

			tagsMap := make(map[string]string, len(opt.Dimensions))

			// Build the TagSet for this series.
			for _, dim := range opt.Dimensions {
				tagsMap[dim] = tags.GetString(dim)
			}

			// Convert the TagSet to a string, so it can be added to a map
			// allowing TagSets to be handled as a set.
			tagsAsKey := tsdb.MarshalTags(tagsMap)
			tagSet, ok := tagSets[string(tagsAsKey)]
			if !ok {
				// This TagSet is new, create a new entry for it.
				tagSet = &query.TagSet{
					Tags: tagsMap,
					Key:  tagsAsKey,
				}
			}

			// Associate the series and filter with the Tagset.
			tagSet.AddFilter(string(models.MakeKey(name, tags)), e.Expr)

			// Ensure it's back in the map.
			tagSets[string(tagsAsKey)] = tagSet
		}
	}

	// Sort the series in each tag set.
	for _, t := range tagSets {
		sort.Sort(t)
	}

	// The TagSets have been created, as a map of TagSets. Just send
	// the values back as a slice, sorting for consistency.
	sortedTagsSets := make([]*query.TagSet, 0, len(tagSets))
	for _, v := range tagSets {
		sortedTagsSets = append(sortedTagsSets, v)
	}
	sort.Sort(byTagKey(sortedTagsSets))

	return sortedTagsSets, nil
}

// SnapshotTo creates hard links to the file set into path.
func (i *Partition) SnapshotTo(path string) error {
	panic("NEED TO MAKE THIS PARTITION AWARE")
	// i.mu.Lock()
	// defer i.mu.Unlock()

	// fs := i.retainFileSet()
	// defer fs.Release()

	// // Flush active log file, if any.
	// if err := i.activeLogFile.Flush(); err != nil {
	// 	return err
	// }

	// if err := os.Mkdir(filepath.Join(path, "index"), 0777); err != nil {
	// 	return err
	// }

	// // Link manifest.
	// if err := os.Link(i.ManifestPath(), filepath.Join(path, "index", filepath.Base(i.ManifestPath()))); err != nil {
	// 	return fmt.Errorf("error creating tsi manifest hard link: %q", err)
	// }

	// // Link series file.
	// if err := os.Link(i.SeriesFilePath(), filepath.Join(path, "index", filepath.Base(i.SeriesFilePath()))); err != nil {
	// 	return fmt.Errorf("error creating tsi series file hard link: %q", err)
	// }

	// // Link files in directory.
	// for _, f := range fs.files {
	// 	if err := os.Link(f.Path(), filepath.Join(path, "index", filepath.Base(f.Path()))); err != nil {
	// 		return fmt.Errorf("error creating tsi hard link: %q", err)
	// 	}
	// }

	// return nil
}

func (i *Partition) SetFieldName(measurement []byte, name string) {}
func (i *Partition) RemoveShard(shardID uint64)                   {}
func (i *Partition) AssignShard(k string, shardID uint64)         {}

func (i *Partition) UnassignShard(k string, shardID uint64) error {
	// This can be called directly once inmem is gone.
	return i.DropSeries([]byte(k))
}

// SeriesPointIterator returns an influxql iterator over all series.
func (i *Partition) SeriesPointIterator(opt query.IteratorOptions) (query.Iterator, error) {
	// NOTE: The iterator handles releasing the file set.
	fs := i.RetainFileSet()
	return newSeriesPointIterator(fs, i.fieldset, opt), nil
}

// Compact requests a compaction of log files.
func (i *Partition) Compact() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.compact()
}

// compact compacts continguous groups of files that are not currently compacting.
func (i *Partition) compact() {
	if i.compactionsDisabled {
		return
	}

	fs := i.retainFileSet()
	defer fs.Release()

	// Iterate over each level we are going to compact.
	// We skip the first level (0) because it is log files and they are compacted separately.
	// We skip the last level because the files have no higher level to compact into.
	minLevel, maxLevel := 1, len(i.levels)-2
	for level := minLevel; level <= maxLevel; level++ {
		// Skip level if it is currently compacting.
		if i.levelCompacting[level] {
			continue
		}

		// Collect contiguous files from the end of the level.
		files := fs.LastContiguousIndexFilesByLevel(level)
		if len(files) < 2 {
			continue
		} else if len(files) > MaxIndexMergeCount {
			files = files[len(files)-MaxIndexMergeCount:]
		}

		// Retain files during compaction.
		IndexFiles(files).Retain()

		// Mark the level as compacting.
		i.levelCompacting[level] = true

		// Execute in closure to save reference to the group within the loop.
		func(files []*IndexFile, level int) {
			// Start compacting in a separate goroutine.
			i.wg.Add(1)
			go func() {
				defer i.wg.Done()

				// Compact to a new level.
				i.compactToLevel(files, level+1)

				// Ensure compaction lock for the level is released.
				i.mu.Lock()
				i.levelCompacting[level] = false
				i.mu.Unlock()

				// Check for new compactions
				i.Compact()
			}()
		}(files, level)
	}
}

// compactToLevel compacts a set of files into a new file. Replaces old files with
// compacted file on successful completion. This runs in a separate goroutine.
func (i *Partition) compactToLevel(files []*IndexFile, level int) {
	assert(len(files) >= 2, "at least two index files are required for compaction")
	assert(level > 0, "cannot compact level zero")

	// Build a logger for this compaction.
	logger := i.logger.With(zap.String("token", generateCompactionToken()))

	// Files have already been retained by caller.
	// Ensure files are released only once.
	var once sync.Once
	defer once.Do(func() { IndexFiles(files).Release() })

	// Track time to compact.
	start := time.Now()

	// Create new index file.
	path := filepath.Join(i.path, FormatIndexFileName(i.NextSequence(), level))
	f, err := os.Create(path)
	if err != nil {
		logger.Error("cannot create compation files", zap.Error(err))
		return
	}
	defer f.Close()

	logger.Info("performing full compaction",
		zap.String("src", joinIntSlice(IndexFiles(files).IDs(), ",")),
		zap.String("dst", path),
	)

	// Compact all index files to new index file.
	lvl := i.levels[level]
	n, err := IndexFiles(files).CompactTo(f, i.sfile, lvl.M, lvl.K)
	if err != nil {
		logger.Error("cannot compact index files", zap.Error(err))
		return
	}

	// Close file.
	if err := f.Close(); err != nil {
		logger.Error("error closing index file", zap.Error(err))
		return
	}

	// Reopen as an index file.
	file := NewIndexFile(i.sfile)
	file.SetPath(path)
	if err := file.Open(); err != nil {
		logger.Error("cannot open new index file", zap.Error(err))
		return
	}

	// Obtain lock to swap in index file and write manifest.
	if err := func() error {
		i.mu.Lock()
		defer i.mu.Unlock()

		// Replace previous files with new index file.
		i.fileSet = i.fileSet.MustReplace(IndexFiles(files).Files(), file)

		// Write new manifest.
		if err := i.writeManifestFile(); err != nil {
			// TODO: Close index if write fails.
			return err
		}
		return nil
	}(); err != nil {
		logger.Error("cannot write manifest", zap.Error(err))
		return
	}

	elapsed := time.Since(start)
	logger.Info("full compaction complete",
		zap.String("path", path),
		zap.String("elapsed", elapsed.String()),
		zap.Int64("bytes", n),
		zap.Int("kb_per_sec", int(float64(n)/elapsed.Seconds())/1024),
	)

	// Release old files.
	once.Do(func() { IndexFiles(files).Release() })

	// Close and delete all old index files.
	for _, f := range files {
		logger.Info("removing index file", zap.String("path", f.Path()))

		if err := f.Close(); err != nil {
			logger.Error("cannot close index file", zap.Error(err))
			return
		} else if err := os.Remove(f.Path()); err != nil {
			logger.Error("cannot remove index file", zap.Error(err))
			return
		}
	}
}

func (i *Partition) Rebuild() {}

func (i *Partition) CheckLogFile() error {
	// Check log file size under read lock.
	if size := func() int64 {
		i.mu.RLock()
		defer i.mu.RUnlock()
		return i.activeLogFile.Size()
	}(); size < i.MaxLogFileSize {
		return nil
	}

	// If file size exceeded then recheck under write lock and swap files.
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.checkLogFile()
}

func (i *Partition) checkLogFile() error {
	if i.activeLogFile.Size() < i.MaxLogFileSize {
		return nil
	}

	// Swap current log file.
	logFile := i.activeLogFile

	// Open new log file and insert it into the first position.
	if err := i.prependActiveLogFile(); err != nil {
		return err
	}

	// Begin compacting in a background goroutine.
	i.wg.Add(1)
	go func() {
		defer i.wg.Done()
		i.compactLogFile(logFile)
		i.Compact() // check for new compactions
	}()

	return nil
}

// compactLogFile compacts f into a tsi file. The new file will share the
// same identifier but will have a ".tsi" extension. Once the log file is
// compacted then the manifest is updated and the log file is discarded.
func (i *Partition) compactLogFile(logFile *LogFile) {
	start := time.Now()

	// Retrieve identifier from current path.
	id := logFile.ID()
	assert(id != 0, "cannot parse log file id: %s", logFile.Path())

	// Build a logger for this compaction.
	logger := i.logger.With(
		zap.String("token", generateCompactionToken()),
		zap.Int("id", id),
	)

	// Create new index file.
	path := filepath.Join(i.path, FormatIndexFileName(id, 1))
	f, err := os.Create(path)
	if err != nil {
		logger.Error("cannot create index file", zap.Error(err))
		return
	}
	defer f.Close()

	// Compact log file to new index file.
	lvl := i.levels[1]
	n, err := logFile.CompactTo(f, lvl.M, lvl.K)
	if err != nil {
		logger.Error("cannot compact log file", zap.Error(err), zap.String("path", logFile.Path()))
		return
	}

	// Close file.
	if err := f.Close(); err != nil {
		logger.Error("cannot close log file", zap.Error(err))
		return
	}

	// Reopen as an index file.
	file := NewIndexFile(i.sfile)
	file.SetPath(path)
	if err := file.Open(); err != nil {
		logger.Error("cannot open compacted index file", zap.Error(err), zap.String("path", file.Path()))
		return
	}

	// Obtain lock to swap in index file and write manifest.
	if err := func() error {
		i.mu.Lock()
		defer i.mu.Unlock()

		// Replace previous log file with index file.
		i.fileSet = i.fileSet.MustReplace([]File{logFile}, file)

		// Write new manifest.
		if err := i.writeManifestFile(); err != nil {
			// TODO: Close index if write fails.
			return err
		}
		return nil
	}(); err != nil {
		logger.Error("cannot update manifest", zap.Error(err))
		return
	}

	elapsed := time.Since(start)
	logger.Error("log file compacted",
		zap.String("elapsed", elapsed.String()),
		zap.Int64("bytes", n),
		zap.Int("kb_per_sec", int(float64(n)/elapsed.Seconds())/1024),
	)

	// Closing the log file will automatically wait until the ref count is zero.
	if err := logFile.Close(); err != nil {
		logger.Error("cannot close log file", zap.Error(err))
		return
	} else if err := os.Remove(logFile.Path()); err != nil {
		logger.Error("cannot remove log file", zap.Error(err))
		return
	}

	return
}

// seriesPointIterator adapts SeriesIterator to an influxql.Iterator.
type seriesPointIterator struct {
	once     sync.Once
	fs       *FileSet
	fieldset *tsdb.MeasurementFieldSet
	mitr     MeasurementIterator
	sitr     SeriesIDIterator
	opt      query.IteratorOptions

	point query.FloatPoint // reusable point
}

// newSeriesPointIterator returns a new instance of seriesPointIterator.
func newSeriesPointIterator(fs *FileSet, fieldset *tsdb.MeasurementFieldSet, opt query.IteratorOptions) *seriesPointIterator {
	return &seriesPointIterator{
		fs:       fs,
		fieldset: fieldset,
		mitr:     fs.MeasurementIterator(),
		point: query.FloatPoint{
			Aux: make([]interface{}, len(opt.Aux)),
		},
		opt: opt,
	}
}

// Stats returns stats about the points processed.
func (itr *seriesPointIterator) Stats() query.IteratorStats { return query.IteratorStats{} }

// Close closes the iterator.
func (itr *seriesPointIterator) Close() error {
	itr.once.Do(func() { itr.fs.Release() })
	return nil
}

// Next emits the next point in the iterator.
func (itr *seriesPointIterator) Next() (*query.FloatPoint, error) {
	for {
		// Create new series iterator, if necessary.
		// Exit if there are no measurements remaining.
		if itr.sitr == nil {
			m := itr.mitr.Next()
			if m == nil {
				return nil, nil
			}

			sitr, err := itr.fs.MeasurementSeriesByExprIterator(m.Name(), itr.opt.Condition, itr.fieldset)
			if err != nil {
				return nil, err
			} else if sitr == nil {
				continue
			}
			itr.sitr = sitr
		}

		// Read next series element.
		elem := itr.sitr.Next()
		if elem.SeriesID == 0 {
			itr.sitr = nil
			continue
		}

		// Convert to a key.
		seriesKey := itr.fs.sfile.SeriesKey(elem.SeriesID)
		if seriesKey == nil {
			continue
		}

		// Write auxiliary fields.
		name, tags := ParseSeriesKey(seriesKey)
		key := models.MakeKey(name, tags)
		for i, f := range itr.opt.Aux {
			switch f.Val {
			case "key":
				itr.point.Aux[i] = key
			}
		}
		return &itr.point, nil
	}
}

// unionStringSets returns the union of two sets
func unionStringSets(a, b map[string]struct{}) map[string]struct{} {
	other := make(map[string]struct{})
	for k := range a {
		other[k] = struct{}{}
	}
	for k := range b {
		other[k] = struct{}{}
	}
	return other
}

// intersectStringSets returns the intersection of two sets.
func intersectStringSets(a, b map[string]struct{}) map[string]struct{} {
	if len(a) < len(b) {
		a, b = b, a
	}

	other := make(map[string]struct{})
	for k := range a {
		if _, ok := b[k]; ok {
			other[k] = struct{}{}
		}
	}
	return other
}

var fileIDRegex = regexp.MustCompile(`^L(\d+)-(\d+)\..+$`)

// ParseFilename extracts the numeric id from a log or index file path.
// Returns 0 if it cannot be parsed.
func ParseFilename(name string) (level, id int) {
	a := fileIDRegex.FindStringSubmatch(filepath.Base(name))
	if a == nil {
		return 0, 0
	}

	level, _ = strconv.Atoi(a[1])
	id, _ = strconv.Atoi(a[2])
	return id, level
}

// Manifest represents the list of log & index files that make up the index.
// The files are listed in time order, not necessarily ID order.
type Manifest struct {
	Levels []CompactionLevel `json:"levels,omitempty"`
	Files  []string          `json:"files,omitempty"`

	// Version should be updated whenever the TSI format has changed.
	Version int `json:"version,omitempty"`
}

// NewManifest returns a new instance of Manifest with default compaction levels.
func NewManifest() *Manifest {
	m := &Manifest{
		Levels:  make([]CompactionLevel, len(DefaultCompactionLevels)),
		Version: Version,
	}
	copy(m.Levels, DefaultCompactionLevels[:])
	return m
}

// HasFile returns true if name is listed in the log files or index files.
func (m *Manifest) HasFile(name string) bool {
	for _, filename := range m.Files {
		if filename == name {
			return true
		}
	}
	return false
}

// Validate checks if the Manifest's version is compatible with this version
// of the tsi1 index.
func (m *Manifest) Validate() error {
	// If we don't have an explicit version in the manifest file then we know
	// it's not compatible with the latest tsi1 Index.
	if m.Version != Version {
		return ErrIncompatibleVersion
	}
	return nil
}

// ReadManifestFile reads a manifest from a file path.
func ReadManifestFile(path string) (*Manifest, error) {
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// Decode manifest.
	var m Manifest
	if err := json.Unmarshal(buf, &m); err != nil {
		return nil, err
	}

	return &m, nil
}

// WriteManifestFile writes a manifest to a file path.
func WriteManifestFile(path string, m *Manifest) error {
	buf, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	buf = append(buf, '\n')

	if err := ioutil.WriteFile(path, buf, 0666); err != nil {
		return err
	}

	return nil
}

func joinIntSlice(a []int, sep string) string {
	other := make([]string, len(a))
	for i := range a {
		other[i] = strconv.Itoa(a[i])
	}
	return strings.Join(other, sep)
}

// CompactionLevel represents a grouping of index files based on bloom filter
// settings. By having the same bloom filter settings, the filters
// can be merged and evaluated at a higher level.
type CompactionLevel struct {
	// Bloom filter bit size & hash count
	M uint64 `json:"m,omitempty"`
	K uint64 `json:"k,omitempty"`
}

// DefaultCompactionLevels is the default settings used by the index.
var DefaultCompactionLevels = []CompactionLevel{
	{M: 0, K: 0},       // L0: Log files, no filter.
	{M: 1 << 25, K: 6}, // L1: Initial compaction
	{M: 1 << 25, K: 6}, // L2
	{M: 1 << 26, K: 6}, // L3
	{M: 1 << 27, K: 6}, // L4
	{M: 1 << 28, K: 6}, // L5
	{M: 1 << 29, K: 6}, // L6
	{M: 1 << 30, K: 6}, // L7
}

// MaxIndexMergeCount is the maximum number of files that can be merged together at once.
const MaxIndexMergeCount = 2

// MaxIndexFileSize is the maximum expected size of an index file.
const MaxIndexFileSize = 4 * (1 << 30)

// generateCompactionToken returns a short token to track an individual compaction.
// It is only used for logging so it doesn't need strong uniqueness guarantees.
func generateCompactionToken() string {
	token := make([]byte, 3)
	rand.Read(token)
	return fmt.Sprintf("%x", token)
}
