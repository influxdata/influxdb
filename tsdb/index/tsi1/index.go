package tsi1

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/tsdb"
)

// Default compaction thresholds.
const (
	DefaultMaxLogFileSize           = 5 * 1024 * 1024
	DefaultMaxLogFileIdleDuration   = 1 * time.Minute
	DefaultMaxIndexFileIdleDuration = 5 * time.Minute

	DefaultCompactionMonitorInterval = 30 * time.Second
)

func init() {
	tsdb.RegisterIndex("tsi1", func(id uint64, path string, opt tsdb.EngineOptions) tsdb.Index {
		idx := NewIndex()
		idx.ShardID = id
		idx.Path = path
		return idx
	})
}

// File extensions.
const (
	LogFileExt   = ".tsl"
	IndexFileExt = ".tsi"

	CompactingExt = ".compacting"
)

// ManifestFileName is the name of the index manifest file.
const ManifestFileName = "MANIFEST"

// Ensure index implements the interface.
var _ tsdb.Index = &Index{}

// Index represents a collection of layered index files and WAL.
type Index struct {
	mu     sync.RWMutex
	opened bool

	activeLogFile *LogFile
	logFiles      []*LogFile
	indexFiles    IndexFiles
	fileSet       FileSet

	// Compaction management.
	manualCompactNotify chan compactNotify
	fastCompactNotify   chan struct{}

	// Close management.
	once    sync.Once
	closing chan struct{}
	wg      sync.WaitGroup

	// Fieldset shared with engine.
	fieldset *tsdb.MeasurementFieldSet

	// Associated shard info.
	ShardID uint64

	// Root directory of the index files.
	Path string

	// Log file compaction thresholds.
	MaxLogFileSize           int64
	MaxLogFileIdleDuration   time.Duration
	MaxIndexFileIdleDuration time.Duration

	// Frequency of compaction checks.
	CompactionMonitorInterval time.Duration
}

// NewIndex returns a new instance of Index.
func NewIndex() *Index {
	return &Index{
		manualCompactNotify: make(chan compactNotify),
		fastCompactNotify:   make(chan struct{}),

		closing: make(chan struct{}),

		// Default compaction thresholds.
		MaxLogFileSize:           DefaultMaxLogFileSize,
		MaxLogFileIdleDuration:   DefaultMaxLogFileIdleDuration,
		MaxIndexFileIdleDuration: DefaultMaxIndexFileIdleDuration,

		CompactionMonitorInterval: DefaultCompactionMonitorInterval,
	}
}

// Open opens the index.
func (i *Index) Open() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.opened {
		return errors.New("index already open")
	}

	// Create directory if it doesn't exist.
	if err := os.MkdirAll(i.Path, 0777); err != nil {
		return err
	}

	// Read manifest file.
	m, err := ReadManifestFile(filepath.Join(i.Path, ManifestFileName))
	if os.IsNotExist(err) {
		m = &Manifest{}
	} else if err != nil {
		return err
	}

	// Open each log file in the manifest.
	for _, filename := range m.LogFiles {
		f, err := i.openLogFile(filepath.Join(i.Path, filename))
		if err != nil {
			return err
		}
		i.logFiles = append(i.logFiles, f)
	}

	// Make first log file active.
	if len(i.logFiles) > 0 {
		i.activeLogFile = i.logFiles[0]
	}

	// Open each index file in the manifest.
	for _, filename := range m.IndexFiles {
		f, err := i.openIndexFile(filepath.Join(i.Path, filename))
		if err != nil {
			return err
		}
		i.indexFiles = append(i.indexFiles, f)
	}

	// Delete any files not in the manifest.
	if err := i.deleteNonManifestFiles(m); err != nil {
		return err
	}

	// Build initial file set.
	i.buildFileSet()

	// Start compaction monitor.
	i.wg.Add(1)
	go func() { defer i.wg.Done(); i.monitorCompaction() }()

	// Mark opened.
	i.opened = true

	return nil
}

// openLogFile opens a log file and appends it to the index.
func (i *Index) openLogFile(path string) (*LogFile, error) {
	f := NewLogFile()
	f.SetPath(path)
	if err := f.Open(); err != nil {
		return nil, err
	}
	return f, nil
}

// openIndexFile opens a log file and appends it to the index.
func (i *Index) openIndexFile(path string) (*IndexFile, error) {
	f := NewIndexFile()
	f.SetPath(path)
	if err := f.Open(); err != nil {
		return nil, err
	}
	return f, nil
}

// deleteNonManifestFiles removes all files not in the manifest.
func (i *Index) deleteNonManifestFiles(m *Manifest) error {
	dir, err := os.Open(i.Path)
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

// Close closes the index.
func (i *Index) Close() error {
	// Wait for goroutines to finish.
	i.once.Do(func() { close(i.closing) })
	i.wg.Wait()

	// Lock index and close remaining
	i.mu.Lock()
	defer i.mu.Unlock()

	// Close log files.
	for _, f := range i.logFiles {
		f.Close()
	}
	i.logFiles = nil

	// Close index files.
	for _, f := range i.indexFiles {
		f.Close()
	}
	i.indexFiles = nil

	// Clear fileset.
	i.buildFileSet()

	return nil
}

// ManifestPath returns the path to the index's manifest file.
func (i *Index) ManifestPath() string {
	return filepath.Join(i.Path, ManifestFileName)
}

// Manifest returns a manifest for the index.
func (i *Index) Manifest() *Manifest {
	m := &Manifest{
		LogFiles:   make([]string, len(i.logFiles)),
		IndexFiles: make([]string, len(i.indexFiles)),
	}

	for j, f := range i.logFiles {
		m.LogFiles[j] = filepath.Base(f.Path())
	}
	for j, f := range i.indexFiles {
		m.IndexFiles[j] = filepath.Base(f.Path())
	}

	return m
}

// writeManifestFile writes the manifest to the appropriate file path.
func (i *Index) writeManifestFile() error {
	return WriteManifestFile(i.ManifestPath(), i.Manifest())
}

// maxFileID returns the highest file id from the index.
func (i *Index) maxFileID() int {
	var max int
	for _, f := range i.logFiles {
		if i := ParseFileID(f.Path()); i > max {
			max = i
		}
	}
	for _, f := range i.indexFiles {
		if i := ParseFileID(f.Path()); i > max {
			max = i
		}
	}
	return max
}

// SetFieldSet sets a shared field set from the engine.
func (i *Index) SetFieldSet(fs *tsdb.MeasurementFieldSet) {
	i.mu.Lock()
	i.fieldset = fs
	i.mu.Unlock()
}

// buildFileSet constructs the file set from the log & index files.
func (i *Index) buildFileSet() {
	fs := make(FileSet, 0, len(i.logFiles)+len(i.indexFiles))
	for _, f := range i.logFiles {
		fs = append(fs, f)
	}
	for _, f := range i.indexFiles {
		fs = append(fs, f)
	}
	i.fileSet = fs
}

// RetainFileSet returns the current fileset and adds a reference count.
func (i *Index) RetainFileSet() FileSet {
	i.mu.Lock()
	fs := i.retainFileSet()
	i.mu.Unlock()
	return fs
}

func (i *Index) retainFileSet() FileSet {
	fs := i.fileSet
	fs.Retain()
	return fs
}

// FileN returns the active files in the file set.
func (i *Index) FileN() int { return len(i.fileSet) }

// prependActiveLogFile adds a new log file so that the current log file can be compacted.
func (i *Index) prependActiveLogFile() error {
	// Generate new file identifier.
	id := i.maxFileID() + 1

	// Open file and insert it into the first position.
	f, err := i.openLogFile(filepath.Join(i.Path, FormatLogFileName(id)))
	if err != nil {
		return err
	}
	i.activeLogFile = f
	i.logFiles = append([]*LogFile{f}, i.logFiles...)
	i.buildFileSet()

	// Write new manifest.
	if err := i.writeManifestFile(); err != nil {
		// TODO: Close index if write fails.
		return err
	}

	return nil
}

// WithLogFile executes fn with the active log file under write lock.
func (i *Index) WithLogFile(fn func(f *LogFile) error) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.withLogFile(fn)
}

func (i *Index) withLogFile(fn func(f *LogFile) error) error {
	// Create log file if it doesn't exist.
	if i.activeLogFile == nil {
		if err := i.prependActiveLogFile(); err != nil {
			return err
		}
	}

	return fn(i.activeLogFile)
}

// ForEachMeasurementName iterates over all measurement names in the index.
func (i *Index) ForEachMeasurementName(fn func(name []byte) error) error {
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
func (i *Index) MeasurementExists(name []byte) (bool, error) {
	fs := i.RetainFileSet()
	defer fs.Release()
	m := fs.Measurement(name)
	return m != nil && !m.Deleted(), nil
}

func (i *Index) MeasurementNamesByExpr(expr influxql.Expr) ([][]byte, error) {
	fs := i.RetainFileSet()
	defer fs.Release()
	return fs.MeasurementNamesByExpr(expr)
}

func (i *Index) MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error) {
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
func (i *Index) DropMeasurement(name []byte) error {
	fs := i.RetainFileSet()
	defer fs.Release()

	// Delete all keys and values.
	if kitr := fs.TagKeyIterator(name); kitr != nil {
		for k := kitr.Next(); k != nil; k = kitr.Next() {
			// Delete key if not already deleted.
			if !k.Deleted() {
				if err := i.WithLogFile(func(f *LogFile) error {
					return f.DeleteTagKey(name, k.Key())
				}); err != nil {
					return err
				}
			}

			// Delete each value in key.
			if vitr := k.TagValueIterator(); vitr != nil {
				for v := vitr.Next(); v != nil; v = vitr.Next() {
					if !v.Deleted() {
						if err := i.WithLogFile(func(f *LogFile) error {
							return f.DeleteTagValue(name, k.Key(), v.Value())
						}); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	// Delete all series in measurement.
	if sitr := fs.MeasurementSeriesIterator(name); sitr != nil {
		for s := sitr.Next(); s != nil; s = sitr.Next() {
			if !s.Deleted() {
				if err := i.WithLogFile(func(f *LogFile) error {
					return f.DeleteSeries(s.Name(), s.Tags())
				}); err != nil {
					return err
				}
			}
		}
	}

	// Mark measurement as deleted.
	if err := i.WithLogFile(func(f *LogFile) error {
		return f.DeleteMeasurement(name)
	}); err != nil {
		return err
	}

	i.CheckFastCompaction()
	return nil
}

// CreateSeriesListIfNotExists creates a list of series if they doesn't exist in bulk.
func (i *Index) CreateSeriesListIfNotExists(_, names [][]byte, tagsSlice []models.Tags) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	// All slices must be of equal length.
	if len(names) != len(tagsSlice) {
		return errors.New("names/tags length mismatch")
	}

	fs := i.retainFileSet()
	defer fs.Release()

	// Filter out existing series.
	names, tagsSlice = fs.FilterNamesTags(names, tagsSlice)

	if len(names) > 0 {
		if err := i.withLogFile(func(f *LogFile) error {
			return f.AddSeriesList(names, tagsSlice)
		}); err != nil {
			return err
		}
	}

	i.checkFastCompaction()
	return nil
}

// CreateSeriesIfNotExists creates a series if it doesn't exist or is deleted.
func (i *Index) CreateSeriesIfNotExists(key, name []byte, tags models.Tags) error {
	i.mu.RLock()
	defer i.mu.RUnlock()

	fs := i.retainFileSet()
	defer fs.Release()

	if fs.HasSeries(name, tags) {
		return nil
	}

	if err := i.withLogFile(func(f *LogFile) error {
		return f.AddSeries(name, tags)
	}); err != nil {
		return err
	}

	i.checkFastCompaction()
	return nil
}

func (i *Index) DropSeries(keys [][]byte) error {
	i.mu.RLock()
	defer i.mu.RUnlock()

	for _, key := range keys {
		name, tags, err := models.ParseKey(key)
		if err != nil {
			return err
		}

		mname := []byte(name)
		if err := i.withLogFile(func(f *LogFile) error {
			if err := f.DeleteSeries(mname, tags); err != nil {
				return err
			}

			// Obtain file set after deletion because that may add a new log file.
			fs := i.retainFileSet()
			defer fs.Release()

			// Check if that was the last series for the measurement in the entire index.
			itr := fs.MeasurementSeriesIterator(mname)
			if itr == nil {
				return nil
			} else if e := itr.Next(); e != nil {
				return nil
			}

			// If no more series exist in the measurement then delete the measurement.
			if err := f.DeleteMeasurement(mname); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}

	i.checkFastCompaction()
	return nil
}

// SeriesSketches returns the two sketches for the index by merging all
// instances sketches from TSI files and the WAL.
func (i *Index) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	fs := i.RetainFileSet()
	defer fs.Release()
	return fs.SeriesSketches()
}

// MeasurementsSketches returns the two sketches for the index by merging all
// instances of the type sketch types in all the index files.
func (i *Index) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	fs := i.RetainFileSet()
	defer fs.Release()
	return fs.MeasurementsSketches()
}

// SeriesN returns the number of unique non-tombstoned series in the index.
// Since indexes are not shared across shards, the count returned by SeriesN
// cannot be combined with other shard's results. If you need to count series
// across indexes then use SeriesSketches and merge the results from other
// indexes.
func (i *Index) SeriesN() int64 {
	fs := i.RetainFileSet()
	defer fs.Release()

	var total int64
	for _, f := range fs {
		total += int64(f.SeriesN())
	}
	return total
}

// Dereference is a nop.
func (i *Index) Dereference([]byte) {}

// HasTagKey returns true if tag key exists.
func (i *Index) HasTagKey(name, key []byte) (bool, error) {
	fs := i.RetainFileSet()
	defer fs.Release()
	return fs.HasTagKey(name, key), nil
}

// MeasurementTagKeysByExpr extracts the tag keys wanted by the expression.
func (i *Index) MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) {
	fs := i.RetainFileSet()
	defer fs.Release()
	return fs.MeasurementTagKeysByExpr(name, expr)
}

// ForEachMeasurementSeriesByExpr iterates over all series in a measurement filtered by an expression.
func (i *Index) ForEachMeasurementSeriesByExpr(name []byte, condition influxql.Expr, fn func(tags models.Tags) error) error {
	fs := i.RetainFileSet()
	defer fs.Release()

	itr, err := fs.MeasurementSeriesByExprIterator(name, condition, i.fieldset)
	if err != nil {
		return err
	} else if itr == nil {
		return nil
	}

	for e := itr.Next(); e != nil; e = itr.Next() {
		if err := fn(e.Tags()); err != nil {
			return err
		}
	}

	return nil
}

// ForEachMeasurementTagKey iterates over all tag keys in a measurement.
func (i *Index) ForEachMeasurementTagKey(name []byte, fn func(key []byte) error) error {
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

// MeasurementSeriesKeysByExpr returns a list of series keys matching expr.
func (i *Index) MeasurementSeriesKeysByExpr(name []byte, expr influxql.Expr) ([][]byte, error) {
	fs := i.RetainFileSet()
	defer fs.Release()
	return fs.MeasurementSeriesKeysByExpr(name, expr, i.fieldset)
}

// TagSets returns an ordered list of tag sets for a measurement by dimension
// and filtered by an optional conditional expression.
func (i *Index) TagSets(name []byte, dimensions []string, condition influxql.Expr) ([]*influxql.TagSet, error) {
	fs := i.RetainFileSet()
	defer fs.Release()

	itr, err := fs.MeasurementSeriesByExprIterator(name, condition, i.fieldset)
	if err != nil {
		return nil, err
	} else if itr == nil {
		return nil, nil
	}

	// For every series, get the tag values for the requested tag keys i.e.
	// dimensions. This is the TagSet for that series. Series with the same
	// TagSet are then grouped together, because for the purpose of GROUP BY
	// they are part of the same composite series.
	tagSets := make(map[string]*influxql.TagSet, 64)

	if itr != nil {
		for e := itr.Next(); e != nil; e = itr.Next() {
			tags := make(map[string]string, len(dimensions))

			// Build the TagSet for this series.
			for _, dim := range dimensions {
				tags[dim] = e.Tags().GetString(dim)
			}

			// Convert the TagSet to a string, so it can be added to a map
			// allowing TagSets to be handled as a set.
			tagsAsKey := tsdb.MarshalTags(tags)
			tagSet, ok := tagSets[string(tagsAsKey)]
			if !ok {
				// This TagSet is new, create a new entry for it.
				tagSet = &influxql.TagSet{
					Tags: tags,
					Key:  tagsAsKey,
				}
			}
			// Associate the series and filter with the Tagset.
			tagSet.AddFilter(string(SeriesElemKey(e)), e.Expr())

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
	sortedTagsSets := make([]*influxql.TagSet, 0, len(tagSets))
	for _, v := range tagSets {
		sortedTagsSets = append(sortedTagsSets, v)
	}
	sort.Sort(byTagKey(sortedTagsSets))

	return sortedTagsSets, nil
}

// SnapshotTo creates hard links to the file set into path.
func (i *Index) SnapshotTo(path string) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	fs := i.retainFileSet()
	defer fs.Release()

	if err := os.Mkdir(filepath.Join(path, "index"), 0777); err != nil {
		return err
	}

	for _, f := range fs {
		if err := os.Link(f.Path(), filepath.Join(path, "index", filepath.Base(f.Path()))); err != nil {
			return fmt.Errorf("error creating tsi hard link: %q", err)
		}
	}

	return nil
}

func (i *Index) SetFieldName(measurement, name string) {}
func (i *Index) RemoveShard(shardID uint64)            {}
func (i *Index) AssignShard(k string, shardID uint64)  {}

func (i *Index) UnassignShard(k string, shardID uint64) error {
	// This can be called directly once inmem is gone.
	return i.DropSeries([][]byte{[]byte(k)})
}

// SeriesPointIterator returns an influxql iterator over all series.
func (i *Index) SeriesPointIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	// NOTE: The iterator handles releasing the file set.
	fs := i.RetainFileSet()
	return newSeriesPointIterator(fs, i.fieldset, opt), nil
}

// Compact runs a compaction check. Returns once the check is complete.
// If force is true then all files are compacted into a single index file regardless of size.
func (i *Index) Compact(force bool) error {
	info := compactNotify{force: force, ch: make(chan error)}
	i.manualCompactNotify <- info

	select {
	case err := <-info.ch:
		return err
	case <-i.closing:
		return nil
	}
}

// monitorCompaction periodically checks for files that need to be compacted.
func (i *Index) monitorCompaction() {
	// Ignore full compaction if interval is unset.
	var c <-chan time.Time
	if i.CompactionMonitorInterval > 0 {
		ticker := time.NewTicker(i.CompactionMonitorInterval)
		c = ticker.C
		defer ticker.Stop()
	}

	// Wait for compaction checks or for the index to close.
	for {
		select {
		case <-i.closing:
			return
		case <-i.fastCompactNotify:
			if err := i.compactLogFile(); err != nil {
				log.Printf("fast compaction error: %s", err)
			}
		case <-c:
			if err := i.checkFullCompaction(false); err != nil {
				log.Printf("full compaction error: %s", err)
			}
		case info := <-i.manualCompactNotify:
			if err := i.compactLogFile(); err != nil {
				info.ch <- err
				continue
			} else if err := i.checkFullCompaction(info.force); err != nil {
				info.ch <- err
				continue
			}
			info.ch <- nil
		}
	}
}

// compactLogFile starts a new log file and compacts the previous one.
func (i *Index) compactLogFile() error {
	start := time.Now()

	// Deactivate current log file & determine next file id.
	i.mu.Lock()
	logFile := i.activeLogFile
	i.activeLogFile = nil
	id := i.maxFileID() + 1
	i.mu.Unlock()

	// Exit if there is no active log file.
	if logFile == nil {
		return nil
	}

	log.Printf("tsi1: compacting log file: file=%s", logFile.Path)

	// Create new index file.
	path := filepath.Join(i.Path, FormatIndexFileName(id))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Compact log file to new index file.
	if _, err := logFile.WriteTo(f); err != nil {
		return err
	}

	// Close file.
	if err := f.Close(); err != nil {
		return err
	}

	// Reopen as an index file.
	file := NewIndexFile()
	file.SetPath(path)
	if err := file.Open(); err != nil {
		return err
	}

	// Obtain lock to swap in index file and write manifest.
	if err := func() error {
		i.mu.Lock()
		defer i.mu.Unlock()

		// Only use active log file, if one was created during compaction.
		if i.activeLogFile != nil {
			i.logFiles = []*LogFile{i.activeLogFile}
		} else {
			i.logFiles = nil
		}

		// Prepend new index file.
		i.indexFiles = append(IndexFiles{file}, i.indexFiles...)
		i.buildFileSet()

		// Write new manifest.
		if err := i.writeManifestFile(); err != nil {
			// TODO: Close index if write fails.
			return err
		}
		return nil
	}(); err != nil {
		return err
	}
	log.Printf("tsi1: finished compacting log file: file=%s, t=%s", logFile.Path, time.Since(start))

	// Closing the log file will automatically wait until the ref count is zero.
	log.Printf("tsi1: removing log file: file=%s", logFile.Path)
	if err := logFile.Close(); err != nil {
		return err
	} else if err := os.Remove(logFile.Path()); err != nil {
		return err
	}

	return nil
}

// checkFullCompaction compacts all index files if the total size of index files
// is double the size of the largest index file. If force is true then all files
// are compacted regardless of size.
func (i *Index) checkFullCompaction(force bool) error {
	start := time.Now()

	// Only perform size check if compaction check is not forced.
	if !force {
		// Calculate total & max file sizes.
		maxN, totalN, modTime, err := i.indexFileStats()
		if err != nil {
			return err
		}

		// Ignore if largest file is larger than all other files.
		// Perform compaction if last mod time of all files is above threshold.
		if (totalN-maxN) < maxN && time.Since(modTime) < i.MaxIndexFileIdleDuration {
			return nil
		}
	}

	// Retrieve list of index files under lock.
	i.mu.Lock()
	oldIndexFiles := i.indexFiles
	id := i.maxFileID() + 1
	i.mu.Unlock()

	// Ignore if there are not at least two index files.
	if len(oldIndexFiles) < 2 {
		return nil
	}

	// Create new index file.
	path := filepath.Join(i.Path, FormatIndexFileName(id))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	log.Printf("tsi1: performing full compaction: file=%s", path)

	// Compact all index files to new index file.
	if _, err := oldIndexFiles.WriteTo(f); err != nil {
		return err
	}

	// Close file.
	if err := f.Close(); err != nil {
		return err
	}

	// Reopen as an index file.
	file := NewIndexFile()
	file.SetPath(path)
	if err := file.Open(); err != nil {
		return err
	}

	// Obtain lock to swap in index file and write manifest.
	if err := func() error {
		i.mu.Lock()
		defer i.mu.Unlock()

		// Replace index files with new index file.
		i.indexFiles = IndexFiles{file}
		i.buildFileSet()

		// Write new manifest.
		if err := i.writeManifestFile(); err != nil {
			// TODO: Close index if write fails.
			return err
		}
		return nil
	}(); err != nil {
		return err
	}

	log.Printf("tsi1: full compaction complete: file=%s, t=%s", path, time.Since(start))

	// Close and delete all old index files.
	for _, f := range oldIndexFiles {
		log.Printf("tsi1: removing index file: file=%s", f.Path)

		if err := f.Close(); err != nil {
			return err
		} else if err := os.Remove(f.Path()); err != nil {
			return err
		}
	}

	return nil
}

// indexFileStats returns the max index file size and the total file size for all index files.
func (i *Index) indexFileStats() (maxN, totalN int64, modTime time.Time, err error) {
	// Retrieve index file list under lock.
	i.mu.Lock()
	indexFiles := i.indexFiles
	i.mu.Unlock()

	// Iterate over each file and determine size.
	for _, f := range indexFiles {
		fi, err := os.Stat(f.Path())
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			return 0, 0, time.Time{}, err
		}

		if fi.Size() > maxN {
			maxN = fi.Size()
		}
		if fi.ModTime().After(modTime) {
			modTime = fi.ModTime()
		}

		totalN += fi.Size()
	}
	return maxN, totalN, modTime, nil
}

// CheckFastCompaction notifies the index to begin compacting log file if the
// log file is above the max log file size.
func (i *Index) CheckFastCompaction() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.checkFastCompaction()
}

func (i *Index) checkFastCompaction() {
	if len(i.logFiles) == 0 {
		return
	}

	// Ignore fast compaction if size and idle time are within threshold.
	size, modTime := i.logFiles[0].Stat()
	if size < i.MaxLogFileSize && time.Since(modTime) < i.MaxLogFileIdleDuration {
		return
	}

	// Send signal to begin compaction of current log file.
	select {
	case i.fastCompactNotify <- struct{}{}:
	default:
	}
}

// compactNotify represents a manual compaction notification.
type compactNotify struct {
	force bool
	ch    chan error
}

// seriesPointIterator adapts SeriesIterator to an influxql.Iterator.
type seriesPointIterator struct {
	once     sync.Once
	fs       FileSet
	fieldset *tsdb.MeasurementFieldSet
	mitr     MeasurementIterator
	sitr     SeriesIterator
	opt      influxql.IteratorOptions

	point influxql.FloatPoint // reusable point
}

// newSeriesPointIterator returns a new instance of seriesPointIterator.
func newSeriesPointIterator(fs FileSet, fieldset *tsdb.MeasurementFieldSet, opt influxql.IteratorOptions) *seriesPointIterator {
	return &seriesPointIterator{
		fs:       fs,
		fieldset: fieldset,
		mitr:     fs.MeasurementIterator(),
		point: influxql.FloatPoint{
			Aux: make([]interface{}, len(opt.Aux)),
		},
		opt: opt,
	}
}

// Stats returns stats about the points processed.
func (itr *seriesPointIterator) Stats() influxql.IteratorStats { return influxql.IteratorStats{} }

// Close closes the iterator.
func (itr *seriesPointIterator) Close() error {
	itr.once.Do(func() { itr.fs.Release() })
	return nil
}

// Next emits the next point in the iterator.
func (itr *seriesPointIterator) Next() (*influxql.FloatPoint, error) {
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
		e := itr.sitr.Next()
		if e == nil {
			itr.sitr = nil
			continue
		}

		// Convert to a key.
		key := string(models.MakeKey(e.Name(), e.Tags()))

		// Write auxiliary fields.
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

var fileIDRegex = regexp.MustCompile(`^(\d+)\..+$`)

// ParseFileID extracts the numeric id from a log or index file path.
// Returns 0 if it cannot be parsed.
func ParseFileID(name string) int {
	a := fileIDRegex.FindStringSubmatch(filepath.Base(name))
	if a == nil {
		return 0
	}

	i, _ := strconv.Atoi(a[1])
	return i
}

// Manifest represents the list of log & index files that make up the index.
// The files are listed in time order, not necessarily ID order.
type Manifest struct {
	LogFiles   []string `json:"logs,omitempty"`
	IndexFiles []string `json:"indexes,omitempty"`
}

// HasFile returns true if name is listed in the log files or index files.
func (m *Manifest) HasFile(name string) bool {
	for _, filename := range m.LogFiles {
		if filename == name {
			return true
		}
	}
	for _, filename := range m.IndexFiles {
		if filename == name {
			return true
		}
	}
	return false
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
