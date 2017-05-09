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
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/tsdb"
)

// IndexName is the name of the index.
const IndexName = "tsi1"

// Default compaction thresholds.
const (
	DefaultMaxLogFileSize   = 5 * 1024 * 1024
	DefaultCompactionFactor = 1.8
)

func init() {
	tsdb.RegisterIndex(IndexName, func(id uint64, path string, opt tsdb.EngineOptions) tsdb.Index {
		idx := NewIndex()
		idx.ShardID = id
		idx.Path = path
		idx.options = opt
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
	mu      sync.RWMutex
	opened  bool
	options tsdb.EngineOptions

	activeLogFile *LogFile // current log file
	fileSet       FileSet  // current file set
	seq           int      // file id sequence

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
	MaxLogFileSize   int64
	CompactionFactor float64

	// Frequency of compaction checks.
	CompactionEnabled         bool
	CompactionMonitorInterval time.Duration
}

// NewIndex returns a new instance of Index.
func NewIndex() *Index {
	return &Index{
		closing: make(chan struct{}),

		// Default compaction thresholds.
		MaxLogFileSize:    DefaultMaxLogFileSize,
		CompactionEnabled: true,
		CompactionFactor:  DefaultCompactionFactor,
	}
}

func (i *Index) Type() string { return IndexName }

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

	// Open each file in the manifest.
	for _, filename := range m.Files {
		switch filepath.Ext(filename) {
		case LogFileExt:
			f, err := i.openLogFile(filepath.Join(i.Path, filename))
			if err != nil {
				return err
			}
			i.fileSet = append(i.fileSet, f)

			// Make first log file active, if within threshold.
			sz, _ := f.Stat()
			if i.activeLogFile == nil && sz < i.MaxLogFileSize {
				i.activeLogFile = f
			}

		case IndexFileExt:
			f, err := i.openIndexFile(filepath.Join(i.Path, filename))
			if err != nil {
				return err
			}
			i.fileSet = append(i.fileSet, f)
		}
	}

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
func (i *Index) openLogFile(path string) (*LogFile, error) {
	f := NewLogFile(path)
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
	for _, f := range i.fileSet {
		f.Close()
	}
	i.fileSet = nil

	return nil
}

// NextSequence returns the next file identifier.
func (i *Index) NextSequence() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.nextSequence()
}

func (i *Index) nextSequence() int {
	i.seq++
	return i.seq
}

// ManifestPath returns the path to the index's manifest file.
func (i *Index) ManifestPath() string {
	return filepath.Join(i.Path, ManifestFileName)
}

// Manifest returns a manifest for the index.
func (i *Index) Manifest() *Manifest {
	m := &Manifest{
		Files: make([]string, len(i.fileSet)),
	}

	for j, f := range i.fileSet {
		m.Files[j] = filepath.Base(f.Path())
	}

	return m
}

// writeManifestFile writes the manifest to the appropriate file path.
func (i *Index) writeManifestFile() error {
	return WriteManifestFile(i.ManifestPath(), i.Manifest())
}

// SetFieldSet sets a shared field set from the engine.
func (i *Index) SetFieldSet(fs *tsdb.MeasurementFieldSet) {
	i.mu.Lock()
	i.fieldset = fs
	i.mu.Unlock()
}

// RetainFileSet returns the current fileset and adds a reference count.
func (i *Index) RetainFileSet() FileSet {
	i.mu.RLock()
	fs := i.retainFileSet()
	i.mu.RUnlock()
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
	// Open file and insert it into the first position.
	f, err := i.openLogFile(filepath.Join(i.Path, FormatLogFileName(i.nextSequence())))
	if err != nil {
		return err
	}
	i.activeLogFile = f
	i.fileSet = append([]File{f}, i.fileSet...)

	// Write new manifest.
	if err := i.writeManifestFile(); err != nil {
		// TODO: Close index if write fails.
		return err
	}

	return nil
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
	if sitr := fs.MeasurementSeriesIterator(name); sitr != nil {
		for s := sitr.Next(); s != nil; s = sitr.Next() {
			if !s.Deleted() {
				if err := func() error {
					i.mu.RLock()
					defer i.mu.RUnlock()
					return i.activeLogFile.DeleteSeries(s.Name(), s.Tags())
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
func (i *Index) CreateSeriesListIfNotExists(_, names [][]byte, tagsSlice []models.Tags) error {
	// All slices must be of equal length.
	if len(names) != len(tagsSlice) {
		return errors.New("names/tags length mismatch")
	}

	// Maintain reference count on files in file set.
	fs := i.RetainFileSet()
	defer fs.Release()

	// Filter out existing series. Exit if no new series exist.
	names, tagsSlice = fs.FilterNamesTags(names, tagsSlice)
	if len(names) == 0 {
		return nil
	}

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
func (i *Index) InitializeSeries(key, name []byte, tags models.Tags) error {
	return nil
}

// CreateSeriesIfNotExists creates a series if it doesn't exist or is deleted.
func (i *Index) CreateSeriesIfNotExists(key, name []byte, tags models.Tags) error {
	if err := func() error {
		i.mu.RLock()
		defer i.mu.RUnlock()

		fs := i.retainFileSet()
		defer fs.Release()

		if fs.HasSeries(name, tags, nil) {
			return nil
		}

		if err := i.activeLogFile.AddSeries(name, tags); err != nil {
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

func (i *Index) DropSeries(key []byte) error {
	if err := func() error {
		i.mu.RLock()
		defer i.mu.RUnlock()

		name, tags, err := models.ParseKey(key)
		if err != nil {
			return err
		}

		mname := []byte(name)
		if err := i.activeLogFile.DeleteSeries(mname, tags); err != nil {
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

// TagKeyCardinality always returns zero.
// It is not possible to determine cardinality of tags across index files.
func (i *Index) TagKeyCardinality(name, key []byte) int {
	return 0
}

// MeasurementSeriesKeysByExpr returns a list of series keys matching expr.
func (i *Index) MeasurementSeriesKeysByExpr(name []byte, expr influxql.Expr) ([][]byte, error) {
	fs := i.RetainFileSet()
	defer fs.Release()
	return fs.MeasurementSeriesKeysByExpr(name, expr, i.fieldset)
}

// TagSets returns an ordered list of tag sets for a measurement by dimension
// and filtered by an optional conditional expression.
func (i *Index) TagSets(name []byte, opt influxql.IteratorOptions) ([]*influxql.TagSet, error) {
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
	tagSets := make(map[string]*influxql.TagSet, 64)

	if itr != nil {
		for e := itr.Next(); e != nil; e = itr.Next() {
			tags := make(map[string]string, len(opt.Dimensions))

			// Build the TagSet for this series.
			for _, dim := range opt.Dimensions {
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

	// Flush active log file, if any.
	if err := i.activeLogFile.Flush(); err != nil {
		return err
	}

	if err := os.Mkdir(filepath.Join(path, "index"), 0777); err != nil {
		return err
	}

	// Link manifest.
	if err := os.Link(i.ManifestPath(), filepath.Join(path, "index", filepath.Base(i.ManifestPath()))); err != nil {
		return fmt.Errorf("error creating tsi manifest hard link: %q", err)
	}

	// Link files in directory.
	for _, f := range fs {
		if err := os.Link(f.Path(), filepath.Join(path, "index", filepath.Base(f.Path()))); err != nil {
			return fmt.Errorf("error creating tsi hard link: %q", err)
		}
	}

	return nil
}

func (i *Index) SetFieldName(measurement []byte, name string) {}
func (i *Index) RemoveShard(shardID uint64)                   {}
func (i *Index) AssignShard(k string, shardID uint64)         {}

func (i *Index) UnassignShard(k string, shardID uint64) error {
	// This can be called directly once inmem is gone.
	return i.DropSeries([]byte(k))
}

// SeriesPointIterator returns an influxql iterator over all series.
func (i *Index) SeriesPointIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	// NOTE: The iterator handles releasing the file set.
	fs := i.RetainFileSet()
	return newSeriesPointIterator(fs, i.fieldset, opt), nil
}

// Compact requests a compaction of log files.
func (i *Index) Compact() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.compact()
}

// compact compacts continguous groups of files that are not currently compacting.
func (i *Index) compact() {
	if !i.CompactionEnabled {
		return
	}

	fs := i.retainFileSet()
	defer fs.Release()

	// Return contiguous groups of files that are available for compaction.
	for _, group := range i.compactionGroups(fs) {
		// Mark files in group as compacting.
		for _, f := range group {
			f.Retain()
			f.setCompacting(true)
		}

		// Execute in closure to save reference to the group within the loop.
		func(group []*IndexFile) {
			// Start compacting in a separate goroutine.
			i.wg.Add(1)
			go func() {
				defer i.wg.Done()
				i.compactGroup(group)
				i.Compact() // check for new compactions
			}()
		}(group)
	}
}

// compactionGroups returns contiguous groups of index files that can be compacted.
//
// All groups will have at least two files and the total size is more than the
// largest file times the compaction factor. For example, if the compaction
// factor is 2 then the total size will be at least double the max file size.
func (i *Index) compactionGroups(fileSet FileSet) [][]*IndexFile {
	log.Printf("%s: checking for compaction groups: n=%d", IndexName, len(fileSet))

	var groups [][]*IndexFile

	// Loop over all files to find contiguous group of compactable files.
	var group []*IndexFile
	for _, f := range fileSet {
		indexFile, ok := f.(*IndexFile)

		// Skip over log files. They compact themselves.
		if !ok {
			if isCompactableGroup(group, i.CompactionFactor) {
				group, groups = nil, append(groups, group)
			} else {
				group = nil
			}
			continue
		}

		// If file is currently compacting then stop current group.
		if indexFile.Compacting() {
			if isCompactableGroup(group, i.CompactionFactor) {
				group, groups = nil, append(groups, group)
			} else {
				group = nil
			}
			continue
		}

		// Stop current group if adding file will invalidate group.
		// This can happen when appending a large file to a group of small files.
		if isCompactableGroup(group, i.CompactionFactor) && !isCompactableGroup(append(group, indexFile), i.CompactionFactor) {
			group, groups = []*IndexFile{indexFile}, append(groups, group)
			continue
		}

		// Otherwise append to the current group.
		group = append(group, indexFile)
	}

	// Append final group, if compactable.
	if isCompactableGroup(group, i.CompactionFactor) {
		groups = append(groups, group)
	}

	return groups
}

// isCompactableGroup returns true if total file size is greater than max file size times factor.
func isCompactableGroup(files []*IndexFile, factor float64) bool {
	if len(files) < 2 {
		return false
	}

	var max, total int64
	for _, f := range files {
		sz := f.Size()
		if sz > max {
			max = sz
		}
		total += sz
	}
	return total >= int64(float64(max)*factor)
}

// compactGroup compacts files into a new file. Replaces old files with
// compacted file on successful completion. This runs in a separate goroutine.
func (i *Index) compactGroup(files []*IndexFile) {
	assert(len(files) >= 2, "at least two index files are required for compaction")

	// Files have already been retained by caller.
	// Ensure files are released only once.
	var once sync.Once
	defer once.Do(func() { IndexFiles(files).Release() })

	// Track time to compact.
	start := time.Now()

	// Create new index file.
	path := filepath.Join(i.Path, FormatIndexFileName(i.NextSequence()))
	f, err := os.Create(path)
	if err != nil {
		log.Printf("%s: error creating compaction files: %s", IndexName, err)
		return
	}
	defer f.Close()

	srcIDs := joinIntSlice(IndexFiles(files).IDs(), ",")
	log.Printf("%s: performing full compaction: src=%s, path=%s", IndexName, srcIDs, path)

	// Compact all index files to new index file.
	n, err := IndexFiles(files).WriteTo(f)
	if err != nil {
		log.Printf("%s: error compacting index files: src=%s, path=%s, err=%s", IndexName, srcIDs, path, err)
		return
	}

	// Close file.
	if err := f.Close(); err != nil {
		log.Printf("%s: error closing index file: %s", IndexName, err)
		return
	}

	// Reopen as an index file.
	file := NewIndexFile()
	file.SetPath(path)
	if err := file.Open(); err != nil {
		log.Printf("%s: error opening new index file: %s", IndexName, err)
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
		log.Printf("%s: error writing manifest: %s", IndexName, err)
		return
	}
	log.Printf("%s: full compaction complete: file=%s, t=%s, sz=%d", IndexName, path, time.Since(start), n)

	// Release old files.
	once.Do(func() { IndexFiles(files).Release() })

	// Close and delete all old index files.
	for _, f := range files {
		log.Printf("%s: removing index file: file=%s", IndexName, f.Path())

		if err := f.Close(); err != nil {
			log.Printf("%s: error closing index file: %s", IndexName, err)
			return
		} else if err := os.Remove(f.Path()); err != nil {
			log.Printf("%s: error removing index file: %s", IndexName, err)
			return
		}
	}
}

func (i *Index) CheckLogFile() error {
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

func (i *Index) checkLogFile() error {
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
func (i *Index) compactLogFile(logFile *LogFile) {
	start := time.Now()
	log.Printf("tsi1: compacting log file: file=%s", logFile.Path())

	// Retrieve identifier from current path.
	id := ParseFileID(logFile.Path())
	assert(id != 0, "cannot parse log file id: %s", logFile.Path())

	// Create new index file.
	path := filepath.Join(i.Path, FormatIndexFileName(id))
	f, err := os.Create(path)
	if err != nil {
		log.Printf("tsi1: error creating index file: %s", err)
		return
	}
	defer f.Close()

	// Compact log file to new index file.
	n, err := logFile.WriteTo(f)
	if err != nil {
		log.Printf("%s: error compacting log file: path=%s, err=%s", IndexName, logFile.Path(), err)
		return
	}

	// Close file.
	if err := f.Close(); err != nil {
		log.Printf("tsi1: error closing log file: %s", err)
		return
	}

	// Reopen as an index file.
	file := NewIndexFile()
	file.SetPath(path)
	if err := file.Open(); err != nil {
		log.Printf("tsi1: error opening compacted index file: path=%s, err=%s", file.Path(), err)
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
		log.Printf("%s: error updating manifest: %s", IndexName, err)
		return
	}
	log.Printf("%s: finished compacting log file: file=%s, t=%v, sz=%d", IndexName, logFile.Path(), time.Since(start), n)

	// Closing the log file will automatically wait until the ref count is zero.
	log.Printf("%s: removing log file: file=%s", IndexName, logFile.Path())
	if err := logFile.Close(); err != nil {
		log.Printf("%s: error closing log file: %s", IndexName, err)
		return
	} else if err := os.Remove(logFile.Path()); err != nil {
		log.Printf("%s: error removing log file: %s", IndexName, err)
		return
	}

	return
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
	Files []string `json:"files,omitempty"`
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
