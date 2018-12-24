package tsi1

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/bytesutil"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

// Version is the current version of the TSI index.
const Version = 1

// File extensions.
const (
	LogFileExt   = ".tsl"
	IndexFileExt = ".tsi"

	CompactingExt = ".compacting"
)

// ManifestFileName is the name of the index manifest file.
const ManifestFileName = "MANIFEST"

// Partition represents a collection of layered index files and WAL.
type Partition struct {
	mu     sync.RWMutex
	opened bool

	sfile         *tsdb.SeriesFile // series lookup file
	activeLogFile *LogFile         // current log file
	fileSet       *FileSet         // current file set
	seq           int              // file id sequence

	// Fast series lookup of series IDs in the series file that have been present
	// in this partition. This set tracks both insertions and deletions of a series.
	seriesIDSet *tsdb.SeriesIDSet

	// Compaction management
	levels          []CompactionLevel // compaction levels
	levelCompacting []bool            // level compaction status

	// Close management.
	once    sync.Once
	closing chan struct{} // closing is used to inform iterators the partition is closing.
	wg      sync.WaitGroup

	// Fieldset shared with engine.
	fieldset *tsdb.MeasurementFieldSet

	// Directory of the Partition's index files.
	path string
	id   string // id portion of path.

	// Log file compaction thresholds.
	MaxLogFileSize int64
	nosync         bool // when true, flushing and syncing of LogFile will be disabled.
	logbufferSize  int  // the LogFile's buffer is set to this value.

	// Frequency of compaction checks.
	compactionInterrupt chan struct{}
	compactionsDisabled int

	logger *zap.Logger

	// Current size of MANIFEST. Used to determine partition size.
	manifestSize int64

	// Index's version.
	version int
}

// NewPartition returns a new instance of Partition.
func NewPartition(sfile *tsdb.SeriesFile, path string) *Partition {
	return &Partition{
		closing:     make(chan struct{}),
		path:        path,
		sfile:       sfile,
		seriesIDSet: tsdb.NewSeriesIDSet(),

		MaxLogFileSize: tsdb.DefaultMaxIndexLogFileSize,

		// compactionEnabled: true,
		compactionInterrupt: make(chan struct{}),

		logger:  zap.NewNop(),
		version: Version,
	}
}

// bytes estimates the memory footprint of this Partition, in bytes.
func (p *Partition) bytes() int {
	var b int
	b += 24 // mu RWMutex is 24 bytes
	b += int(unsafe.Sizeof(p.opened))
	// Do not count SeriesFile because it belongs to the code that constructed this Partition.
	b += int(unsafe.Sizeof(p.activeLogFile)) + p.activeLogFile.bytes()
	b += int(unsafe.Sizeof(p.fileSet)) + p.fileSet.bytes()
	b += int(unsafe.Sizeof(p.seq))
	b += int(unsafe.Sizeof(p.seriesIDSet)) + p.seriesIDSet.Bytes()
	b += int(unsafe.Sizeof(p.levels))
	for _, level := range p.levels {
		b += int(unsafe.Sizeof(level))
	}
	b += int(unsafe.Sizeof(p.levelCompacting))
	for _, levelCompacting := range p.levelCompacting {
		b += int(unsafe.Sizeof(levelCompacting))
	}
	b += 12 // once sync.Once is 12 bytes
	b += int(unsafe.Sizeof(p.closing))
	b += 16 // wg sync.WaitGroup is 16 bytes
	b += int(unsafe.Sizeof(p.fieldset)) + p.fieldset.Bytes()
	b += int(unsafe.Sizeof(p.path)) + len(p.path)
	b += int(unsafe.Sizeof(p.id)) + len(p.id)
	b += int(unsafe.Sizeof(p.MaxLogFileSize))
	b += int(unsafe.Sizeof(p.compactionInterrupt))
	b += int(unsafe.Sizeof(p.compactionsDisabled))
	b += int(unsafe.Sizeof(p.logger))
	b += int(unsafe.Sizeof(p.manifestSize))
	b += int(unsafe.Sizeof(p.version))
	return b
}

// ErrIncompatibleVersion is returned when attempting to read from an
// incompatible tsi1 manifest file.
var ErrIncompatibleVersion = errors.New("incompatible tsi1 index MANIFEST")

// Open opens the partition.
func (p *Partition) Open() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closing = make(chan struct{})

	if p.opened {
		return errors.New("index partition already open")
	}

	// Validate path is correct.
	p.id = filepath.Base(p.path)
	_, err := strconv.Atoi(p.id)
	if err != nil {
		return err
	}

	// Create directory if it doesn't exist.
	if err := os.MkdirAll(p.path, 0777); err != nil {
		return err
	}

	// Read manifest file.
	m, manifestSize, err := ReadManifestFile(filepath.Join(p.path, ManifestFileName))
	if os.IsNotExist(err) {
		m = NewManifest(p.ManifestPath())
	} else if err != nil {
		return err
	}
	// Set manifest size on the partition
	p.manifestSize = manifestSize

	// Check to see if the MANIFEST file is compatible with the current Index.
	if err := m.Validate(); err != nil {
		return err
	}

	// Copy compaction levels to the index.
	p.levels = make([]CompactionLevel, len(m.Levels))
	copy(p.levels, m.Levels)

	// Set up flags to track whether a level is compacting.
	p.levelCompacting = make([]bool, len(p.levels))

	// Open each file in the manifest.
	var files []File
	for _, filename := range m.Files {
		switch filepath.Ext(filename) {
		case LogFileExt:
			f, err := p.openLogFile(filepath.Join(p.path, filename))
			if err != nil {
				return err
			}
			files = append(files, f)

			// Make first log file active, if within threshold.
			sz, _ := f.Stat()
			if p.activeLogFile == nil && sz < p.MaxLogFileSize {
				p.activeLogFile = f
			}

		case IndexFileExt:
			f, err := p.openIndexFile(filepath.Join(p.path, filename))
			if err != nil {
				return err
			}
			files = append(files, f)
		}
	}
	fs, err := NewFileSet(p.levels, p.sfile, files)
	if err != nil {
		return err
	}
	p.fileSet = fs

	// Set initial sequence number.
	p.seq = p.fileSet.MaxID()

	// Delete any files not in the manifest.
	if err := p.deleteNonManifestFiles(m); err != nil {
		return err
	}

	// Ensure a log file exists.
	if p.activeLogFile == nil {
		if err := p.prependActiveLogFile(); err != nil {
			return err
		}
	}

	// Build series existance set.
	if err := p.buildSeriesSet(); err != nil {
		return err
	}

	// Mark opened.
	p.opened = true

	// Send a compaction request on start up.
	p.compact()

	return nil
}

// openLogFile opens a log file and appends it to the index.
func (p *Partition) openLogFile(path string) (*LogFile, error) {
	f := NewLogFile(p.sfile, path)
	f.nosync = p.nosync
	f.bufferSize = p.logbufferSize

	if err := f.Open(); err != nil {
		return nil, err
	}
	return f, nil
}

// openIndexFile opens a log file and appends it to the index.
func (p *Partition) openIndexFile(path string) (*IndexFile, error) {
	f := NewIndexFile(p.sfile)
	f.SetPath(path)
	if err := f.Open(); err != nil {
		return nil, err
	}
	return f, nil
}

// deleteNonManifestFiles removes all files not in the manifest.
func (p *Partition) deleteNonManifestFiles(m *Manifest) error {
	dir, err := os.Open(p.path)
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

	return dir.Close()
}

func (p *Partition) buildSeriesSet() error {
	fs := p.retainFileSet()
	defer fs.Release()

	p.seriesIDSet = tsdb.NewSeriesIDSet()

	// Read series sets from files in reverse.
	for i := len(fs.files) - 1; i >= 0; i-- {
		f := fs.files[i]

		// Delete anything that's been tombstoned.
		ts, err := f.TombstoneSeriesIDSet()
		if err != nil {
			return err
		}
		p.seriesIDSet.Diff(ts)

		// Add series created within the file.
		ss, err := f.SeriesIDSet()
		if err != nil {
			return err
		}
		p.seriesIDSet.Merge(ss)
	}
	return nil
}

// Wait returns once outstanding compactions have finished.
func (p *Partition) Wait() {
	p.wg.Wait()
}

// Close closes the index.
func (p *Partition) Close() error {
	// Wait for goroutines to finish outstanding compactions.
	p.once.Do(func() {
		close(p.closing)
		close(p.compactionInterrupt)
	})
	p.wg.Wait()

	// Lock index and close remaining
	p.mu.Lock()
	defer p.mu.Unlock()

	var err error

	// Close log files.
	for _, f := range p.fileSet.files {
		if localErr := f.Close(); localErr != nil {
			err = localErr
		}
	}
	p.fileSet.files = nil

	return err
}

// closing returns true if the partition is currently closing. It does not require
// a lock so will always return to callers.
func (p *Partition) isClosing() bool {
	select {
	case <-p.closing:
		return true
	default:
		return false
	}
}

// Path returns the path to the partition.
func (p *Partition) Path() string { return p.path }

// SeriesFile returns the attached series file.
func (p *Partition) SeriesFile() *tsdb.SeriesFile { return p.sfile }

// NextSequence returns the next file identifier.
func (p *Partition) NextSequence() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.nextSequence()
}

func (p *Partition) nextSequence() int {
	p.seq++
	return p.seq
}

// ManifestPath returns the path to the index's manifest file.
func (p *Partition) ManifestPath() string {
	return filepath.Join(p.path, ManifestFileName)
}

// Manifest returns a manifest for the index.
func (p *Partition) Manifest() *Manifest {
	m := &Manifest{
		Levels:  p.levels,
		Files:   make([]string, len(p.fileSet.files)),
		Version: p.version,
		path:    p.ManifestPath(),
	}

	for j, f := range p.fileSet.files {
		m.Files[j] = filepath.Base(f.Path())
	}

	return m
}

// WithLogger sets the logger for the index.
func (p *Partition) WithLogger(logger *zap.Logger) {
	p.logger = logger.With(zap.String("index", "tsi"))
}

// SetFieldSet sets a shared field set from the engine.
func (p *Partition) SetFieldSet(fs *tsdb.MeasurementFieldSet) {
	p.mu.Lock()
	p.fieldset = fs
	p.mu.Unlock()
}

// FieldSet returns the fieldset.
func (p *Partition) FieldSet() *tsdb.MeasurementFieldSet {
	p.mu.Lock()
	fs := p.fieldset
	p.mu.Unlock()
	return fs
}

// RetainFileSet returns the current fileset and adds a reference count.
func (p *Partition) RetainFileSet() (*FileSet, error) {
	select {
	case <-p.closing:
		return nil, tsdb.ErrIndexClosing
	default:
		p.mu.RLock()
		defer p.mu.RUnlock()
		return p.retainFileSet(), nil
	}
}

func (p *Partition) retainFileSet() *FileSet {
	fs := p.fileSet
	fs.Retain()
	return fs
}

// FileN returns the active files in the file set.
func (p *Partition) FileN() int { return len(p.fileSet.files) }

// prependActiveLogFile adds a new log file so that the current log file can be compacted.
func (p *Partition) prependActiveLogFile() error {
	// Open file and insert it into the first position.
	f, err := p.openLogFile(filepath.Join(p.path, FormatLogFileName(p.nextSequence())))
	if err != nil {
		return err
	}
	p.activeLogFile = f

	// Prepend and generate new fileset.
	p.fileSet = p.fileSet.PrependLogFile(f)

	// Write new manifest.
	manifestSize, err := p.Manifest().Write()
	if err != nil {
		// TODO: Close index if write fails.
		return err
	}
	p.manifestSize = manifestSize
	return nil
}

// ForEachMeasurementName iterates over all measurement names in the index.
func (p *Partition) ForEachMeasurementName(fn func(name []byte) error) error {
	fs, err := p.RetainFileSet()
	if err != nil {
		return err
	}
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

// MeasurementHasSeries returns true if a measurement has at least one non-tombstoned series.
func (p *Partition) MeasurementHasSeries(name []byte) (bool, error) {
	fs, err := p.RetainFileSet()
	if err != nil {
		return false, err
	}
	defer fs.Release()

	for _, f := range fs.files {
		if f.MeasurementHasSeries(p.seriesIDSet, name) {
			return true, nil
		}
	}

	return false, nil
}

// MeasurementIterator returns an iterator over all measurement names.
func (p *Partition) MeasurementIterator() (tsdb.MeasurementIterator, error) {
	fs, err := p.RetainFileSet()
	if err != nil {
		return nil, err
	}
	itr := fs.MeasurementIterator()
	if itr == nil {
		fs.Release()
		return nil, nil
	}
	return newFileSetMeasurementIterator(fs, NewTSDBMeasurementIteratorAdapter(itr)), nil
}

// MeasurementExists returns true if a measurement exists.
func (p *Partition) MeasurementExists(name []byte) (bool, error) {
	fs, err := p.RetainFileSet()
	if err != nil {
		return false, err
	}
	defer fs.Release()
	m := fs.Measurement(name)
	return m != nil && !m.Deleted(), nil
}

func (p *Partition) MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error) {
	fs, err := p.RetainFileSet()
	if err != nil {
		return nil, err
	}
	defer fs.Release()

	itr := fs.MeasurementIterator()
	if itr == nil {
		return nil, nil
	}

	var a [][]byte
	for e := itr.Next(); e != nil; e = itr.Next() {
		if re.Match(e.Name()) {
			// Clone bytes since they will be used after the fileset is released.
			a = append(a, bytesutil.Clone(e.Name()))
		}
	}
	return a, nil
}

func (p *Partition) MeasurementSeriesIDIterator(name []byte) (tsdb.SeriesIDIterator, error) {
	fs, err := p.RetainFileSet()
	if err != nil {
		return nil, err
	}
	return newFileSetSeriesIDIterator(fs, fs.MeasurementSeriesIDIterator(name)), nil
}

// DropMeasurement deletes a measurement from the index. DropMeasurement does
// not remove any series from the index directly.
func (p *Partition) DropMeasurement(name []byte) error {
	fs, err := p.RetainFileSet()
	if err != nil {
		return err
	}
	defer fs.Release()

	// Delete all keys and values.
	if kitr := fs.TagKeyIterator(name); kitr != nil {
		for k := kitr.Next(); k != nil; k = kitr.Next() {
			// Delete key if not already deleted.
			if !k.Deleted() {
				if err := func() error {
					p.mu.RLock()
					defer p.mu.RUnlock()
					return p.activeLogFile.DeleteTagKey(name, k.Key())
				}(); err != nil {
					return err
				}
			}

			// Delete each value in key.
			if vitr := k.TagValueIterator(); vitr != nil {
				for v := vitr.Next(); v != nil; v = vitr.Next() {
					if !v.Deleted() {
						if err := func() error {
							p.mu.RLock()
							defer p.mu.RUnlock()
							return p.activeLogFile.DeleteTagValue(name, k.Key(), v.Value())
						}(); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	// Delete all series.
	if itr := fs.MeasurementSeriesIDIterator(name); itr != nil {
		defer itr.Close()
		for {
			elem, err := itr.Next()
			if err != nil {
				return err
			} else if elem.SeriesID == 0 {
				break
			}
			if err := p.activeLogFile.DeleteSeriesID(elem.SeriesID); err != nil {
				return err
			}
		}
		if err = itr.Close(); err != nil {
			return err
		}
	}

	// Mark measurement as deleted.
	if err := func() error {
		p.mu.RLock()
		defer p.mu.RUnlock()
		return p.activeLogFile.DeleteMeasurement(name)
	}(); err != nil {
		return err
	}

	// Check if the log file needs to be swapped.
	if err := p.CheckLogFile(); err != nil {
		return err
	}

	return nil
}

// createSeriesListIfNotExists creates a list of series if they doesn't exist in
// bulk.
func (p *Partition) createSeriesListIfNotExists(names [][]byte, tagsSlice []models.Tags) ([]uint64, error) {
	// Is there anything to do? The partition may have been sent an empty batch.
	if len(names) == 0 {
		return nil, nil
	} else if len(names) != len(tagsSlice) {
		return nil, fmt.Errorf("uneven batch, partition %s sent %d names and %d tags", p.id, len(names), len(tagsSlice))
	}

	// Maintain reference count on files in file set.
	fs, err := p.RetainFileSet()
	if err != nil {
		return nil, err
	}
	defer fs.Release()

	// Ensure fileset cannot change during insert.
	p.mu.RLock()
	// Insert series into log file.
	ids, err := p.activeLogFile.AddSeriesList(p.seriesIDSet, names, tagsSlice)
	if err != nil {
		p.mu.RUnlock()
		return nil, err
	}
	p.mu.RUnlock()

	if err := p.CheckLogFile(); err != nil {
		return nil, err
	}
	return ids, nil
}

func (p *Partition) DropSeries(seriesID uint64) error {
	// Delete series from index.
	if err := p.activeLogFile.DeleteSeriesID(seriesID); err != nil {
		return err
	}

	p.seriesIDSet.Remove(seriesID)

	// Swap log file, if necessary.
	return p.CheckLogFile()
}

// MeasurementsSketches returns the two sketches for the partition by merging all
// instances of the type sketch types in all the index files.
func (p *Partition) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	fs, err := p.RetainFileSet()
	if err != nil {
		return nil, nil, err
	}
	defer fs.Release()
	return fs.MeasurementsSketches()
}

// SeriesSketches returns the two sketches for the partition by merging all
// instances of the type sketch types in all the index files.
func (p *Partition) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	fs, err := p.RetainFileSet()
	if err != nil {
		return nil, nil, err
	}
	defer fs.Release()
	return fs.SeriesSketches()
}

// HasTagKey returns true if tag key exists.
func (p *Partition) HasTagKey(name, key []byte) (bool, error) {
	fs, err := p.RetainFileSet()
	if err != nil {
		return false, err
	}
	defer fs.Release()
	return fs.HasTagKey(name, key), nil
}

// HasTagValue returns true if tag value exists.
func (p *Partition) HasTagValue(name, key, value []byte) (bool, error) {
	fs, err := p.RetainFileSet()
	if err != nil {
		return false, err
	}
	defer fs.Release()
	return fs.HasTagValue(name, key, value), nil
}

// TagKeyIterator returns an iterator for all keys across a single measurement.
func (p *Partition) TagKeyIterator(name []byte) tsdb.TagKeyIterator {
	fs, err := p.RetainFileSet()
	if err != nil {
		return nil // TODO(edd): this should probably return an error.
	}

	itr := fs.TagKeyIterator(name)
	if itr == nil {
		fs.Release()
		return nil
	}
	return newFileSetTagKeyIterator(fs, NewTSDBTagKeyIteratorAdapter(itr))
}

// TagValueIterator returns an iterator for all values across a single key.
func (p *Partition) TagValueIterator(name, key []byte) tsdb.TagValueIterator {
	fs, err := p.RetainFileSet()
	if err != nil {
		return nil // TODO(edd): this should probably return an error.
	}

	itr := fs.TagValueIterator(name, key)
	if itr == nil {
		fs.Release()
		return nil
	}
	return newFileSetTagValueIterator(fs, NewTSDBTagValueIteratorAdapter(itr))
}

// TagKeySeriesIDIterator returns a series iterator for all values across a single key.
func (p *Partition) TagKeySeriesIDIterator(name, key []byte) tsdb.SeriesIDIterator {
	fs, err := p.RetainFileSet()
	if err != nil {
		return nil // TODO(edd): this should probably return an error.
	}

	itr := fs.TagKeySeriesIDIterator(name, key)
	if itr == nil {
		fs.Release()
		return nil
	}
	return newFileSetSeriesIDIterator(fs, itr)
}

// TagValueSeriesIDIterator returns a series iterator for a single key value.
func (p *Partition) TagValueSeriesIDIterator(name, key, value []byte) (tsdb.SeriesIDIterator, error) {
	fs, err := p.RetainFileSet()
	if err != nil {
		return nil, err
	}

	itr, err := fs.TagValueSeriesIDIterator(name, key, value)
	if err != nil {
		return nil, err
	} else if itr == nil {
		fs.Release()
		return nil, nil
	}
	return newFileSetSeriesIDIterator(fs, itr), nil
}

// MeasurementTagKeysByExpr extracts the tag keys wanted by the expression.
func (p *Partition) MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) {
	fs, err := p.RetainFileSet()
	if err != nil {
		return nil, err
	}
	defer fs.Release()

	return fs.MeasurementTagKeysByExpr(name, expr)
}

// ForEachMeasurementTagKey iterates over all tag keys in a measurement.
func (p *Partition) ForEachMeasurementTagKey(name []byte, fn func(key []byte) error) error {
	fs, err := p.RetainFileSet()
	if err != nil {
		return err
	}
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
func (p *Partition) TagKeyCardinality(name, key []byte) int {
	return 0
}

func (p *Partition) SetFieldName(measurement []byte, name string) {}
func (p *Partition) RemoveShard(shardID uint64)                   {}
func (p *Partition) AssignShard(k string, shardID uint64)         {}

// Compact requests a compaction of log files.
func (p *Partition) Compact() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.compact()
}

func (p *Partition) DisableCompactions() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.compactionsDisabled++

	select {
	case <-p.closing:
		return
	default:
	}

	if p.compactionsDisabled == 0 {
		close(p.compactionInterrupt)
		p.compactionInterrupt = make(chan struct{})
	}
}

func (p *Partition) EnableCompactions() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Already enabled?
	if p.compactionsEnabled() {
		return
	}
	p.compactionsDisabled--
}

func (p *Partition) compactionsEnabled() bool {
	return p.compactionsDisabled == 0
}

// compact compacts continguous groups of files that are not currently compacting.
func (p *Partition) compact() {
	if p.isClosing() {
		return
	} else if !p.compactionsEnabled() {
		return
	}
	interrupt := p.compactionInterrupt

	fs := p.retainFileSet()
	defer fs.Release()

	// Iterate over each level we are going to compact.
	// We skip the first level (0) because it is log files and they are compacted separately.
	// We skip the last level because the files have no higher level to compact into.
	minLevel, maxLevel := 1, len(p.levels)-2
	for level := minLevel; level <= maxLevel; level++ {
		// Skip level if it is currently compacting.
		if p.levelCompacting[level] {
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
		p.levelCompacting[level] = true

		// Execute in closure to save reference to the group within the loop.
		func(files []*IndexFile, level int) {
			// Start compacting in a separate goroutine.
			p.wg.Add(1)
			go func() {

				// Compact to a new level.
				p.compactToLevel(files, level+1, interrupt)

				// Ensure compaction lock for the level is released.
				p.mu.Lock()
				p.levelCompacting[level] = false
				p.mu.Unlock()
				p.wg.Done()

				// Check for new compactions
				p.Compact()
			}()
		}(files, level)
	}
}

// compactToLevel compacts a set of files into a new file. Replaces old files with
// compacted file on successful completion. This runs in a separate goroutine.
func (p *Partition) compactToLevel(files []*IndexFile, level int, interrupt <-chan struct{}) {
	assert(len(files) >= 2, "at least two index files are required for compaction")
	assert(level > 0, "cannot compact level zero")

	// Build a logger for this compaction.
	log, logEnd := logger.NewOperation(p.logger, "TSI level compaction", "tsi1_compact_to_level", zap.Int("tsi1_level", level))
	defer logEnd()

	// Check for cancellation.
	select {
	case <-interrupt:
		log.Error("Cannot begin compaction", zap.Error(ErrCompactionInterrupted))
		return
	default:
	}

	// Files have already been retained by caller.
	// Ensure files are released only once.
	var once sync.Once
	defer once.Do(func() { IndexFiles(files).Release() })

	// Track time to compact.
	start := time.Now()

	// Create new index file.
	path := filepath.Join(p.path, FormatIndexFileName(p.NextSequence(), level))
	f, err := os.Create(path)
	if err != nil {
		log.Error("Cannot create compaction files", zap.Error(err))
		return
	}
	defer f.Close()

	log.Info("Performing full compaction",
		zap.String("src", joinIntSlice(IndexFiles(files).IDs(), ",")),
		zap.String("dst", path),
	)

	// Compact all index files to new index file.
	lvl := p.levels[level]
	n, err := IndexFiles(files).CompactTo(f, p.sfile, lvl.M, lvl.K, interrupt)
	if err != nil {
		log.Error("Cannot compact index files", zap.Error(err))
		return
	}

	// Close file.
	if err := f.Close(); err != nil {
		log.Error("Error closing index file", zap.Error(err))
		return
	}

	// Reopen as an index file.
	file := NewIndexFile(p.sfile)
	file.SetPath(path)
	if err := file.Open(); err != nil {
		log.Error("Cannot open new index file", zap.Error(err))
		return
	}

	// Obtain lock to swap in index file and write manifest.
	if err := func() error {
		p.mu.Lock()
		defer p.mu.Unlock()

		// Replace previous files with new index file.
		p.fileSet = p.fileSet.MustReplace(IndexFiles(files).Files(), file)

		// Write new manifest.
		manifestSize, err := p.Manifest().Write()
		if err != nil {
			// TODO: Close index if write fails.
			return err
		}
		p.manifestSize = manifestSize
		return nil
	}(); err != nil {
		log.Error("Cannot write manifest", zap.Error(err))
		return
	}

	elapsed := time.Since(start)
	log.Info("Full compaction complete",
		zap.String("path", path),
		logger.DurationLiteral("elapsed", elapsed),
		zap.Int64("bytes", n),
		zap.Int("kb_per_sec", int(float64(n)/elapsed.Seconds())/1024),
	)

	// Release old files.
	once.Do(func() { IndexFiles(files).Release() })

	// Close and delete all old index files.
	for _, f := range files {
		log.Info("Removing index file", zap.String("path", f.Path()))

		if err := f.Close(); err != nil {
			log.Error("Cannot close index file", zap.Error(err))
			return
		} else if err := os.Remove(f.Path()); err != nil {
			log.Error("Cannot remove index file", zap.Error(err))
			return
		}
	}
}

func (p *Partition) Rebuild() {}

func (p *Partition) CheckLogFile() error {
	// Check log file size under read lock.
	if size := func() int64 {
		p.mu.RLock()
		defer p.mu.RUnlock()
		return p.activeLogFile.Size()
	}(); size < p.MaxLogFileSize {
		return nil
	}

	// If file size exceeded then recheck under write lock and swap files.
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.checkLogFile()
}

func (p *Partition) checkLogFile() error {
	if p.activeLogFile.Size() < p.MaxLogFileSize {
		return nil
	}

	// Swap current log file.
	logFile := p.activeLogFile

	// Open new log file and insert it into the first position.
	if err := p.prependActiveLogFile(); err != nil {
		return err
	}

	// Begin compacting in a background goroutine.
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.compactLogFile(logFile)
		p.Compact() // check for new compactions
	}()

	return nil
}

// compactLogFile compacts f into a tsi file. The new file will share the
// same identifier but will have a ".tsi" extension. Once the log file is
// compacted then the manifest is updated and the log file is discarded.
func (p *Partition) compactLogFile(logFile *LogFile) {
	if p.isClosing() {
		return
	}

	p.mu.Lock()
	interrupt := p.compactionInterrupt
	p.mu.Unlock()

	start := time.Now()

	// Retrieve identifier from current path.
	id := logFile.ID()
	assert(id != 0, "cannot parse log file id: %s", logFile.Path())

	// Build a logger for this compaction.
	log, logEnd := logger.NewOperation(p.logger, "TSI log compaction", "tsi1_compact_log_file", zap.Int("tsi1_log_file_id", id))
	defer logEnd()

	// Create new index file.
	path := filepath.Join(p.path, FormatIndexFileName(id, 1))
	f, err := os.Create(path)
	if err != nil {
		log.Error("Cannot create index file", zap.Error(err))
		return
	}
	defer f.Close()

	// Compact log file to new index file.
	lvl := p.levels[1]
	n, err := logFile.CompactTo(f, lvl.M, lvl.K, interrupt)
	if err != nil {
		log.Error("Cannot compact log file", zap.Error(err), zap.String("path", logFile.Path()))
		return
	}

	// Close file.
	if err := f.Close(); err != nil {
		log.Error("Cannot close log file", zap.Error(err))
		return
	}

	// Reopen as an index file.
	file := NewIndexFile(p.sfile)
	file.SetPath(path)
	if err := file.Open(); err != nil {
		log.Error("Cannot open compacted index file", zap.Error(err), zap.String("path", file.Path()))
		return
	}

	// Obtain lock to swap in index file and write manifest.
	if err := func() error {
		p.mu.Lock()
		defer p.mu.Unlock()

		// Replace previous log file with index file.
		p.fileSet = p.fileSet.MustReplace([]File{logFile}, file)

		// Write new manifest.
		manifestSize, err := p.Manifest().Write()
		if err != nil {
			// TODO: Close index if write fails.
			return err
		}

		p.manifestSize = manifestSize
		return nil
	}(); err != nil {
		log.Error("Cannot update manifest", zap.Error(err))
		return
	}

	elapsed := time.Since(start)
	log.Info("Log file compacted",
		logger.DurationLiteral("elapsed", elapsed),
		zap.Int64("bytes", n),
		zap.Int("kb_per_sec", int(float64(n)/elapsed.Seconds())/1024),
	)

	// Closing the log file will automatically wait until the ref count is zero.
	if err := logFile.Close(); err != nil {
		log.Error("Cannot close log file", zap.Error(err))
		return
	} else if err := os.Remove(logFile.Path()); err != nil {
		log.Error("Cannot remove log file", zap.Error(err))
		return
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

	path string // location on disk of the manifest.
}

// NewManifest returns a new instance of Manifest with default compaction levels.
func NewManifest(path string) *Manifest {
	m := &Manifest{
		Levels:  make([]CompactionLevel, len(DefaultCompactionLevels)),
		Version: Version,
		path:    path,
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

// Write writes the manifest file to the provided path, returning the number of
// bytes written and an error, if any.
func (m *Manifest) Write() (int64, error) {
	buf, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return 0, err
	}
	buf = append(buf, '\n')

	if err := ioutil.WriteFile(m.path, buf, 0666); err != nil {
		return 0, err
	}
	return int64(len(buf)), nil
}

// ReadManifestFile reads a manifest from a file path and returns the Manifest,
// the size of the manifest on disk, and any error if appropriate.
func ReadManifestFile(path string) (*Manifest, int64, error) {
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}

	// Decode manifest.
	var m Manifest
	if err := json.Unmarshal(buf, &m); err != nil {
		return nil, 0, err
	}

	// Set the path of the manifest.
	m.path = path
	return &m, int64(len(buf)), nil
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

// IsPartitionDir returns true if directory contains a MANIFEST file.
func IsPartitionDir(path string) (bool, error) {
	if _, err := os.Stat(filepath.Join(path, ManifestFileName)); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}
