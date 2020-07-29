package tsi1

import (
	"context"
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

	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/pkg/bytesutil"
	"github.com/influxdata/influxdb/v2/pkg/fs"
	"github.com/influxdata/influxdb/v2/pkg/lifecycle"
	"github.com/influxdata/influxdb/v2/pkg/mincore"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/seriesfile"
	"github.com/influxdata/influxql"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// Version is the current version of the TSI index.
const Version = 1

// File extensions.
const (
	LogFileExt   = ".tsl"
	IndexFileExt = ".tsi"

	CompactingExt = ".compacting"
)

const (
	// ManifestFileName is the name of the index manifest file.
	ManifestFileName = "MANIFEST"
)

// Partition represents a collection of layered index files and WAL.
type Partition struct {
	// The rule to ensure no deadlocks, no resource leaks, and no use after close
	// is that if the partition launches a goroutine, it must acquire a reference
	// to itself first and releases it only after it has done all of its use of mu.
	mu    sync.RWMutex
	resmu sync.Mutex // protects res Open and Close
	res   lifecycle.Resource

	sfile    *seriesfile.SeriesFile // series lookup file
	sfileref *lifecycle.Reference   // reference to series lookup file

	activeLogFile *LogFile // current log file
	fileSet       *FileSet // current file set
	seq           int      // file id sequence

	// Running statistics
	tracker *partitionTracker

	// Fast series lookup of series IDs in the series file that have been present
	// in this partition. This set tracks both insertions and deletions of a series.
	seriesIDSet *tsdb.SeriesIDSet

	// Stats caching
	StatsTTL      time.Duration
	statsCache    MeasurementCardinalityStats
	lastStatsTime time.Time

	// Compaction management
	levels              []CompactionLevel // compaction levels
	levelCompacting     []bool            // level compaction status
	compactionsDisabled int               // counter of disables
	currentCompactionN  int               // counter of in-progress compactions

	// Directory of the Partition's index files.
	path string
	id   string // id portion of path.

	// Log file compaction thresholds.
	MaxLogFileSize int64
	nosync         bool // when true, flushing and syncing of LogFile will be disabled.
	logbufferSize  int  // the LogFile's buffer is set to this value.

	pageFaultLimiter *rate.Limiter

	logger *zap.Logger

	// Current size of MANIFEST. Used to determine partition size.
	manifestSize int64

	// Index's version.
	version int
}

// NewPartition returns a new instance of Partition.
func NewPartition(sfile *seriesfile.SeriesFile, path string) *Partition {
	partition := &Partition{
		path:        path,
		sfile:       sfile,
		seriesIDSet: tsdb.NewSeriesIDSet(),

		MaxLogFileSize: DefaultMaxIndexLogFileSize,

		logger:  zap.NewNop(),
		version: Version,
	}

	defaultLabels := prometheus.Labels{"index_partition": ""}
	partition.tracker = newPartitionTracker(newPartitionMetrics(nil), defaultLabels)
	return partition
}

// bytes estimates the memory footprint of this Partition, in bytes.
func (p *Partition) bytes() int {
	var b int
	b += int(unsafe.Sizeof(p.mu))
	b += int(unsafe.Sizeof(p.resmu))
	b += int(unsafe.Sizeof(p.res))
	// Do not count SeriesFile contents because it belongs to the code that constructed this Partition.
	b += int(unsafe.Sizeof(p.sfile))
	b += int(unsafe.Sizeof(p.sfileref))
	b += int(unsafe.Sizeof(p.activeLogFile)) + p.activeLogFile.bytes()
	b += int(unsafe.Sizeof(p.fileSet)) + p.fileSet.bytes()
	b += int(unsafe.Sizeof(p.seq))
	b += int(unsafe.Sizeof(p.tracker))
	b += int(unsafe.Sizeof(p.seriesIDSet)) + p.seriesIDSet.Bytes()
	b += int(unsafe.Sizeof(p.levels))
	for _, level := range p.levels {
		b += int(unsafe.Sizeof(level))
	}
	b += int(unsafe.Sizeof(p.levelCompacting))
	for _, levelCompacting := range p.levelCompacting {
		b += int(unsafe.Sizeof(levelCompacting))
	}
	b += int(unsafe.Sizeof(p.compactionsDisabled))
	b += int(unsafe.Sizeof(p.path)) + len(p.path)
	b += int(unsafe.Sizeof(p.id)) + len(p.id)
	b += int(unsafe.Sizeof(p.MaxLogFileSize))
	b += int(unsafe.Sizeof(p.nosync))
	b += int(unsafe.Sizeof(p.logbufferSize))
	b += int(unsafe.Sizeof(p.logger))
	b += int(unsafe.Sizeof(p.manifestSize))
	b += int(unsafe.Sizeof(p.version))
	return b
}

// ErrIncompatibleVersion is returned when attempting to read from an
// incompatible tsi1 manifest file.
var ErrIncompatibleVersion = errors.New("incompatible tsi1 index MANIFEST")

// Open opens the partition.
func (p *Partition) Open() (err error) {
	p.resmu.Lock()
	defer p.resmu.Unlock()

	if p.res.Opened() {
		return errors.New("index partition already open")
	}

	// Try to acquire a reference to the series file
	p.sfileref, err = p.sfile.Acquire()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			p.close()
		}
	}()

	// Validate path is correct.
	p.id = filepath.Base(p.path)
	if _, err := strconv.Atoi(p.id); err != nil {
		return err
	}

	// Create directory if it doesn't exist.
	if err := os.MkdirAll(p.path, 0777); err != nil {
		return err
	}

	// Read manifest file.
	m, manifestSize, err := ReadManifestFile(p.manifestPath())
	if os.IsNotExist(err) {
		m = NewManifest(p.manifestPath())
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
	files, err := func() (files []File, err error) {
		// Ensure any opened files are closed in the case of an error.
		defer func() {
			if err != nil {
				for _, file := range files {
					file.Close()
				}
			}
		}()

		// Open all of the files in the manifest.
		for _, filename := range m.Files {
			switch filepath.Ext(filename) {
			case LogFileExt:
				f, err := p.openLogFile(filepath.Join(p.path, filename))
				if err != nil {
					return nil, err
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
					return nil, err
				}
				files = append(files, f)
			}
		}

		return files, nil
	}()
	if err != nil {
		return err
	}

	// Place the files in a file set.
	p.fileSet, err = NewFileSet(p.sfile, files)
	if err != nil {
		for _, file := range files {
			file.Close()
		}
		return err
	}

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

	// Build series existence set.
	if err := p.buildSeriesSet(); err != nil {
		return err
	}
	p.tracker.SetSeries(p.seriesIDSet.Cardinality())
	p.tracker.SetFiles(uint64(len(p.fileSet.IndexFiles())), "index")
	p.tracker.SetFiles(uint64(len(p.fileSet.LogFiles())), "log")
	p.tracker.SetDiskSize(uint64(p.fileSet.Size()))

	// Mark opened.
	p.res.Open()

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
	f.pageFaultLimiter = mincore.NewLimiter(p.pageFaultLimiter, f.data)
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
	p.seriesIDSet = tsdb.NewSeriesIDSet()

	// Read series sets from files in reverse.
	for i := len(p.fileSet.files) - 1; i >= 0; i-- {
		f := p.fileSet.files[i]

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

// Close closes the partition.
func (p *Partition) Close() error {
	p.resmu.Lock()
	defer p.resmu.Unlock()

	// Close the resource.
	p.res.Close()
	p.Wait()

	// There are now no internal outstanding callers holding a reference
	// so we can acquire this mutex to protect against external callers.
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.close()
}

// close does the work of closing and cleaning up the partition after it
// has acquired locks and ensured no one is using it.
func (p *Partition) close() error {
	// Release series file.
	if p.sfileref != nil {
		p.sfileref.Release()
		p.sfileref = nil
	}

	// Release the file set and close all of the files.
	var err error
	if p.fileSet != nil {
		p.fileSet.Release()
		for _, file := range p.fileSet.files {
			if e := file.Close(); e != nil && err == nil {
				err = e
			}
		}
		p.fileSet = nil
	}

	return err
}

// Path returns the path to the partition.
func (p *Partition) Path() string { return p.path }

// SeriesFile returns the attached series file.
func (p *Partition) SeriesFile() *seriesfile.SeriesFile { return p.sfile }

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

// manifestPath returns the path to the index's manifest file.
func (p *Partition) manifestPath() string {
	return filepath.Join(p.path, ManifestFileName)
}

// Manifest returns a Manifest for the partition given a file set.
func (p *Partition) Manifest(fs *FileSet) *Manifest {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.manifest(fs)
}

// manifest returns a Manifest for the partition given a file set. It
// requires that at least a read lock is held.
func (p *Partition) manifest(fs *FileSet) *Manifest {
	m := &Manifest{
		Levels:  p.levels,
		Files:   make([]string, len(fs.files)),
		Version: p.version,
		path:    p.manifestPath(),
	}

	for j, f := range fs.files {
		m.Files[j] = filepath.Base(f.Path())
	}

	return m
}

// WithLogger sets the logger for the index.
func (p *Partition) WithLogger(logger *zap.Logger) {
	p.logger = logger.With(zap.String("index", "tsi"))
}

// FileSet returns a copy of the current file set. You must call Release on it when
// you are finished.
func (p *Partition) FileSet() (*FileSet, error) {
	p.mu.RLock()
	fs, err := p.fileSet.Duplicate()
	p.mu.RUnlock()
	return fs, err
}

// replaceFileSet is a helper to replace the file set of the partition. It releases
// the resources on the old file set before replacing it with the new one.
func (p *Partition) replaceFileSet(fs *FileSet) {
	p.fileSet.Release()
	p.fileSet = fs
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

	// Prepend and generate new fileset.
	fileSet, err := p.fileSet.PrependLogFile(f)
	if err != nil {
		f.Close()
		return err
	}

	// Write new manifest.
	manifestSize, err := p.manifest(fileSet).Write()
	if err != nil {
		// TODO: Close index if write fails.
		fileSet.Release()
		f.Close()
		return err
	}

	// Now that we can no longer error, update the partition state.
	p.activeLogFile = f
	p.replaceFileSet(fileSet)
	p.manifestSize = manifestSize

	// Set the file metrics again.
	p.tracker.SetFiles(uint64(len(p.fileSet.IndexFiles())), "index")
	p.tracker.SetFiles(uint64(len(p.fileSet.LogFiles())), "log")
	p.tracker.SetDiskSize(uint64(p.fileSet.Size()))
	return nil
}

// ForEachMeasurementName iterates over all measurement names in the index.
func (p *Partition) ForEachMeasurementName(fn func(name []byte) error) error {
	fs, err := p.FileSet()
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
	fs, err := p.FileSet()
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
	fs, err := p.FileSet()
	if err != nil {
		return nil, err
	}
	return newFileSetMeasurementIterator(fs,
		NewTSDBMeasurementIteratorAdapter(fs.MeasurementIterator())), nil
}

// MeasurementExists returns true if a measurement exists.
func (p *Partition) MeasurementExists(name []byte) (bool, error) {
	fs, err := p.FileSet()
	if err != nil {
		return false, err
	}
	defer fs.Release()

	m := fs.Measurement(name)
	return m != nil && !m.Deleted(), nil
}

func (p *Partition) MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error) {
	fs, err := p.FileSet()
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
	fs, err := p.FileSet()
	if err != nil {
		return nil, err
	}
	return newFileSetSeriesIDIterator(fs, fs.MeasurementSeriesIDIterator(name)), nil
}

// DropMeasurement deletes a measurement from the index. DropMeasurement does
// not remove any series from the index directly.
func (p *Partition) DropMeasurement(name []byte) error {
	fs, err := p.FileSet()
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
					return p.activeLogFile.DeleteTagKeyNoSync(name, k.Key())
				}(); err != nil {
					return err
				}
			}

			// Delete each value in key.
			if vitr := k.TagValueIterator(nil); vitr != nil {
				for v := vitr.Next(); v != nil; v = vitr.Next() {
					if !v.Deleted() {
						if err := func() error {
							p.mu.RLock()
							defer p.mu.RUnlock()
							return p.activeLogFile.DeleteTagValueNoSync(name, k.Key(), v.Value())
						}(); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	// Delete all series.
	// TODO(edd): it's not clear to me why we have to delete all series IDs from
	// the index when we could just mark the measurement as deleted.
	if itr := fs.MeasurementSeriesIDIterator(name); itr != nil {
		defer itr.Close()

		// 1024 is assuming that typically a bucket (measurement) will have at least
		// 1024 series in it.
		all := make([]tsdb.SeriesID, 0, 1024)
		for {
			elem, err := itr.Next()
			if err != nil {
				return err
			} else if elem.SeriesID.IsZero() {
				break
			}
			all = append(all, elem.SeriesID)

			// Update series set.
			p.seriesIDSet.Remove(elem.SeriesID)
		}

		if err := p.activeLogFile.DeleteSeriesIDList(all); err != nil {
			return err
		}

		p.tracker.AddSeriesDropped(uint64(len(all)))
		p.tracker.SubSeries(uint64(len(all)))

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

	// Ensure log is flushed & synced.
	if err := func() error {
		p.mu.RLock()
		defer p.mu.RUnlock()
		return p.activeLogFile.FlushAndSync()
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
func (p *Partition) createSeriesListIfNotExists(collection *tsdb.SeriesCollection) ([]tsdb.SeriesID, error) {
	// Is there anything to do? The partition may have been sent an empty batch.
	if collection.Length() == 0 {
		return nil, nil
	} else if len(collection.Names) != len(collection.Tags) {
		return nil, fmt.Errorf("uneven batch, partition %s sent %d names and %d tags", p.id, len(collection.Names), len(collection.Tags))
	}

	// Ensure fileset cannot change during insert.
	now := time.Now()
	p.mu.RLock()

	// Try to acquire a resource on the active log file
	res, err := p.activeLogFile.Acquire()
	if err != nil {
		p.mu.RUnlock()
		return nil, err
	}

	// Insert series into log file.
	ids, err := p.activeLogFile.AddSeriesList(p.seriesIDSet, collection)

	// Release our resources.
	res.Release()
	p.mu.RUnlock()

	// Check the error from insert.
	if err != nil {
		return nil, err
	}

	if err := p.CheckLogFile(); err != nil {
		return nil, err
	}

	// NOTE(edd): if this becomes expensive then we can move the count into the
	// log file.
	var totalNew uint64
	for _, id := range ids {
		if !id.IsZero() {
			totalNew++
		}
	}
	if totalNew > 0 {
		p.tracker.AddSeriesCreated(totalNew, time.Since(now))
		p.tracker.AddSeries(totalNew)
		p.mu.RLock()
		p.tracker.SetDiskSize(uint64(p.fileSet.Size()))
		p.mu.RUnlock()
	}
	return ids, nil
}

// DropSeries removes the provided set of series id from the index.
func (p *Partition) DropSeries(ids []tsdb.SeriesID) error {
	// Count total affected series.
	var n uint64
	for _, id := range ids {
		if p.seriesIDSet.Contains(id) {
			n++
		}
	}

	// Delete series from index.
	if err := p.activeLogFile.DeleteSeriesIDs(ids); err != nil {
		return err
	}

	// Update series set.
	for _, id := range ids {
		p.seriesIDSet.Remove(id)
	}
	p.tracker.AddSeriesDropped(n)
	p.tracker.SubSeries(n)

	// Swap log file, if necessary.
	return p.CheckLogFile()
}

// HasTagKey returns true if tag key exists.
func (p *Partition) HasTagKey(name, key []byte) (bool, error) {
	fs, err := p.FileSet()
	if err != nil {
		return false, err
	}
	defer fs.Release()

	return fs.HasTagKey(name, key), nil
}

// HasTagValue returns true if tag value exists.
func (p *Partition) HasTagValue(name, key, value []byte) (bool, error) {
	fs, err := p.FileSet()
	if err != nil {
		return false, err
	}
	defer fs.Release()

	return fs.HasTagValue(name, key, value), nil
}

// TagKeyIterator returns an iterator for all keys across a single measurement.
func (p *Partition) TagKeyIterator(name []byte) (tsdb.TagKeyIterator, error) {
	fs, err := p.FileSet()
	if err != nil {
		return nil, err
	}
	return newFileSetTagKeyIterator(fs,
		NewTSDBTagKeyIteratorAdapter(fs.TagKeyIterator(name))), nil
}

// TagValueIterator returns an iterator for all values across a single key.
func (p *Partition) TagValueIterator(name, key []byte) (tsdb.TagValueIterator, error) {
	fs, err := p.FileSet()
	if err != nil {
		return nil, err
	}
	return newFileSetTagValueIterator(fs,
		NewTSDBTagValueIteratorAdapter(fs.TagValueIterator(name, key))), nil
}

// TagKeySeriesIDIterator returns a series iterator for all values across a single key.
func (p *Partition) TagKeySeriesIDIterator(name, key []byte) (tsdb.SeriesIDIterator, error) {
	fs, err := p.FileSet()
	if err != nil {
		return nil, err
	}

	itr, err := fs.TagKeySeriesIDIterator(name, key)
	if err != nil {
		fs.Release()
		return nil, err
	}
	return newFileSetSeriesIDIterator(fs, itr), nil
}

// TagValueSeriesIDIterator returns a series iterator for a single key value.
func (p *Partition) TagValueSeriesIDIterator(name, key, value []byte) (tsdb.SeriesIDIterator, error) {
	fs, err := p.FileSet()
	if err != nil {
		return nil, err
	}
	itr, err := fs.TagValueSeriesIDIterator(name, key, value)
	if err != nil {
		fs.Release()
		return nil, err
	}
	return newFileSetSeriesIDIterator(fs, itr), nil
}

// MeasurementTagKeysByExpr extracts the tag keys wanted by the expression.
func (p *Partition) MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) {
	fs, err := p.FileSet()
	if err != nil {
		return nil, err
	}
	defer fs.Release()

	return fs.MeasurementTagKeysByExpr(name, expr)
}

// ForEachMeasurementTagKey iterates over all tag keys in a measurement.
func (p *Partition) ForEachMeasurementTagKey(name []byte, fn func(key []byte) error) error {
	fs, err := p.FileSet()
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

// DisableCompactions stops any compactions from starting until a call to EnableCompactions.
func (p *Partition) DisableCompactions() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.compactionsDisabled++
}

// EnableCompactions allows compactions to proceed again after a call to DisableCompactions.
func (p *Partition) EnableCompactions() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.compactionsDisabled--
}

// CurrentCompactionN returns the number of compactions currently running.
func (p *Partition) CurrentCompactionN() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.currentCompactionN
}

// Wait will block until all compactions are finished.
// Must only be called while they are disabled.
func (p *Partition) Wait() {
	if p.CurrentCompactionN() == 0 { // Is it possible to immediately return?
		return
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		if p.CurrentCompactionN() == 0 {
			return
		}
	}
}

// compact compacts continguous groups of files that are not currently compacting.
func (p *Partition) compact() {
	if p.compactionsDisabled > 0 {
		p.logger.Error("Cannot start a compaction while disabled")
		return
	}

	fs, err := p.fileSet.Duplicate()
	if err != nil {
		p.logger.Error("Attempt to compact while partition is closing", zap.Error(err))
		return
	}
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

		// We intend to do a compaction. Acquire a resource to do so.
		ref, err := p.res.Acquire()
		if err != nil {
			p.logger.Error("Attempt to compact while partition is closing", zap.Error(err))
			return
		}

		// Acquire references to the files to keep them alive through compaction.
		frefs, err := IndexFiles(files).Acquire()
		if err != nil {
			p.logger.Error("Attempt to compact a file that is closed", zap.Error(err))
			continue
		}

		// Mark the level as compacting.
		p.levelCompacting[level] = true

		// Start compacting in a separate goroutine.
		p.currentCompactionN++
		go func(level int) {
			// Compact to a new level.
			p.compactToLevel(files, frefs, level+1, ref.Closing())

			// Ensure references are released.
			frefs.Release()
			ref.Release()

			// Ensure compaction lock for the level is released.
			p.mu.Lock()
			p.levelCompacting[level] = false
			p.currentCompactionN--
			p.mu.Unlock()

			// Check for new compactions
			p.Compact()
		}(level)
	}
}

// compactToLevel compacts a set of files into a new file. Replaces old files with
// compacted file on successful completion. This runs in a separate goroutine.
func (p *Partition) compactToLevel(files []*IndexFile, frefs lifecycle.References,
	level int, interrupt <-chan struct{}) {

	assert(len(files) >= 2, "at least two index files are required for compaction")
	assert(level > 0, "cannot compact level zero")

	var err error
	var start time.Time

	p.tracker.IncActiveCompaction(level)
	// Set the relevant metrics at the end of any compaction.
	defer func() {
		p.mu.RLock()
		defer p.mu.RUnlock()
		p.tracker.SetFiles(uint64(len(p.fileSet.IndexFiles())), "index")
		p.tracker.SetFiles(uint64(len(p.fileSet.LogFiles())), "log")
		p.tracker.SetDiskSize(uint64(p.fileSet.Size()))
		p.tracker.DecActiveCompaction(level)

		success := err == nil
		p.tracker.CompactionAttempted(level, success, time.Since(start))
	}()

	span, ctx := tracing.StartSpanFromContext(context.Background())
	defer span.Finish()

	// Build a logger for this compaction.
	log, logEnd := logger.NewOperation(ctx, p.logger, "TSI level compaction", "tsi1_compact_to_level", zap.Int("tsi1_level", level))
	defer logEnd()

	// Check for cancellation.
	select {
	case <-interrupt:
		log.Error("Cannot begin compaction", zap.Error(ErrCompactionInterrupted))
		return
	default:
	}

	// Track time to compact.
	start = time.Now()

	// Create new index file.
	path := filepath.Join(p.path, FormatIndexFileName(p.NextSequence(), level))
	var f *os.File
	if f, err = fs.CreateFile(path); err != nil {
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
	var n int64
	if n, err = IndexFiles(files).CompactTo(f, p.sfile, lvl.M, lvl.K, interrupt); err != nil {
		log.Error("Cannot compact index files", zap.Error(err))
		return
	}

	// Close file.
	if err = f.Close(); err != nil {
		log.Error("Error closing index file", zap.Error(err))
		return
	}

	// Reopen as an index file.
	file := NewIndexFile(p.sfile)
	file.SetPath(path)
	if err = file.Open(); err != nil {
		log.Error("Cannot open new index file", zap.Error(err))
		return
	}
	file.pageFaultLimiter = mincore.NewLimiter(p.pageFaultLimiter, file.data)

	// Obtain lock to swap in index file and write manifest.
	if err = func() error {
		p.mu.Lock()
		defer p.mu.Unlock()

		// Replace previous files with new index file.
		fileSet, err := p.fileSet.MustReplace(IndexFiles(files).Files(), file)
		if err != nil {
			return err
		}

		// Write new manifest.
		manifestSize, err := p.manifest(fileSet).Write()
		if err != nil {
			// TODO: Close index if write fails.
			fileSet.Release()
			return err
		}

		// Now that we can no longer error, update the local state.
		p.replaceFileSet(fileSet)
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
	frefs.Release()

	// Close and delete all old index files.
	for _, f := range files {
		log.Info("Removing index file", zap.String("path", f.Path()))

		if err = f.Close(); err != nil {
			log.Error("Cannot close index file", zap.Error(err))
			return
		} else if err = os.Remove(f.Path()); err != nil {
			log.Error("Cannot remove index file", zap.Error(err))
			return
		}
	}
}

func (p *Partition) CheckLogFile() error {
	// Check log file size under read lock.
	p.mu.RLock()
	size := p.activeLogFile.Size()
	p.mu.RUnlock()

	if size < p.MaxLogFileSize {
		return nil
	}

	// If file size exceeded then recheck under write lock and swap files.
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.checkLogFile()
}

func (p *Partition) checkLogFile() error {
	if p.compactionsDisabled > 0 {
		return nil
	}

	// Acquire a reference to hold the partition open.
	ref, err := p.res.Acquire()
	if err != nil {
		return err
	}

	if p.activeLogFile.Size() < p.MaxLogFileSize {
		ref.Release()
		return nil
	}

	span, ctx := tracing.StartSpanFromContext(context.Background())
	defer span.Finish()

	// Swap current log file.
	logFile := p.activeLogFile

	// Open new log file and insert it into the first position.
	if err := p.prependActiveLogFile(); err != nil {
		ref.Release()
		return err
	}

	// Begin compacting in a background goroutine.
	p.currentCompactionN++
	go func() {
		p.compactLogFile(ctx, logFile, ref.Closing())
		ref.Release() // release our reference

		p.mu.Lock()
		p.currentCompactionN-- // compaction is now complete
		p.mu.Unlock()

		p.Compact() // check for new compactions
	}()

	return nil
}

// compactLogFile compacts f into a tsi file. The new file will share the
// same identifier but will have a ".tsi" extension. Once the log file is
// compacted then the manifest is updated and the log file is discarded.
func (p *Partition) compactLogFile(ctx context.Context, logFile *LogFile, interrupt <-chan struct{}) {
	defer func() {
		p.mu.RLock()
		defer p.mu.RUnlock()
		p.tracker.SetFiles(uint64(len(p.fileSet.IndexFiles())), "index")
		p.tracker.SetFiles(uint64(len(p.fileSet.LogFiles())), "log")
		p.tracker.SetDiskSize(uint64(p.fileSet.Size()))
	}()

	start := time.Now()

	// Retrieve identifier from current path.
	id := logFile.ID()
	assert(id != 0, "cannot parse log file id: %s", logFile.Path())

	// Build a logger for this compaction.
	log, logEnd := logger.NewOperation(ctx, p.logger, "TSI log compaction", "tsi1_compact_log_file", zap.Int("tsi1_log_file_id", id))
	defer logEnd()

	// Create new index file.
	path := filepath.Join(p.path, FormatIndexFileName(id, 1))
	f, err := fs.CreateFile(path)
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
	file.pageFaultLimiter = mincore.NewLimiter(p.pageFaultLimiter, file.data)

	// Obtain lock to swap in index file and write manifest.
	if err := func() error {
		p.mu.Lock()
		defer p.mu.Unlock()

		// Replace previous log file with index file.
		fileSet, err := p.fileSet.MustReplace([]File{logFile}, file)
		if err != nil {
			return err
		}

		// Write new manifest.
		manifestSize, err := p.manifest(fileSet).Write()
		if err != nil {
			// TODO: Close index if write fails.
			fileSet.Release()
			return err
		}

		// Now that we can no longer error, update the local state.
		p.replaceFileSet(fileSet)
		p.manifestSize = manifestSize

		return nil
	}(); err != nil {
		log.Error("Cannot update manifest or stats", zap.Error(err))
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

// MeasurementCardinalityStats returns cardinality stats for all measurements.
func (p *Partition) MeasurementCardinalityStats() (MeasurementCardinalityStats, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Return cached version, if enabled and the TTL is less than the last cache time.
	if p.StatsTTL > 0 && !p.lastStatsTime.IsZero() && time.Since(p.lastStatsTime) < p.StatsTTL {
		return p.statsCache.Clone(), nil
	}

	// If cache is unavailable then generate fresh stats.
	stats, err := p.measurementCardinalityStats()
	if err != nil {
		return nil, err
	}

	// Cache the stats if enabled.
	if p.StatsTTL > 0 {
		p.statsCache = stats
		p.lastStatsTime = time.Now()
	}

	return stats, nil
}

func (p *Partition) measurementCardinalityStats() (MeasurementCardinalityStats, error) {
	fs, err := p.fileSet.Duplicate()
	if err != nil {
		return nil, err
	}
	defer fs.Release()

	stats := make(MeasurementCardinalityStats)
	mitr := fs.MeasurementIterator()
	if mitr == nil {
		return stats, nil
	}

	for {
		// Iterate over each measurement and set cardinality.
		mm := mitr.Next()
		if mm == nil {
			return stats, nil
		}

		// Obtain all series for measurement.
		sitr := fs.MeasurementSeriesIDIterator(mm.Name())
		if sitr == nil {
			continue
		}

		// All iterators should be series id set iterators except legacy 1.x data.
		// Skip if it does not conform as aggregation would be too slow.
		ssitr, ok := sitr.(tsdb.SeriesIDSetIterator)
		if !ok {
			continue
		}

		// Intersect with partition set to ensure deleted series are removed.
		set := p.seriesIDSet.And(ssitr.SeriesIDSet())
		cardinality := int(set.Cardinality())
		if cardinality == 0 {
			continue
		}

		// Set cardinality for the given measurement.
		stats[string(mm.Name())] = cardinality
	}
}

type partitionTracker struct {
	metrics *partitionMetrics
	labels  prometheus.Labels
	enabled bool // Allows tracker to be disabled.
}

func newPartitionTracker(metrics *partitionMetrics, defaultLabels prometheus.Labels) *partitionTracker {
	return &partitionTracker{
		metrics: metrics,
		labels:  defaultLabels,
		enabled: true,
	}
}

// Labels returns a copy of labels for use with index partition metrics.
func (t *partitionTracker) Labels() prometheus.Labels {
	l := make(map[string]string, len(t.labels))
	for k, v := range t.labels {
		l[k] = v
	}
	return l
}

// AddSeriesCreated increases the number of series created in the partition by n
// and sets a sample of the time taken to create a series.
func (t *partitionTracker) AddSeriesCreated(n uint64, d time.Duration) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.SeriesCreated.With(labels).Add(float64(n))

	if n == 0 {
		return // Nothing to record
	}

	perseries := d.Seconds() / float64(n)
	t.metrics.SeriesCreatedDuration.With(labels).Observe(perseries)
}

// AddSeriesDropped increases the number of series dropped in the partition by n.
func (t *partitionTracker) AddSeriesDropped(n uint64) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.SeriesDropped.With(labels).Add(float64(n))
}

// SetSeries sets the number of series in the partition.
func (t *partitionTracker) SetSeries(n uint64) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.Series.With(labels).Set(float64(n))
}

// AddSeries increases the number of series in the partition by n.
func (t *partitionTracker) AddSeries(n uint64) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.Series.With(labels).Add(float64(n))
}

// SubSeries decreases the number of series in the partition by n.
func (t *partitionTracker) SubSeries(n uint64) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.Series.With(labels).Sub(float64(n))
}

// SetMeasurements sets the number of measurements in the partition.
func (t *partitionTracker) SetMeasurements(n uint64) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.Measurements.With(labels).Set(float64(n))
}

// AddMeasurements increases the number of measurements in the partition by n.
func (t *partitionTracker) AddMeasurements(n uint64) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.Measurements.With(labels).Add(float64(n))
}

// SubMeasurements decreases the number of measurements in the partition by n.
func (t *partitionTracker) SubMeasurements(n uint64) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.Measurements.With(labels).Sub(float64(n))
}

// SetFiles sets the number of files in the partition.
func (t *partitionTracker) SetFiles(n uint64, typ string) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	labels["type"] = typ
	t.metrics.FilesTotal.With(labels).Set(float64(n))
}

// SetDiskSize sets the size of files in the partition.
func (t *partitionTracker) SetDiskSize(n uint64) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.DiskSize.With(labels).Set(float64(n))
}

// IncActiveCompaction increments the number of active compactions for the provided level.
func (t *partitionTracker) IncActiveCompaction(level int) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	labels["level"] = fmt.Sprint(level)

	t.metrics.CompactionsActive.With(labels).Inc()
}

// DecActiveCompaction decrements the number of active compactions for the provided level.
func (t *partitionTracker) DecActiveCompaction(level int) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	labels["level"] = fmt.Sprint(level)

	t.metrics.CompactionsActive.With(labels).Dec()
}

// CompactionAttempted updates the number of compactions attempted for the provided level.
func (t *partitionTracker) CompactionAttempted(level int, success bool, d time.Duration) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	labels["level"] = fmt.Sprint(level)
	if success {
		t.metrics.CompactionDuration.With(labels).Observe(d.Seconds())

		labels["status"] = "ok"
		t.metrics.Compactions.With(labels).Inc()
		return
	}

	labels["status"] = "error"
	t.metrics.Compactions.With(labels).Inc()
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
