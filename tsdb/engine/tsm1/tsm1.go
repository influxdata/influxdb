package tsm1

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/golang/snappy"
	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/tsdb"
)

const (
	// Format is the file format name of this engine.
	Format = "tsm1"

	//IDsFileExtension is the extension for the file that keeps the compressed map
	// of keys to uint64 IDs.
	IDsFileExtension = "ids"

	// FieldsFileExtension is the extension for the file that stores compressed field
	// encoding data for this db
	FieldsFileExtension = "fields"

	// SeriesFileExtension is the extension for the file that stores the compressed
	// series metadata for series in this db
	SeriesFileExtension = "series"

	// CollisionsFileExtension is the extension for the file that keeps a map of which
	// keys have hash collisions and what their actual IDs are
	CollisionsFileExtension = "collisions"

	// CheckpointExtension is the extension given to files that checkpoint a rewrite or compaction.
	// The checkpoint files are created when a new file is first created. They
	// are removed after the file has been synced and is safe for use. If a file
	// has an associated checkpoint file, it wasn't safely written and both should be removed
	CheckpointExtension = "check"

	// CompactionExtension is the extension given to the file that marks when a compaction has been
	// fully written, but the compacted files have not yet been deleted. It is used for cleanup
	// if the server was not cleanly shutdown before the compacted files could be deleted.
	CompactionExtension = "compact"

	// keyFieldSeparator separates the series key from the field name in the composite key
	// that identifies a specific field in series
	keyFieldSeparator = "#!~#"

	blockBufferSize = 1024 * 1024
)

type TimePrecision uint8

const (
	Seconds TimePrecision = iota
	Milliseconds
	Microseconds
	Nanoseconds
)

func init() {
	tsdb.RegisterEngine(Format, NewEngine)
}

const (
	MaxDataFileSize = 1024 * 1024 * 1024 * 2 // 2GB

	DefaultRotateFileSize = 5 * 1024 * 1024 // 5MB

	DefaultMaxPointsPerBlock = 1000

	// MAP_POPULATE is for the mmap syscall. For some reason this isn't defined in golang's syscall
	MAP_POPULATE = 0x8000
)

// Ensure Engine implements the interface.
var _ tsdb.Engine = &Engine{}

// Engine represents a storage engine with compressed blocks.
type Engine struct {
	writeLock *WriteLock
	metaLock  sync.Mutex
	path      string
	logger    *log.Logger

	// deletesPending mark how many old data files are waiting to be deleted. This will
	// keep a close from returning until all deletes finish
	deletesPending sync.WaitGroup

	// HashSeriesField is a function that takes a series key and a field name
	// and returns a hash identifier. It's not guaranteed to be unique.
	HashSeriesField func(key string) uint64

	WAL *Log

	RotateFileSize             uint32
	MaxFileSize                uint32
	SkipCompaction             bool
	CompactionAge              time.Duration
	MinCompactionFileCount     int
	IndexCompactionFullAge     time.Duration
	IndexMinCompactionInterval time.Duration
	MaxPointsPerBlock          int

	// filesLock is only for modifying and accessing the files slice
	filesLock          sync.RWMutex
	files              dataFiles
	currentFileID      int
	compactionRunning  bool
	lastCompactionTime time.Time

	// deletes is a map of keys that are deleted, but haven't yet been
	// compacted and flushed. They map the ID to the corresponding key
	deletes map[uint64]string

	// deleteMeasurements is a map of the measurements that are deleted
	// but haven't yet been compacted and flushed
	deleteMeasurements map[string]bool

	collisionsLock sync.RWMutex
	collisions     map[string]uint64

	// queryLock keeps data files from being deleted or the store from
	// being closed while queries are running
	queryLock sync.RWMutex
}

// NewEngine returns a new instance of Engine.
func NewEngine(path string, walPath string, opt tsdb.EngineOptions) tsdb.Engine {
	w := NewLog(path)
	w.FlushColdInterval = time.Duration(opt.Config.WALFlushColdInterval)
	w.FlushMemorySizeThreshold = opt.Config.WALFlushMemorySizeThreshold
	w.MaxMemorySizeThreshold = opt.Config.WALMaxMemorySizeThreshold
	w.LoggingEnabled = opt.Config.WALLoggingEnabled

	e := &Engine{
		path:      path,
		writeLock: &WriteLock{},
		logger:    log.New(os.Stderr, "[tsm1] ", log.LstdFlags),

		// TODO: this is the function where we can inject a check against the in memory collisions
		HashSeriesField:            hashSeriesField,
		WAL:                        w,
		RotateFileSize:             DefaultRotateFileSize,
		MaxFileSize:                MaxDataFileSize,
		CompactionAge:              opt.Config.IndexCompactionAge,
		MinCompactionFileCount:     opt.Config.IndexMinCompactionFileCount,
		IndexCompactionFullAge:     opt.Config.IndexCompactionFullAge,
		IndexMinCompactionInterval: opt.Config.IndexMinCompactionInterval,
		MaxPointsPerBlock:          DefaultMaxPointsPerBlock,
	}
	e.WAL.IndexWriter = e

	return e
}

// Path returns the path the engine was opened with.
func (e *Engine) Path() string { return e.path }

// PerformMaintenance is for periodic maintenance of the store. A no-op for b1
func (e *Engine) PerformMaintenance() {
	if f := e.WAL.shouldFlush(); f != noFlush {
		go func() {
			if err := e.WAL.flush(f); err != nil {
				e.logger.Printf("PerformMaintenance: WAL flush failed: %v", err)
			}
		}()
		return
	}

	// don't do a full compaction if the WAL received writes in the time window
	if time.Since(e.WAL.LastWriteTime()) < e.IndexCompactionFullAge {
		return
	}

	e.filesLock.RLock()
	running := e.compactionRunning
	deletesPending := len(e.deletes) > 0
	e.filesLock.RUnlock()
	if running || deletesPending {
		return
	}

	// do a full compaction if all the index files are older than the compaction time
	for _, f := range e.copyFilesCollection() {
		if time.Since(f.modTime) < e.IndexCompactionFullAge {
			return
		}
	}

	go func() {
		if err := e.Compact(true); err != nil {
			e.logger.Printf("PerformMaintenance: error during compaction: %v", err)
		}
	}()
}

// Format returns the format type of this engine
func (e *Engine) Format() tsdb.EngineFormat {
	return tsdb.TSM1Format
}

// Open opens and initializes the engine.
func (e *Engine) Open() error {
	if err := os.MkdirAll(e.path, 0777); err != nil {
		return err
	}

	// perform any cleanup on metafiles that were halfway written
	e.cleanupMetafile(SeriesFileExtension)
	e.cleanupMetafile(FieldsFileExtension)
	e.cleanupMetafile(IDsFileExtension)
	e.cleanupMetafile(CollisionsFileExtension)

	e.cleanupUnfinishedCompaction()

	files, err := filepath.Glob(filepath.Join(e.path, fmt.Sprintf("*.%s", Format)))
	if err != nil {
		return err
	}
	for _, fn := range files {
		// if the file has a checkpoint it's not valid, so remove it
		if removed := e.removeFileIfCheckpointExists(fn); removed {
			continue
		}

		id, err := idFromFileName(fn)
		if err != nil {
			return err
		}
		if id >= e.currentFileID {
			e.currentFileID = id + 1
		}
		f, err := os.OpenFile(fn, os.O_RDONLY, 0666)
		if err != nil {
			return fmt.Errorf("error opening file %s: %s", fn, err.Error())
		}
		df, err := NewDataFile(f)
		if err != nil {
			return fmt.Errorf("error opening memory map for file %s: %s", fn, err.Error())
		}
		e.files = append(e.files, df)
	}
	sort.Sort(e.files)

	if err := e.readCollisions(); err != nil {
		return err
	}

	e.deletes = make(map[uint64]string)
	e.deleteMeasurements = make(map[string]bool)

	// mark the last compaction as now so it doesn't try to compact while
	// flushing the WAL on load
	e.lastCompactionTime = time.Now()

	if err := e.WAL.Open(); err != nil {
		return err
	}

	e.lastCompactionTime = time.Now()

	return nil
}

// cleanupUnfinishedConpaction will read any compaction markers. If the marker exists, the compaction finished successfully,
// but didn't get fully cleaned up. Remove the old files and their checkpoints
func (e *Engine) cleanupUnfinishedCompaction() {
	files, err := filepath.Glob(filepath.Join(e.path, fmt.Sprintf("*.%s", CompactionExtension)))
	if err != nil {
		panic(fmt.Sprintf("error getting compaction checkpoints: %s", err.Error()))
	}

	for _, fn := range files {
		f, err := os.OpenFile(fn, os.O_RDONLY, 0666)
		if err != nil {
			panic(fmt.Sprintf("error opening compaction info file: %s", err.Error()))
		}
		data, err := ioutil.ReadAll(f)
		if err != nil {
			panic(fmt.Sprintf("error reading compaction info file: %s", err.Error()))
		}

		c := &compactionCheckpoint{}
		err = json.Unmarshal(data, c)
		if err == nil {
			c.cleanup()
		}

		if err := f.Close(); err != nil {
			panic(fmt.Sprintf("error closing compaction checkpoint: %s", err.Error()))
		}
		if err := os.RemoveAll(f.Name()); err != nil {
			panic(fmt.Sprintf("error removing compaction checkpoint: %s", err.Error()))
		}
	}
}

// Close closes the engine.
func (e *Engine) Close() error {
	// get all the locks so queries, writes, and compactions stop before closing
	e.queryLock.Lock()
	defer e.queryLock.Unlock()
	e.metaLock.Lock()
	defer e.metaLock.Unlock()
	min, max := int64(math.MinInt64), int64(math.MaxInt64)
	e.writeLock.LockRange(min, max)
	defer e.writeLock.UnlockRange(min, max)
	e.filesLock.Lock()
	defer e.filesLock.Unlock()

	e.WAL.Close()

	// ensure all deletes have been processed
	e.deletesPending.Wait()

	for _, df := range e.files {
		_ = df.Close()
	}
	e.files = nil
	e.currentFileID = 0
	e.collisions = nil
	e.deletes = nil
	e.deleteMeasurements = nil
	return nil
}

// DataFileCount returns the number of data files in the database
func (e *Engine) DataFileCount() int {
	e.filesLock.RLock()
	defer e.filesLock.RUnlock()
	return len(e.files)
}

// SetLogOutput is a no-op.
func (e *Engine) SetLogOutput(w io.Writer) {}

// LoadMetadataIndex loads the shard metadata into memory.
func (e *Engine) LoadMetadataIndex(shard *tsdb.Shard, index *tsdb.DatabaseIndex, measurementFields map[string]*tsdb.MeasurementFields) error {
	// Load measurement metadata
	fields, err := e.readFields()
	if err != nil {
		return err
	}
	for k, mf := range fields {
		m := index.CreateMeasurementIndexIfNotExists(string(k))
		for name := range mf.Fields {
			m.SetFieldName(name)
		}
		mf.Codec = tsdb.NewFieldCodec(mf.Fields)
		measurementFields[m.Name] = mf
	}

	// Load series metadata
	series, err := e.readSeries()
	if err != nil {
		return err
	}

	// Load the series into the in-memory index in sorted order to ensure
	// it's always consistent for testing purposes
	a := make([]string, 0, len(series))
	for k := range series {
		a = append(a, k)
	}
	sort.Strings(a)
	for _, key := range a {
		s := series[key]
		s.InitializeShards()
		index.CreateSeriesIndexIfNotExists(tsdb.MeasurementFromSeriesKey(string(key)), s)
	}

	return nil
}

// WritePoints writes metadata and point data into the engine.
// Returns an error if new points are added to an existing key.
func (e *Engine) WritePoints(points []models.Point, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
	return e.WAL.WritePoints(points, measurementFieldsToSave, seriesToCreate)
}

func (e *Engine) Write(pointsByKey map[string]Values, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
	// Flush any deletes before writing new data from the WAL
	e.filesLock.RLock()
	hasDeletes := len(e.deletes) > 0
	e.filesLock.RUnlock()
	if hasDeletes {
		e.flushDeletes()
	}

	startTime, endTime, valuesByID, err := e.convertKeysAndWriteMetadata(pointsByKey, measurementFieldsToSave, seriesToCreate)
	if err != nil {
		return err
	}
	if len(valuesByID) == 0 {
		return nil
	}

	files, lockStart, lockEnd := e.filesAndLock(startTime, endTime)
	defer e.writeLock.UnlockRange(lockStart, lockEnd)

	if len(files) == 0 {
		return e.rewriteFile(nil, valuesByID)
	}

	maxTime := int64(math.MaxInt64)

	// do the file rewrites in parallel
	var mu sync.Mutex
	var writes sync.WaitGroup
	var errors []error

	// reverse through the data files and write in the data
	for i := len(files) - 1; i >= 0; i-- {
		f := files[i]
		// max times are exclusive, so add 1 to it
		fileMax := f.MaxTime() + 1
		fileMin := f.MinTime()
		// if the file is < rotate, write all data between fileMin and maxTime
		if f.size < e.RotateFileSize {
			writes.Add(1)
			go func(df *dataFile, vals map[uint64]Values) {
				if err := e.rewriteFile(df, vals); err != nil {
					mu.Lock()
					errors = append(errors, err)
					mu.Unlock()
				}
				writes.Done()
			}(f, e.filterDataBetweenTimes(valuesByID, fileMin, maxTime))
			continue
		}
		// if the file is > rotate:
		//   write all data between fileMax and maxTime into new file
		//   write all data between fileMin and fileMax into old file
		writes.Add(1)
		go func(vals map[uint64]Values) {
			if err := e.rewriteFile(nil, vals); err != nil {
				mu.Lock()
				errors = append(errors, err)
				mu.Unlock()
			}
			writes.Done()
		}(e.filterDataBetweenTimes(valuesByID, fileMax, maxTime))
		writes.Add(1)
		go func(df *dataFile, vals map[uint64]Values) {
			if err := e.rewriteFile(df, vals); err != nil {
				mu.Lock()
				errors = append(errors, err)
				mu.Unlock()
			}
			writes.Done()
		}(f, e.filterDataBetweenTimes(valuesByID, fileMin, fileMax))
		maxTime = fileMin
	}
	// for any data leftover, write into a new file since it's all older
	// than any file we currently have
	writes.Add(1)
	go func() {
		if err := e.rewriteFile(nil, valuesByID); err != nil {
			mu.Lock()
			errors = append(errors, err)
			mu.Unlock()
		}
		writes.Done()
	}()

	writes.Wait()

	if len(errors) > 0 {
		// TODO: log errors
		return errors[0]
	}

	if !e.SkipCompaction && e.shouldCompact() {
		go func() {
			if err := e.Compact(false); err != nil {
				e.logger.Printf("Write: error during compaction: %v", err)
			}
		}()
	}

	return nil
}

// MarkDeletes will mark the given keys for deletion in memory. They will be deleted from data
// files on the next flush. This mainly for the WAL to use on startup
func (e *Engine) MarkDeletes(keys []string) {
	e.filesLock.Lock()
	defer e.filesLock.Unlock()
	for _, k := range keys {
		e.deletes[e.keyToID(k)] = k
	}
}

func (e *Engine) MarkMeasurementDelete(name string) {
	e.filesLock.Lock()
	defer e.filesLock.Unlock()
	e.deleteMeasurements[name] = true
}

// filesAndLock returns the data files that match the given range and
// ensures that the write lock will hold for the entire range
func (e *Engine) filesAndLock(min, max int64) (a dataFiles, lockStart, lockEnd int64) {
	for {
		a = make([]*dataFile, 0)
		files := e.copyFilesCollection()

		e.filesLock.RLock()
		for _, f := range e.files {
			fmin, fmax := f.MinTime(), f.MaxTime()
			if min < fmax && fmin >= fmin {
				a = append(a, f)
			} else if max >= fmin && max < fmax {
				a = append(a, f)
			}
		}
		e.filesLock.RUnlock()

		if len(a) > 0 {
			lockStart = a[0].MinTime()
			lockEnd = a[len(a)-1].MaxTime()
			if max > lockEnd {
				lockEnd = max
			}
		} else {
			lockStart = min
			lockEnd = max
		}

		e.writeLock.LockRange(lockStart, lockEnd)

		// it's possible for compaction to change the files collection while we
		// were waiting for a write lock on the range. Make sure the files are still the
		// same after we got the lock, otherwise try again. This shouldn't happen often.
		filesAfterLock := e.copyFilesCollection()
		if dataFilesEquals(files, filesAfterLock) {
			return
		}

		e.writeLock.UnlockRange(lockStart, lockEnd)
	}
}

// getCompactionFiles will return the list of files ready to be compacted along with the min and
// max time of the write lock obtained for compaction
func (e *Engine) getCompactionFiles(fullCompaction bool) (minTime, maxTime int64, files dataFiles) {
	// we're looping here to ensure that the files we've marked to compact are
	// still there after we've obtained the write lock
	for {
		if fullCompaction {
			files = e.copyFilesCollection()
		} else {
			files = e.filesToCompact()
		}
		if len(files) < 2 {
			return minTime, maxTimeOffset, nil
		}
		minTime = files[0].MinTime()
		maxTime = files[len(files)-1].MaxTime()

		e.writeLock.LockRange(minTime, maxTime)

		// if the files are different after obtaining the write lock, one or more
		// was rewritten. Release the lock and try again. This shouldn't happen really.
		var filesAfterLock dataFiles
		if fullCompaction {
			filesAfterLock = e.copyFilesCollection()
		} else {
			filesAfterLock = e.filesToCompact()
		}
		if !dataFilesEquals(files, filesAfterLock) {
			e.writeLock.UnlockRange(minTime, maxTime)
			continue
		}

		// we've got the write lock and the files are all there
		return
	}
}

// compactToNewFiles will compact the passed in data files into as few files as possible
func (e *Engine) compactToNewFiles(minTime, maxTime int64, files dataFiles) []*os.File {
	fileName := e.nextFileName()
	e.logger.Printf("Starting compaction in %s of %d files to new file %s", e.path, len(files), fileName)

	compaction := newCompactionJob(files, minTime, maxTime, e.MaxFileSize, e.MaxPointsPerBlock)
	compaction.newCurrentFile(fileName)

	// loop writing data until we've read through all the files
	for {
		nextID := compaction.nextID()
		if nextID == dataFileEOF {
			break
		}

		// write data for this ID while rotating to new files if necessary
		for {
			moreToWrite := compaction.writeBlocksForID(nextID)
			if !moreToWrite {
				break
			}
			compaction.newCurrentFile(e.nextFileName())
		}
	}

	// close out the current compacted file
	compaction.writeOutCurrentFile()

	return compaction.newFiles
}

// Compact will compact data files in the directory into the fewest possible data files they
// can be combined into
func (e *Engine) Compact(fullCompaction bool) error {
	minTime, maxTime, files := e.getCompactionFiles(fullCompaction)
	if len(files) < 2 {
		return nil
	}

	// mark the compaction as running
	e.filesLock.Lock()
	if e.compactionRunning {
		e.filesLock.Unlock()
		return nil
	}
	e.compactionRunning = true
	e.filesLock.Unlock()
	defer func() {
		//release the lock
		e.writeLock.UnlockRange(minTime, maxTime)
		e.filesLock.Lock()
		e.lastCompactionTime = time.Now()
		e.compactionRunning = false
		e.filesLock.Unlock()
	}()

	st := time.Now()

	newFiles := e.compactToNewFiles(minTime, maxTime, files)

	newDataFiles := make(dataFiles, len(newFiles))
	for i, f := range newFiles {
		// now open it as a memory mapped data file
		newDF, err := NewDataFile(f)
		if err != nil {
			return err
		}
		newDataFiles[i] = newDF
	}

	// write the compaction file to note that we've successfully commpleted the write portion of compaction
	compactedFileNames := make([]string, len(files))
	newFileNames := make([]string, len(newFiles))
	for i, f := range files {
		compactedFileNames[i] = f.f.Name()
	}
	for i, f := range newFiles {
		newFileNames[i] = f.Name()
	}
	compactionCheckpointName, err := e.writeCompactionCheckpointFile(compactedFileNames, newFileNames)
	if err != nil {
		return err
	}

	// update engine with new file pointers
	e.filesLock.Lock()
	var replacementFiles dataFiles
	for _, df := range e.files {
		// exclude any files that were compacted
		include := true
		for _, f := range files {
			if f == df {
				include = false
				break
			}
		}
		if include {
			replacementFiles = append(replacementFiles, df)
		}
	}
	replacementFiles = append(replacementFiles, newDataFiles...)
	sort.Sort(replacementFiles)
	e.files = replacementFiles
	e.filesLock.Unlock()

	e.logger.Printf("Compaction of %s took %s", e.path, time.Since(st))

	e.clearCompactedFiles(compactionCheckpointName, newFiles, files)

	return nil
}

// clearCompactedFiles will remove the compaction checkpoints for new files, remove the old compacted files, and
// finally remove the compaction checkpoint
func (e *Engine) clearCompactedFiles(compactionCheckpointName string, newFiles []*os.File, oldFiles dataFiles) {
	// delete the old files in a goroutine so running queries won't block the write
	// from completing
	e.deletesPending.Add(1)
	go func() {
		// first clear out the compaction checkpoints
		for _, f := range newFiles {
			if err := removeCheckpoint(f.Name()); err != nil {
				// panic here since continuing could cause data loss. It's better to fail hard so
				// everything can be recovered on restart
				panic(fmt.Sprintf("error removing checkpoint file %s: %s", f.Name(), err.Error()))
			}
		}

		// now delete the underlying data files
		for _, f := range oldFiles {
			if err := f.Delete(); err != nil {
				panic(fmt.Sprintf("error deleting old file after compaction %s: %s", f.f.Name(), err.Error()))
			}
		}

		// finally remove the compaction marker
		if err := os.RemoveAll(compactionCheckpointName); err != nil {
			e.logger.Printf("error removing %s: %s", compactionCheckpointName, err.Error())
		}

		e.deletesPending.Done()
	}()
}

// writeCompactionCheckpointFile will save the compacted filenames and new filenames in
// a file. This is used on startup to clean out files that weren't deleted if the server
// wasn't shut down cleanly.
func (e *Engine) writeCompactionCheckpointFile(compactedFiles, newFiles []string) (string, error) {
	m := &compactionCheckpoint{
		CompactedFiles: compactedFiles,
		NewFiles:       newFiles,
	}

	data, err := json.Marshal(m)
	if err != nil {
		return "", err
	}

	// make the compacted filename the same name as the first compacted file, but with the compacted extension
	name := strings.Split(filepath.Base(compactedFiles[0]), ".")[0]
	fn := fmt.Sprintf("%s.%s", name, CompactionExtension)
	fileName := filepath.Join(filepath.Dir(compactedFiles[0]), fn)

	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return "", err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fileName, err
	}

	return fileName, f.Close()
}

func (e *Engine) writeIndexAndGetDataFile(f *os.File, minTime, maxTime int64, ids []uint64, newPositions []uint32) (*dataFile, error) {
	if err := writeIndex(f, minTime, maxTime, ids, newPositions); err != nil {
		return nil, err
	}

	if err := removeCheckpoint(f.Name()); err != nil {
		return nil, err
	}

	// now open it as a memory mapped data file
	newDF, err := NewDataFile(f)
	if err != nil {
		return nil, err
	}

	return newDF, nil
}

func (e *Engine) shouldCompact() bool {
	e.filesLock.RLock()
	running := e.compactionRunning
	since := time.Since(e.lastCompactionTime)
	deletesPending := len(e.deletes) > 0
	e.filesLock.RUnlock()
	if running || since < e.IndexMinCompactionInterval || deletesPending {
		return false
	}
	return len(e.filesToCompact()) >= e.MinCompactionFileCount
}

func (e *Engine) filesToCompact() dataFiles {
	e.filesLock.RLock()
	defer e.filesLock.RUnlock()

	var a dataFiles
	for _, df := range e.files {
		if time.Since(df.modTime) > e.CompactionAge && df.size < e.MaxFileSize {
			a = append(a, df)
		} else if len(a) > 0 {
			// only compact contiguous ranges. If we hit the negative case and
			// there are files to compact, stop here
			break
		}
	}
	return a
}

func (e *Engine) convertKeysAndWriteMetadata(pointsByKey map[string]Values, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) (minTime, maxTime int64, valuesByID map[uint64]Values, err error) {
	e.metaLock.Lock()
	defer e.metaLock.Unlock()

	if err := e.writeNewFields(measurementFieldsToSave); err != nil {
		return 0, 0, nil, err
	}
	if err := e.writeNewSeries(seriesToCreate); err != nil {
		return 0, 0, nil, err
	}

	if len(pointsByKey) == 0 {
		return 0, 0, nil, nil
	}

	// read in keys and assign any that aren't defined
	b, err := e.readCompressedFile(IDsFileExtension)
	if err != nil {
		return 0, 0, nil, err
	}
	ids := make(map[string]uint64)
	if b != nil {
		if err := json.Unmarshal(b, &ids); err != nil {
			return 0, 0, nil, err
		}
	}

	// these are values that are newer than anything stored in the shard
	valuesByID = make(map[uint64]Values)

	idToKey := make(map[uint64]string)    // we only use this map if new ids are being created
	collisions := make(map[string]uint64) // we only use this if a collision is encountered
	newKeys := false
	// track the min and max time of values being inserted so we can lock that time range
	minTime = int64(math.MaxInt64)
	maxTime = int64(math.MinInt64)
	for k, values := range pointsByKey {
		var id uint64
		var ok bool
		if id, ok = ids[k]; !ok {
			// populate the map if we haven't already

			if len(idToKey) == 0 {
				for n, id := range ids {
					idToKey[id] = n
				}
			}

			// now see if the hash id collides with a different key
			hashID := e.HashSeriesField(k)
			existingKey, idInMap := idToKey[hashID]
			// we only care if the keys are different. if so, it's a hash collision we have to keep track of
			if idInMap && k != existingKey {
				// we have a collision, find this new key the next available id
				hashID = 0
				for {
					hashID++
					if _, ok := idToKey[hashID]; !ok {
						// next ID is available, use it
						break
					}
				}
				collisions[k] = hashID
			}

			newKeys = true
			ids[k] = hashID
			idToKey[hashID] = k
			id = hashID
		}

		if minTime > values.MinTime() {
			minTime = values.MinTime()
		}
		if maxTime < values.MaxTime() {
			maxTime = values.MaxTime()
		}
		valuesByID[id] = values
	}

	if newKeys {
		b, err := json.Marshal(ids)
		if err != nil {
			return 0, 0, nil, err
		}
		if err := e.replaceCompressedFile(IDsFileExtension, b); err != nil {
			return 0, 0, nil, err
		}
	}

	if len(collisions) > 0 {
		e.saveNewCollisions(collisions)
	}

	return
}

func (e *Engine) saveNewCollisions(collisions map[string]uint64) error {
	e.collisionsLock.Lock()
	defer e.collisionsLock.Unlock()

	for k, v := range collisions {
		e.collisions[k] = v
	}

	data, err := json.Marshal(e.collisions)

	if err != nil {
		return err
	}

	return e.replaceCompressedFile(CollisionsFileExtension, data)
}

func (e *Engine) readCollisions() error {
	e.collisions = make(map[string]uint64)
	data, err := e.readCompressedFile(CollisionsFileExtension)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}

	return json.Unmarshal(data, &e.collisions)
}

// filterDataBetweenTimes will create a new map with data between
// the minTime (inclusive) and maxTime (exclusive) while removing that
// data from the passed in map. It is assume that the Values arrays
// are sorted in time ascending order
func (e *Engine) filterDataBetweenTimes(valuesByID map[uint64]Values, minTime, maxTime int64) map[uint64]Values {
	filteredValues := make(map[uint64]Values)
	for id, values := range valuesByID {
		maxIndex := len(values)
		minIndex := -1
		// find the index of the first value in the range
		for i, v := range values {
			t := v.UnixNano()
			if t >= minTime && t < maxTime {
				minIndex = i
				break
			}
		}
		if minIndex == -1 {
			continue
		}
		// go backwards to find the index of the last value in the range
		for i := len(values) - 1; i >= 0; i-- {
			t := values[i].UnixNano()
			if t < maxTime {
				maxIndex = i + 1
				break
			}
		}

		// write into the result map and filter the passed in map
		filteredValues[id] = values[minIndex:maxIndex]

		// if we grabbed all the values, remove them from the passed in map
		if minIndex == len(values) || (minIndex == 0 && maxIndex == len(values)) {
			delete(valuesByID, id)
			continue
		}

		valuesByID[id] = values[0:minIndex]
		if maxIndex < len(values) {
			valuesByID[id] = append(valuesByID[id], values[maxIndex:]...)
		}
	}
	return filteredValues
}

// rewriteFile will read in the old data file, if provided and merge the values
// in the passed map into a new data file
func (e *Engine) rewriteFile(oldDF *dataFile, valuesByID map[uint64]Values) error {
	if len(valuesByID) == 0 {
		return nil
	}

	// we need the values in sorted order so that we can merge them into the
	// new file as we read the old file
	ids := make([]uint64, 0, len(valuesByID))
	for id := range valuesByID {
		ids = append(ids, id)
	}

	minTime := int64(math.MaxInt64)
	maxTime := int64(math.MinInt64)

	// read header of ids to starting positions and times
	oldIDToPosition := make(map[uint64]uint32)
	if oldDF != nil {
		oldIDToPosition = oldDF.IDToPosition()
		minTime = oldDF.MinTime()
		maxTime = oldDF.MaxTime()
	}

	for _, v := range valuesByID {
		if minTime > v.MinTime() {
			minTime = v.MinTime()
		}
		if maxTime < v.MaxTime() {
			// add 1 ns to the time since maxTime is exclusive
			maxTime = v.MaxTime() + 1
		}
	}

	// add any ids that are in the file that aren't getting flushed here
	for id := range oldIDToPosition {
		if _, ok := valuesByID[id]; !ok {
			ids = append(ids, id)
		}
	}

	// always write in order by ID
	sort.Sort(uint64slice(ids))

	f, err := openFileAndCheckpoint(e.nextFileName())
	if err != nil {
		return err
	}

	if oldDF == nil || oldDF.Deleted() {
		e.logger.Printf("writing new index file %s", f.Name())
	} else {
		e.logger.Printf("rewriting index file %s with %s", oldDF.Name(), f.Name())
	}

	// now combine the old file data with the new values, keeping track of
	// their positions
	currentPosition := uint32(fileHeaderSize)
	newPositions := make([]uint32, len(ids))
	buf := make([]byte, e.MaxPointsPerBlock*20)
	for i, id := range ids {
		// mark the position for this ID
		newPositions[i] = currentPosition

		newVals := valuesByID[id]

		// if this id is only in the file and not in the new values, just copy over from old file
		if len(newVals) == 0 {
			fpos := oldIDToPosition[id]

			// write the blocks until we hit whatever the next id is
			for {
				fid := btou64(oldDF.mmap[fpos : fpos+8])
				if fid != id {
					break
				}
				length := btou32(oldDF.mmap[fpos+8 : fpos+12])
				if _, err := f.Write(oldDF.mmap[fpos : fpos+12+length]); err != nil {
					f.Close()
					return err
				}
				fpos += (12 + length)
				currentPosition += (12 + length)

				// make sure we're not at the end of the file
				if fpos >= oldDF.indexPosition() {
					break
				}
			}

			continue
		}

		// if the values are not in the file, just write the new ones
		fpos, ok := oldIDToPosition[id]
		if !ok {
			// TODO: ensure we encode only the amount in a block
			block, err := newVals.Encode(buf)
			if err != nil {
				f.Close()
				return err
			}

			if err := writeBlock(f, id, block); err != nil {
				f.Close()
				return err
			}
			currentPosition += uint32(blockHeaderSize + len(block))

			continue
		}

		// it's in the file and the new values, combine them and write out
		for {
			fid, _, block := oldDF.block(fpos)
			if fid != id {
				break
			}
			fpos += uint32(blockHeaderSize + len(block))

			// determine if there's a block after this with the same id and get its time
			nextID, nextTime, _ := oldDF.block(fpos)
			hasFutureBlock := nextID == id

			nv, newBlock, err := e.DecodeAndCombine(newVals, block, buf[:0], nextTime, hasFutureBlock)
			newVals = nv
			if err != nil {
				f.Close()
				return err
			}

			if _, err := f.Write(append(u64tob(id), u32tob(uint32(len(newBlock)))...)); err != nil {
				f.Close()
				return err
			}
			if _, err := f.Write(newBlock); err != nil {
				f.Close()
				return err
			}

			currentPosition += uint32(blockHeaderSize + len(newBlock))

			if fpos >= oldDF.indexPosition() {
				break
			}
		}

		// TODO: ensure we encode only the amount in a block, refactor this wil line 450 into func
		if len(newVals) > 0 {
			// TODO: ensure we encode only the amount in a block
			block, err := newVals.Encode(buf)
			if err != nil {
				f.Close()
				return err
			}

			if _, err := f.Write(append(u64tob(id), u32tob(uint32(len(block)))...)); err != nil {
				f.Close()
				return err
			}
			if _, err := f.Write(block); err != nil {
				f.Close()
				return err
			}
			currentPosition += uint32(blockHeaderSize + len(block))
		}
	}

	newDF, err := e.writeIndexAndGetDataFile(f, minTime, maxTime, ids, newPositions)
	if err != nil {
		f.Close()
		return err
	}

	// update the engine to point at the new dataFiles
	e.filesLock.Lock()
	var files dataFiles
	for _, df := range e.files {
		if df != oldDF {
			files = append(files, df)
		}
	}
	files = append(files, newDF)
	sort.Sort(files)
	e.files = files
	e.filesLock.Unlock()

	// remove the old data file. no need to block returning the write,
	// but we need to let any running queries finish before deleting it
	if oldDF != nil {
		e.deletesPending.Add(1)
		go func() {
			if err := oldDF.Delete(); err != nil {
				e.logger.Println("ERROR DELETING FROM REWRITE:", oldDF.f.Name())
			}
			e.deletesPending.Done()
		}()
	}

	return nil
}

// flushDeletes will lock the entire shard and rewrite all index files so they no
// longer contain the flushed IDs
func (e *Engine) flushDeletes() error {
	e.writeLock.LockRange(math.MinInt64, math.MaxInt64)
	defer e.writeLock.UnlockRange(math.MinInt64, math.MaxInt64)
	e.metaLock.Lock()
	defer e.metaLock.Unlock()

	measurements := make(map[string]bool)
	deletes := make(map[uint64]string)
	e.filesLock.RLock()
	for name := range e.deleteMeasurements {
		measurements[name] = true
	}
	for id, key := range e.deletes {
		deletes[id] = key
	}
	e.filesLock.RUnlock()

	// if we're deleting measurements, rewrite the field data
	if len(measurements) > 0 {
		fields, err := e.readFields()
		if err != nil {
			return err
		}
		for name := range measurements {
			delete(fields, name)
		}
		if err := e.writeFields(fields); err != nil {
			return err
		}
	}

	series, err := e.readSeries()
	if err != nil {
		return err
	}
	for _, key := range deletes {
		seriesName, _ := seriesAndFieldFromCompositeKey(key)
		delete(series, seriesName)
	}
	if err := e.writeSeries(series); err != nil {
		return err
	}

	// now remove the raw time series data from the data files
	files := e.copyFilesCollection()
	newFiles := make(dataFiles, 0, len(files))
	for _, f := range files {
		newFiles = append(newFiles, e.writeNewFileExcludeDeletes(f))
	}

	// update the delete map and files
	e.filesLock.Lock()
	defer e.filesLock.Unlock()

	e.files = newFiles

	// remove the things we've deleted from the map
	for name := range measurements {
		delete(e.deleteMeasurements, name)
	}
	for id := range deletes {
		delete(e.deletes, id)
	}

	e.deletesPending.Add(1)
	go func() {
		for _, oldDF := range files {
			if err := oldDF.Delete(); err != nil {
				e.logger.Println("ERROR DELETING FROM REWRITE:", oldDF.f.Name())
			}
		}
		e.deletesPending.Done()
	}()
	return nil
}

func (e *Engine) writeNewFileExcludeDeletes(oldDF *dataFile) *dataFile {
	f, err := openFileAndCheckpoint(e.nextFileName())
	if err != nil {
		panic(fmt.Sprintf("error opening new data file: %s", err.Error()))
	}

	var ids []uint64
	var positions []uint32

	indexPosition := oldDF.indexPosition()
	currentPosition := uint32(fileHeaderSize)
	currentID := uint64(0)
	for currentPosition < indexPosition {
		id := btou64(oldDF.mmap[currentPosition : currentPosition+8])
		length := btou32(oldDF.mmap[currentPosition+8 : currentPosition+blockHeaderSize])
		newPosition := currentPosition + blockHeaderSize + length

		if _, ok := e.deletes[id]; ok {
			currentPosition = newPosition
			continue
		}

		if _, err := f.Write(oldDF.mmap[currentPosition:newPosition]); err != nil {
			panic(fmt.Sprintf("error writing new index file: %s", err.Error()))
		}
		if id != currentID {
			currentID = id
			ids = append(ids, id)
			positions = append(positions, currentPosition)
		}
		currentPosition = newPosition
	}

	df, err := e.writeIndexAndGetDataFile(f, oldDF.MinTime(), oldDF.MaxTime(), ids, positions)
	if err != nil {
		panic(fmt.Sprintf("error writing new index file: %s", err.Error()))
	}

	return df
}

func (e *Engine) nextFileName() string {
	e.filesLock.Lock()
	defer e.filesLock.Unlock()
	e.currentFileID++
	return filepath.Join(e.path, fmt.Sprintf("%07d.%s", e.currentFileID, Format))
}

func (e *Engine) readCompressedFile(name string) ([]byte, error) {
	f, err := os.OpenFile(filepath.Join(e.path, name), os.O_RDONLY, 0666)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	data, err := snappy.Decode(nil, b)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (e *Engine) replaceCompressedFile(name string, data []byte) error {
	tmpName := filepath.Join(e.path, name+"tmp")
	f, err := os.OpenFile(tmpName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	b := snappy.Encode(nil, data)
	if _, err := f.Write(b); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, filepath.Join(e.path, name))
}

// keysWithFields takes the map of measurements to their fields and a set of series keys
// and returns the columnar keys for the keys and fields
func (e *Engine) keysWithFields(fields map[string]*tsdb.MeasurementFields, keys []string) []string {
	e.WAL.cacheLock.RLock()
	defer e.WAL.cacheLock.RUnlock()

	var a []string
	for _, k := range keys {
		measurement := tsdb.MeasurementFromSeriesKey(k)

		// add the fields from the index
		mf := fields[measurement]
		if mf != nil {
			for _, f := range mf.Fields {
				a = append(a, SeriesFieldKey(k, f.Name))
			}
		}

		// now add any fields from the WAL that haven't been flushed yet
		mf = e.WAL.measurementFieldsCache[measurement]
		if mf != nil {
			for _, f := range mf.Fields {
				a = append(a, SeriesFieldKey(k, f.Name))
			}
		}
	}

	return a
}

// DeleteSeries deletes the series from the engine.
func (e *Engine) DeleteSeries(seriesKeys []string) error {
	e.metaLock.Lock()
	defer e.metaLock.Unlock()

	fields, err := e.readFields()
	if err != nil {
		return err
	}

	keyFields := e.keysWithFields(fields, seriesKeys)
	e.filesLock.Lock()
	defer e.filesLock.Unlock()
	for _, key := range keyFields {
		e.deletes[e.keyToID(key)] = key
	}

	return e.WAL.DeleteSeries(keyFields)
}

// DeleteMeasurement deletes a measurement and all related series.
func (e *Engine) DeleteMeasurement(name string, seriesKeys []string) error {
	e.metaLock.Lock()
	defer e.metaLock.Unlock()

	fields, err := e.readFields()
	if err != nil {
		return err
	}

	// mark the measurement, series keys and the fields for deletion on the next flush
	// also serves as a tombstone for any queries that come in before the flush
	keyFields := e.keysWithFields(fields, seriesKeys)
	e.filesLock.Lock()
	defer e.filesLock.Unlock()

	e.deleteMeasurements[name] = true
	for _, k := range keyFields {
		e.deletes[e.keyToID(k)] = k
	}

	return e.WAL.DeleteMeasurement(name, seriesKeys)
}

// SeriesCount returns the number of series buckets on the shard.
func (e *Engine) SeriesCount() (n int, err error) {
	return 0, nil
}

// Begin starts a new transaction on the engine.
func (e *Engine) Begin(writable bool) (tsdb.Tx, error) {
	e.queryLock.RLock()

	var files dataFiles

	// we do this to ensure that the data files haven't been deleted from a compaction
	// while we were waiting to get the query lock
	for {
		files = e.copyFilesCollection()

		// get the query lock
		for _, f := range files {
			f.mu.RLock()
		}

		// ensure they're all still open
		reset := false
		for _, f := range files {
			if f.f == nil {
				reset = true
				break
			}
		}

		// if not, release and try again
		if reset {
			for _, f := range files {
				f.mu.RUnlock()
			}
			continue
		}

		// we're good to go
		break
	}

	return &tx{files: files, engine: e}, nil
}

func (e *Engine) WriteTo(w io.Writer) (n int64, err error) { panic("not implemented") }

func (e *Engine) keyToID(key string) uint64 {
	// get the ID for the key and be sure to check if it had hash collision before
	e.collisionsLock.RLock()
	id, ok := e.collisions[key]
	e.collisionsLock.RUnlock()

	if !ok {
		id = e.HashSeriesField(key)
	}
	return id
}

func (e *Engine) keyAndFieldToID(series, field string) uint64 {
	key := SeriesFieldKey(series, field)
	return e.keyToID(key)
}

func (e *Engine) copyFilesCollection() []*dataFile {
	e.filesLock.RLock()
	defer e.filesLock.RUnlock()
	a := make([]*dataFile, len(e.files))
	copy(a, e.files)
	return a
}

func (e *Engine) writeNewFields(measurementFieldsToSave map[string]*tsdb.MeasurementFields) error {
	if len(measurementFieldsToSave) == 0 {
		return nil
	}

	// read in all the previously saved fields
	fields, err := e.readFields()
	if err != nil {
		return err
	}

	// add the new ones or overwrite old ones
	for name, mf := range measurementFieldsToSave {
		fields[name] = mf
	}

	return e.writeFields(fields)
}

func (e *Engine) writeFields(fields map[string]*tsdb.MeasurementFields) error {
	// compress and save everything
	data, err := json.Marshal(fields)
	if err != nil {
		return err
	}

	fn := filepath.Join(e.path, FieldsFileExtension+"tmp")
	ff, err := os.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	_, err = ff.Write(snappy.Encode(nil, data))
	if err != nil {
		return err
	}
	if err := ff.Close(); err != nil {
		return err
	}
	fieldsFileName := filepath.Join(e.path, FieldsFileExtension)
	return os.Rename(fn, fieldsFileName)
}

func (e *Engine) readFields() (map[string]*tsdb.MeasurementFields, error) {
	fields := make(map[string]*tsdb.MeasurementFields)

	f, err := os.OpenFile(filepath.Join(e.path, FieldsFileExtension), os.O_RDONLY, 0666)
	if os.IsNotExist(err) {
		return fields, nil
	} else if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	data, err := snappy.Decode(nil, b)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}

	return fields, nil
}

func (e *Engine) writeNewSeries(seriesToCreate []*tsdb.SeriesCreate) error {
	if len(seriesToCreate) == 0 {
		return nil
	}

	// read in previously saved series
	series, err := e.readSeries()
	if err != nil {
		return err
	}

	// add new ones, compress and save
	for _, s := range seriesToCreate {
		series[s.Series.Key] = s.Series
	}

	return e.writeSeries(series)
}

func (e *Engine) writeSeries(series map[string]*tsdb.Series) error {
	data, err := json.Marshal(series)
	if err != nil {
		return err
	}

	fn := filepath.Join(e.path, SeriesFileExtension+"tmp")
	ff, err := os.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	_, err = ff.Write(snappy.Encode(nil, data))
	if err != nil {
		return err
	}
	if err := ff.Close(); err != nil {
		return err
	}
	seriesFileName := filepath.Join(e.path, SeriesFileExtension)
	return os.Rename(fn, seriesFileName)
}

func (e *Engine) readSeries() (map[string]*tsdb.Series, error) {
	series := make(map[string]*tsdb.Series)

	f, err := os.OpenFile(filepath.Join(e.path, SeriesFileExtension), os.O_RDONLY, 0666)
	if os.IsNotExist(err) {
		return series, nil
	} else if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	data, err := snappy.Decode(nil, b)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &series); err != nil {
		return nil, err
	}

	return series, nil
}

// DecodeAndCombine take an encoded block from a file, decodes it and interleaves the file
// values with the values passed in. nextTime and hasNext refer to if the file
// has future encoded blocks so that this method can know how much of its values can be
// combined and output in the resulting encoded block.
func (e *Engine) DecodeAndCombine(newValues Values, block, buf []byte, nextTime int64, hasFutureBlock bool) (Values, []byte, error) {
	// No new values passed in, so nothing to combine.  Just return the existing block.
	if len(newValues) == 0 {
		return newValues, block, nil
	}

	var values []Value
	values, err := DecodeBlock(block, values)
	if err != nil {
		panic(fmt.Sprintf("failure decoding block: %v", err))
	}

	var remainingValues Values

	if hasFutureBlock {
		// take all values that have times less than the future block and update the vals array
		pos := sort.Search(len(newValues), func(i int) bool {
			return newValues[i].Time().UnixNano() >= nextTime
		})
		values = append(values, newValues[:pos]...)
		remainingValues = newValues[pos:]
		values = Values(values).Deduplicate()
	} else {
		requireSort := Values(values).MaxTime() >= newValues.MinTime()
		values = append(values, newValues...)
		if requireSort {
			values = Values(values).Deduplicate()
		}
	}

	if len(values) > e.MaxPointsPerBlock {
		remainingValues = values[e.MaxPointsPerBlock:]
		values = values[:e.MaxPointsPerBlock]
	}

	encoded, err := Values(values).Encode(buf)
	if err != nil {
		return nil, nil, err
	}
	return remainingValues, encoded, nil
}

// removeFileIfCheckpointExists will remove the file if its associated checkpoint fil is there.
// It returns true if the file was removed. This is for recovery of data files on startup
func (e *Engine) removeFileIfCheckpointExists(fileName string) bool {
	checkpointName := fmt.Sprintf("%s.%s", fileName, CheckpointExtension)
	_, err := os.Stat(checkpointName)

	// if there's no checkpoint, move on
	if err != nil {
		return false
	}

	// there's a checkpoint so we know this file isn't safe so we should remove it
	err = os.RemoveAll(fileName)
	if err != nil {
		panic(fmt.Sprintf("error removing file %s", err.Error()))
	}

	err = os.RemoveAll(checkpointName)
	if err != nil {
		panic(fmt.Sprintf("error removing file %s", err.Error()))
	}

	return true
}

// cleanupMetafile will remove the tmp file if the other file exists, or rename the
// tmp file to be a regular file if the normal file is missing. This is for recovery on
// startup.
func (e *Engine) cleanupMetafile(name string) {
	fileName := filepath.Join(e.path, name)
	tmpName := fileName + "tmp"

	_, err := os.Stat(tmpName)

	// if the tmp file isn't there, we can just exit
	if err != nil {
		return
	}

	_, err = os.Stat(fileName)

	// the regular file is there so we should just remove the tmp file
	if err == nil {
		err = os.Remove(tmpName)
		if err != nil {
			panic(fmt.Sprintf("error removing meta file %s: %s", tmpName, err.Error()))
		}
	}

	// regular file isn't there so have the tmp file take its place
	err = os.Rename(tmpName, fileName)
	if err != nil {
		panic(fmt.Sprintf("error renaming meta file %s: %s", tmpName, err.Error()))
	}
}

// compactionJob contains the data and methods for compacting multiple data files
// into fewer larger data files that ideally have larger blocks of points together
type compactionJob struct {
	idsInCurrentFile  []uint64
	startingPositions []uint32
	newFiles          []*os.File

	dataFilesToCompact []*dataFile
	dataFilePositions  []uint32
	currentDataFileIDs []uint64

	currentFile     *os.File
	currentPosition uint32

	maxFileSize       uint32
	maxPointsPerBlock int

	minTime int64
	maxTime int64

	// leftoverValues holds values from an ID that is getting split across multiple
	// compacted data files
	leftoverValues Values

	// buffer for encoding
	buf []byte
}

// dataFileOEF is a sentinel values marking that there is no more data to be read from the data file
const dataFileEOF = uint64(math.MaxUint64)

func newCompactionJob(files dataFiles, minTime, maxTime int64, maxFileSize uint32, maxPointsPerBlock int) *compactionJob {
	c := &compactionJob{
		dataFilesToCompact: files,
		dataFilePositions:  make([]uint32, len(files)),
		currentDataFileIDs: make([]uint64, len(files)),
		maxFileSize:        maxFileSize,
		maxPointsPerBlock:  maxPointsPerBlock,
		minTime:            minTime,
		maxTime:            maxTime,
		buf:                make([]byte, blockBufferSize),
	}

	// set the starting positions and ids for the files getting compacted
	for i, df := range files {
		c.dataFilePositions[i] = uint32(fileHeaderSize)
		c.currentDataFileIDs[i] = df.idForPosition(uint32(fileHeaderSize))
	}

	return c
}

// newCurrentFile will create a new compaction file and reset the ids and positions
// in the file so we can write the index out later
func (c *compactionJob) newCurrentFile(fileName string) {
	c.idsInCurrentFile = make([]uint64, 0)
	c.startingPositions = make([]uint32, 0)

	f, err := openFileAndCheckpoint(fileName)
	if err != nil {
		panic(fmt.Sprintf("error opening new file: %s", err.Error()))
	}
	c.currentFile = f
	c.currentPosition = uint32(fileHeaderSize)
}

// writeBlocksForID will read data for the given ID from all the files getting compacted
// and write it into a new compacted file. Blocks from different files will be combined to
// create larger blocks in the compacted file. If the compacted file goes over the max
// file size limit, true will be returned indicating that its time to create a new compaction file
func (c *compactionJob) writeBlocksForID(id uint64) bool {
	// mark this ID as new and track its starting position
	c.idsInCurrentFile = append(c.idsInCurrentFile, id)
	c.startingPositions = append(c.startingPositions, c.currentPosition)

	// loop through the files in order emptying each one of its data for this ID

	// first handle any values that didn't get written to the previous
	// compaction file because it was too large
	previousValues := c.leftoverValues
	c.leftoverValues = nil
	rotateFile := false
	for i, df := range c.dataFilesToCompact {
		idForFile := c.currentDataFileIDs[i]

		// if the next ID in this file doesn't match, move to the next file
		if idForFile != id {
			continue
		}

		var newFilePosition uint32
		var nextID uint64

		// write out the values and keep track of the next ID and position in this file
		previousValues, rotateFile, newFilePosition, nextID = c.writeIDFromFile(id, previousValues, c.dataFilePositions[i], df)
		c.dataFilePositions[i] = newFilePosition
		c.currentDataFileIDs[i] = nextID

		// if we hit the max file size limit, return so a new file to compact into can be allocated
		if rotateFile {
			c.leftoverValues = previousValues
			c.writeOutCurrentFile()
			return true
		}
	}

	if len(previousValues) > 0 {
		bytesWritten := writeValues(c.currentFile, id, previousValues, c.buf)
		c.currentPosition += bytesWritten
	}

	return false
}

// writeIDFromFile will read all data from the passed in file for the given ID and either buffer the values in memory if below the
// max points allowed in a block, or write out to the file. The remaining buffer will be returned along with a bool indicating if
// we need a new file to compact into, the current position of the data file now that we've read data, and the next ID to be read
// from the data file
func (c *compactionJob) writeIDFromFile(id uint64, previousValues Values, filePosition uint32, df *dataFile) (Values, bool, uint32, uint64) {
	for {
		// check if we're at the end of the file
		indexPosition := df.indexPosition()
		if filePosition >= indexPosition {
			return previousValues, false, filePosition, dataFileEOF
		}

		// check if we're at the end of the blocks for this ID
		nextID, _, block := df.block(filePosition)
		if nextID != id {
			return previousValues, false, filePosition, nextID
		}

		blockLength := uint32(blockHeaderSize + len(block))
		filePosition += blockLength

		// decode the block and append to previous values
		// TODO: update this so that blocks already at their limit don't need decoding
		var values []Value
		values, err := DecodeBlock(block, values)
		if err != nil {
			panic(fmt.Sprintf("error decoding block: %s", err.Error()))
		}

		previousValues = append(previousValues, values...)

		// if we've hit the block limit, encode and write out to the file
		if len(previousValues) > c.maxPointsPerBlock {
			valuesToEncode := previousValues[:c.maxPointsPerBlock]
			previousValues = previousValues[c.maxPointsPerBlock:]

			bytesWritten := writeValues(c.currentFile, id, valuesToEncode, c.buf)
			c.currentPosition += bytesWritten

			// if we're at the limit of what should go into the current file,
			// return the values we've decoded and return the ID in the next
			// block
			if c.shouldRotateCurrentFile() {
				if filePosition >= indexPosition {
					return previousValues, true, filePosition, dataFileEOF
				}

				nextID, _, _ = df.block(filePosition)
				return previousValues, true, filePosition, nextID
			}
		}
	}
}

// nextID returns the lowest number ID to be read from one of the data files getting
// compacted. Will return an EOF if all files have been read and compacted
func (c *compactionJob) nextID() uint64 {
	minID := dataFileEOF
	for _, id := range c.currentDataFileIDs {
		if minID > id {
			minID = id
		}
	}

	// if the min is still EOF, we're done with all the data from all files to compact
	if minID == dataFileEOF {
		return dataFileEOF
	}

	return minID
}

// writeOutCurrentFile will write the index out to the current file in preparation for a new file to compact into
func (c *compactionJob) writeOutCurrentFile() {
	if c.currentFile == nil {
		return
	}

	// write out the current file
	if err := writeIndex(c.currentFile, c.minTime, c.maxTime, c.idsInCurrentFile, c.startingPositions); err != nil {
		panic(fmt.Sprintf("error writing index: %s", err.Error()))
	}

	// mark it as a new file and reset
	c.newFiles = append(c.newFiles, c.currentFile)
	c.currentFile = nil
}

// shouldRotateCurrentFile returns true if the current file is over the max file size
func (c *compactionJob) shouldRotateCurrentFile() bool {
	return c.currentPosition+footerSize(len(c.idsInCurrentFile)) > c.maxFileSize
}

type dataFile struct {
	f       *os.File
	mu      sync.RWMutex
	size    uint32
	modTime time.Time
	mmap    []byte
}

// byte size constants for the data file
const (
	fileHeaderSize     = 4
	seriesCountSize    = 4
	timeSize           = 8
	blockHeaderSize    = 12
	seriesIDSize       = 8
	seriesPositionSize = 4
	seriesHeaderSize   = seriesIDSize + seriesPositionSize
	minTimeOffset      = 20
	maxTimeOffset      = 12
)

func NewDataFile(f *os.File) (*dataFile, error) {
	// seek back to the beginning to hand off to the mmap
	if _, err := f.Seek(0, 0); err != nil {
		return nil, err
	}

	fInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	mmap, err := syscall.Mmap(int(f.Fd()), 0, int(fInfo.Size()), syscall.PROT_READ, syscall.MAP_SHARED|MAP_POPULATE)
	if err != nil {
		return nil, err
	}

	return &dataFile{
		f:       f,
		mmap:    mmap,
		size:    uint32(fInfo.Size()),
		modTime: fInfo.ModTime(),
	}, nil
}

func (d *dataFile) Name() string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.Deleted() {
		return ""
	}
	return d.f.Name()
}

func (d *dataFile) Deleted() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.f == nil
}

func (d *dataFile) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.close()
}

func (d *dataFile) Delete() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.close(); err != nil {
		return err
	}

	if d.f == nil {
		return nil
	}

	err := os.RemoveAll(d.f.Name())
	if err != nil {
		return err
	}
	d.f = nil
	return nil
}

func (d *dataFile) close() error {
	if d.mmap == nil {
		return nil
	}
	err := syscall.Munmap(d.mmap)
	if err != nil {
		return err
	}

	d.mmap = nil
	return d.f.Close()
}

func (d *dataFile) MinTime() int64 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.mmap) == 0 {
		return 0
	}
	minTimePosition := d.size - minTimeOffset
	timeBytes := d.mmap[minTimePosition : minTimePosition+timeSize]
	return int64(btou64(timeBytes))
}

func (d *dataFile) MaxTime() int64 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.mmap) == 0 {
		return 0
	}

	maxTimePosition := d.size - maxTimeOffset
	timeBytes := d.mmap[maxTimePosition : maxTimePosition+timeSize]
	return int64(btou64(timeBytes))
}

func (d *dataFile) SeriesCount() uint32 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.mmap) == 0 {
		return 0
	}

	return btou32(d.mmap[d.size-seriesCountSize:])
}

func (d *dataFile) IDToPosition() map[uint64]uint32 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.mmap) == 0 {
		return nil
	}

	count := int(d.SeriesCount())
	m := make(map[uint64]uint32)

	indexStart := d.size - uint32(count*12+20)
	for i := 0; i < count; i++ {
		offset := indexStart + uint32(i*12)
		id := btou64(d.mmap[offset : offset+8])
		pos := btou32(d.mmap[offset+8 : offset+12])
		m[id] = pos
	}

	return m
}

func (d *dataFile) indexPosition() uint32 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.mmap) == 0 {
		return 0
	}

	return d.size - uint32(d.SeriesCount()*12+20)
}

// StartingPositionForID returns the position in the file of the
// first block for the given ID. If zero is returned the ID doesn't
// have any data in this file.
func (d *dataFile) StartingPositionForID(id uint64) uint32 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.mmap) == 0 {
		return 0
	}

	seriesCount := d.SeriesCount()
	indexStart := d.indexPosition()

	min := uint32(0)
	max := uint32(seriesCount)

	for min < max {
		mid := (max-min)/2 + min

		offset := mid*seriesHeaderSize + indexStart
		checkID := btou64(d.mmap[offset : offset+8])

		if checkID == id {
			return btou32(d.mmap[offset+8 : offset+12])
		} else if checkID < id {
			min = mid + 1
		} else {
			max = mid
		}
	}

	return uint32(0)
}

func (d *dataFile) block(pos uint32) (id uint64, t int64, block []byte) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.mmap) == 0 {
		return 0, 0, nil
	}

	defer func() {
		if r := recover(); r != nil {
			panic(fmt.Sprintf("panic decoding file: %s at position %d for id %d at time %d", d.f.Name(), pos, id, t))
		}
	}()
	if pos < d.indexPosition() {
		id = d.idForPosition(pos)
		length := btou32(d.mmap[pos+8 : pos+12])
		block = d.mmap[pos+blockHeaderSize : pos+blockHeaderSize+length]
		t = int64(btou64(d.mmap[pos+blockHeaderSize : pos+blockHeaderSize+8]))
	}
	return
}

// idForPosition assumes the position is the start of an ID and will return the converted bytes as a uint64 ID
func (d *dataFile) idForPosition(pos uint32) uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.mmap) == 0 {
		return 0
	}

	return btou64(d.mmap[pos : pos+seriesIDSize])
}

type dataFiles []*dataFile

func (a dataFiles) Len() int           { return len(a) }
func (a dataFiles) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a dataFiles) Less(i, j int) bool { return a[i].MinTime() < a[j].MinTime() }

func dataFilesEquals(a, b []*dataFile) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v.MinTime() != b[i].MinTime() && v.MaxTime() != b[i].MaxTime() {
			return false
		}
	}
	return true
}

// compactionCheckpoint holds the new files and compacted files from a compaction
type compactionCheckpoint struct {
	CompactedFiles []string
	NewFiles       []string
}

// cleanup will remove all the checkpoint files and old compacted files from a compaction
// that finsihed, but didn't get to cleanup yet
func (c *compactionCheckpoint) cleanup() {
	for _, fn := range c.CompactedFiles {
		cn := checkpointFileName(fn)
		if err := os.RemoveAll(cn); err != nil {
			panic(fmt.Sprintf("error removing checkpoint file: %s", err.Error()))
		}
		if err := os.RemoveAll(fn); err != nil {
			panic(fmt.Sprintf("error removing old data file: %s", err.Error()))
		}
	}

	for _, fn := range c.NewFiles {
		cn := checkpointFileName(fn)
		if err := os.RemoveAll(cn); err != nil {
			panic(fmt.Sprintf("error removing checkpoint file: %s", err.Error()))
		}
	}
}

// footerSize will return what the size of the index and footer of a data file
// will be given the passed in series count
func footerSize(seriesCount int) uint32 {
	timeSizes := 2 * timeSize
	return uint32(seriesCount*(seriesIDSize+seriesPositionSize) + timeSizes + seriesCountSize)
}

// writeValues will encode the values and write them as a compressed block to the file
func writeValues(f *os.File, id uint64, values Values, buf []byte) uint32 {
	b, err := values.Encode(buf)
	if err != nil {
		panic(fmt.Sprintf("failure encoding block: %s", err.Error()))
	}

	if err := writeBlock(f, id, b); err != nil {
		// fail hard. If we can't write a file someone needs to get woken up
		panic(fmt.Sprintf("failure writing block: %s", err.Error()))
	}

	return uint32(blockHeaderSize + len(b))
}

// writeBlock will write a compressed block including its header
func writeBlock(f *os.File, id uint64, block []byte) error {
	if _, err := f.Write(append(u64tob(id), u32tob(uint32(len(block)))...)); err != nil {
		return err
	}
	_, err := f.Write(block)
	return err
}

// writeIndex will write out the index block and the footer of the file. After this call it should
// be a read only file that can be mmap'd as a dataFile
func writeIndex(f *os.File, minTime, maxTime int64, ids []uint64, newPositions []uint32) error {
	// write the file index, starting with the series ids and their positions
	for i, id := range ids {
		if _, err := f.Write(u64tob(id)); err != nil {
			return err
		}
		if _, err := f.Write(u32tob(newPositions[i])); err != nil {
			return err
		}
	}

	// write the min time, max time
	if _, err := f.Write(append(u64tob(uint64(minTime)), u64tob(uint64(maxTime))...)); err != nil {
		return err
	}

	// series count
	if _, err := f.Write(u32tob(uint32(len(ids)))); err != nil {
		return err
	}

	// sync it
	return f.Sync()
}

// openFileAndCehckpoint will create a checkpoint file, open a new file for
// writing a data index, write the header and return the file
func openFileAndCheckpoint(fileName string) (*os.File, error) {
	checkpointFile := checkpointFileName(fileName)
	cf, err := os.OpenFile(checkpointFile, os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	if err := cf.Close(); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	// write the header, which is just the magic number
	if _, err := f.Write(u32tob(MagicNumber)); err != nil {
		f.Close()
		return nil, err
	}

	return f, nil
}

// checkpointFileName will return the checkpoint name for the data files
func checkpointFileName(fileName string) string {
	return fmt.Sprintf("%s.%s", fileName, CheckpointExtension)
}

// removeCheckpoint removes the checkpoint for a new data file that was getting written
func removeCheckpoint(fileName string) error {
	checkpointFile := fmt.Sprintf("%s.%s", fileName, CheckpointExtension)
	return os.RemoveAll(checkpointFile)
}

// u64tob converts a uint64 into an 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func btou64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func u32tob(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func btou32(b []byte) uint32 {
	return uint32(binary.BigEndian.Uint32(b))
}

// hashSeriesField will take the fnv-1a hash of the key. It returns the value
// or 1 if the hash is either 0 or the max uint64. It does this to keep sentinel
// values available.
func hashSeriesField(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	n := h.Sum64()
	if n == uint64(0) || n == uint64(math.MaxUint64) {
		return 1
	}
	return n
}

// SeriesFieldKey combine a series key and field name for a unique string to be hashed to a numeric ID
func SeriesFieldKey(seriesKey, field string) string {
	return seriesKey + keyFieldSeparator + field
}

func seriesAndFieldFromCompositeKey(key string) (string, string) {
	parts := strings.Split(key, keyFieldSeparator)
	if len(parts) != 0 {
		return parts[0], strings.Join(parts[1:], keyFieldSeparator)
	}
	return parts[0], parts[1]
}

type uint64slice []uint64

func (a uint64slice) Len() int           { return len(a) }
func (a uint64slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint64slice) Less(i, j int) bool { return a[i] < a[j] }
