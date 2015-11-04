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

	// CheckpointExtension is the extension given to files that checkpoint a rewrite or compaction.
	// The checkpoint files are created when a new file is first created. They
	// are removed after the file has been synced and is safe for use. If a file
	// has an associated checkpoint file, it wasn't safely written and both should be removed
	CheckpointExtension = "check"

	// CompletionCheckpointExtension is the extension given to the file that marks when a compaction
	// or rewrite has been fully written, but the replaced files have not yet been deleted. It is used for cleanup
	// if the server was not cleanly shutdown before the old files could be deleted and the checkpoints removed.
	CompletionCheckpointExtension = "complete"

	// keyFieldSeparator separates the series key from the field name in the composite key
	// that identifies a specific field in series
	keyFieldSeparator = "#!~#"

	// blockBufferSize is the size for the compression buffer for encoding blocks
	blockBufferSize = 1024 * 1024
)

var (
	// zeroLenKey is a key that has a length of zero
	zeroLenKey = []byte{0x00, 0x00}
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

	// magicNumber is written as the first 4 bytes of a data file to
	// identify the file as a tsm1 formatted file
	magicNumber uint32 = 0x16D116D1
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
	// compacted and flushed
	deletes map[string]bool

	// deleteMeasurements is a map of the measurements that are deleted
	// but haven't yet been compacted and flushed
	deleteMeasurements map[string]bool

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

	e.cleanupCompletedRewritesAndCompactions()

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
		df := NewDataFile(f)
		e.files = append(e.files, df)
	}
	sort.Sort(e.files)

	e.deletes = make(map[string]bool)
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

// cleanupCompletedRewritesAndCompactions will read any completion markers. If the marker exists, the
// rewrite or compaction finished successfully, but didn't get fully cleaned up. Remove the old files and their checkpoints
func (e *Engine) cleanupCompletedRewritesAndCompactions() {
	files, err := filepath.Glob(filepath.Join(e.path, fmt.Sprintf("*.%s", CompletionCheckpointExtension)))
	if err != nil {
		panic(fmt.Sprintf("error getting completion checkpoints: %s", err.Error()))
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

		c := &completionCheckpoint{}
		err = json.Unmarshal(data, c)

		// if no error then cleanup, otherwise there was an error it didn't finish writing so just remove it
		if err == nil {
			c.cleanup()
		}

		if err := f.Close(); err != nil {
			panic(fmt.Sprintf("error closing completion checkpoint: %s", err.Error()))
		}
		if err := os.RemoveAll(fn); err != nil {
			panic(fmt.Sprintf("error removing completion checkpoint: %s", err.Error()))
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
	if len(points) == 0 {
		return fmt.Errorf("write must have points")
	}
	return e.WAL.WritePoints(points, measurementFieldsToSave, seriesToCreate)
}

// filesAndNewValues will pair the values with the data file they should be re-written into. Any
// vaues that have times later than the most recent data file are returned
func (e *Engine) filesAndNewValues(files dataFiles, pointsByKey map[string]Values) (rewrites []*fileRewrite, newValues map[uint64][]*valuesWithKey) {
	// first convert the data into data by ID
	pointsByID := make(map[uint64][]*valuesWithKey)
	for k, vals := range pointsByKey {
		id := e.HashSeriesField(k)
		pointsByID[id] = append(pointsByID[id], &valuesWithKey{key: k, values: vals})
	}

	// ensure that the keys are in order before writing
	e.sortPointsByKey(pointsByID)

	// if there aren't any existing files, everything is new so write it
	if len(files) == 0 {
		return nil, pointsByID
	}

	// loop backwards through the files and split the data out
	left := pointsByID
	for i := len(files); i >= 0; i-- {
		if len(left) == 0 {
			return
		}

		if i == len(files) {
			// these are the values that go into a new file
			left, newValues = e.splitValuesByTime(files[i-1].MaxTime()-1, left)
		} else if i == 0 {
			// anything left is on the lower end of the time range and should go into the first file
			rewrites = append(rewrites, &fileRewrite{df: files[0], valuesByID: left})
			return
		} else {
			l, r := e.splitValuesByTime(files[i-1].MaxTime()-1, left)
			rewrites = append(rewrites, &fileRewrite{df: files[i], valuesByID: r})
			left = l
		}
	}
	return
}

// splitValuesByTime will divide the values in the map by the split time. The left result will be times <= the split time while the right side
// will be times > the split time
func (e *Engine) splitValuesByTime(splitTime int64, pointsByID map[uint64][]*valuesWithKey) (left map[uint64][]*valuesWithKey, right map[uint64][]*valuesWithKey) {
	left = make(map[uint64][]*valuesWithKey)
	right = make(map[uint64][]*valuesWithKey)

	for id, vals := range pointsByID {
		var leftVals []*valuesWithKey
		var rightVals []*valuesWithKey

		for _, v := range vals {
			// do this check first since most of the time data will be append only
			if v.values[0].UnixNano() > splitTime {
				rightVals = append(rightVals, v)
				continue
			}

			i := sort.Search(len(v.values), func(i int) bool {
				return v.values[i].UnixNano() > splitTime
			})

			if i != 0 {
				leftVals = append(leftVals, &valuesWithKey{key: v.key, values: v.values[:i]})
			}
			if i != len(v.values) {
				rightVals = append(rightVals, &valuesWithKey{key: v.key, values: v.values[i:]})
			}
		}

		if len(leftVals) > 0 {
			left[id] = leftVals
		}
		if len(rightVals) > 0 {
			right[id] = rightVals
		}
	}

	return
}

// sortPointsByKey will ensure that the []*valuesWithKey are sorted by their key
func (e *Engine) sortPointsByKey(pointsByID map[uint64][]*valuesWithKey) {
	for _, vals := range pointsByID {
		if len(vals) > 1 {
			sort.Sort(valuesWithKeySlice(vals))
		}
	}
}

func (e *Engine) hasDeletes() bool {
	e.filesLock.RLock()
	defer e.filesLock.RUnlock()
	return len(e.deletes) > 0
}

func (e *Engine) Write(pointsByKey map[string]Values, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
	// Flush any deletes before writing new data from the WAL
	if e.hasDeletes() {
		e.flushDeletes()
	}

	startTime := int64(math.MaxInt64)
	endTime := int64(math.MinInt64)

	for _, v := range pointsByKey {
		if min := v.MinTime(); min < startTime {
			startTime = min
		}
		if max := v.MaxTime(); max > endTime {
			endTime = max
		}
	}

	e.writeMetadata(measurementFieldsToSave, seriesToCreate)

	if len(pointsByKey) == 0 {
		return nil
	}

	files, lockStart, lockEnd := e.filesAndLock(startTime, endTime)
	defer e.writeLock.UnlockRange(lockStart, lockEnd)
	defer func(files []*dataFile) {
		for _, f := range files {
			f.Unreference()
		}
	}(files)

	// break out the values by the file that needs to be replaced with them inserted
	fileRewrites, valuesInNewFile := e.filesAndNewValues(files, pointsByKey)

	// do the rewrites in parallel
	newFiles := e.performRewrites(fileRewrites, valuesInNewFile)

	// update the engine to point at the new dataFiles
	e.filesLock.Lock()
	for _, df := range e.files {
		removeFile := false

		for _, fr := range fileRewrites {
			if df == fr.df {
				removeFile = true
				break
			}
		}

		if !removeFile {
			newFiles = append(newFiles, df)
		}
	}

	sort.Sort(dataFiles(newFiles))
	e.files = newFiles
	e.filesLock.Unlock()

	// write the checkpoint file
	oldNames := make([]string, 0, len(fileRewrites))
	for _, fr := range fileRewrites {
		oldNames = append(oldNames, fr.df.f.Name())
	}
	newNames := make([]string, 0, len(newFiles))
	for _, df := range newFiles {
		newNames = append(newNames, df.f.Name())
	}
	checkpoint, checkpointFileName := e.writeCompletionCheckpointFile(oldNames, newNames)

	// remove the old data file. no need to block returning the write,
	// but we need to let any running queries finish before deleting it
	oldFiles := make([]*dataFile, 0, len(fileRewrites))
	for _, f := range fileRewrites {
		oldFiles = append(oldFiles, f.df)
	}
	e.cleanupAndDeleteCheckpoint(checkpointFileName, checkpoint, oldFiles)

	if !e.SkipCompaction && e.shouldCompact() {
		go func() {
			if err := e.Compact(false); err != nil {
				e.logger.Printf("Write: error during compaction: %v", err)
			}
		}()
	}

	return nil
}

// cleanupAndDeleteCheckpoint will remove the checkpoint files for new files saved, delete the data files safely so
// running queries complete first, and finally remove the completion checkpoint file
func (e *Engine) cleanupAndDeleteCheckpoint(checkpointFileName string, checkpoint *completionCheckpoint, files []*dataFile) {
	e.deletesPending.Add(1)

	// do this in a goroutine so deletes don't hold up the write completing
	go func() {
		// first delete the checkpoint files
		for _, fn := range checkpoint.NewFiles {
			removeCheckpoint(fn)
		}

		// now delete the data files
		for _, df := range files {
			if err := df.Delete(); err != nil {
				panic(fmt.Sprintf("error deleting old data file: %s", err.Error()))
			}
		}

		// finally remove the completion checkpoint
		if err := os.RemoveAll(checkpointFileName); err != nil && !os.IsNotExist(err) {
			log.Printf("error removing completion checkpoint file: %s", checkpointFileName)
		}

		e.deletesPending.Done()
	}()
}

// performRewrites will rewrite the given files with the new values and write a new file with values for those not
// associated with an existing block of time. Writes will be done in parallel
func (e *Engine) performRewrites(fileRewrites []*fileRewrite, valuesInNewFile map[uint64][]*valuesWithKey) []*dataFile {
	// do the rewrites in parallel
	var wg sync.WaitGroup
	var mu sync.Mutex
	var newFiles []*dataFile
	for _, rewrite := range fileRewrites {
		wg.Add(1)
		func(rewrite *fileRewrite) {
			files := e.rewriteFile(rewrite.df, rewrite.valuesByID)
			mu.Lock()
			newFiles = append(newFiles, files...)
			mu.Unlock()
			wg.Done()
		}(rewrite)
	}

	// create a new file for values that are newer than anything we currently have
	if len(valuesInNewFile) > 0 {
		wg.Add(1)
		func() {
			f := e.writeNewFile(valuesInNewFile)
			mu.Lock()
			newFiles = append(newFiles, f)
			mu.Unlock()
			wg.Done()
		}()
	}

	wg.Wait()

	return newFiles
}

// fileRewrite has a data file and new values to be written into a new file
type fileRewrite struct {
	df         *dataFile
	valuesByID map[uint64][]*valuesWithKey
}

// MarkDeletes will mark the given keys for deletion in memory. They will be deleted from data
// files on the next flush. This mainly for the WAL to use on startup
func (e *Engine) MarkDeletes(keys []string) {
	e.filesLock.Lock()
	defer e.filesLock.Unlock()
	for _, k := range keys {
		e.deletes[k] = true
	}
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
		for _, f := range a {
			f.Reference()
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

		for _, f := range a {
			f.Unreference()
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

	// run the compaction
	cj := newCompactionJob(e, files)
	newFiles := cj.compact()

	newDataFiles := make(dataFiles, len(newFiles))
	for i, f := range newFiles {
		newDataFiles[i] = NewDataFile(f)
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

	checkpoint, compactionCheckpointName := e.writeCompletionCheckpointFile(compactedFileNames, newFileNames)

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

	e.cleanupAndDeleteCheckpoint(compactionCheckpointName, checkpoint, files)

	return nil
}

// writeCompletionCheckpointFile will save the old filenames and new filenames in a completion
// checkpoint file. This is used on startup to clean out files that weren't deleted if the server
// wasn't shut down cleanly.
func (e *Engine) writeCompletionCheckpointFile(oldFileNames, newFileNames []string) (*completionCheckpoint, string) {
	checkpoint := &completionCheckpoint{OldFiles: oldFileNames, NewFiles: newFileNames}

	data, err := json.Marshal(checkpoint)
	if err != nil {
		panic(fmt.Sprintf("error creating rewrite checkpoint: %s", err.Error()))
	}

	// make the checkpoint filename the same name as the first new file, but with the completion checkpoint extension
	name := strings.Split(filepath.Base(newFileNames[0]), ".")[0]
	fn := fmt.Sprintf("%s.%s", name, CompletionCheckpointExtension)
	fileName := filepath.Join(filepath.Dir(newFileNames[0]), fn)

	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		panic(fmt.Sprintf("error opening file: %s", err.Error()))
	}

	mustWrite(f, data)

	if err := f.Close(); err != nil {
		panic(fmt.Sprintf("error closing file: %s", err.Error()))
	}

	return checkpoint, fileName
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

func (e *Engine) writeMetadata(measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) {
	e.metaLock.Lock()
	defer e.metaLock.Unlock()

	if err := e.writeNewFields(measurementFieldsToSave); err != nil {
		panic(fmt.Sprintf("error saving fields: %s", err.Error()))
	}

	if err := e.writeNewSeries(seriesToCreate); err != nil {
		panic(fmt.Sprintf("error saving series: %s", err.Error()))
	}
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

type indexData struct {
	ids             []uint64
	positions       []uint32
	minTimes        []int64
	maxTimes        []int64
	fileMinTime     int64
	fileMaxTime     int64
	idsInIndex      map[uint64]struct{}
	currentPosition int
}

func newIndexData() *indexData {
	return &indexData{
		fileMinTime:     math.MaxInt64,
		fileMaxTime:     math.MinInt64,
		idsInIndex:      make(map[uint64]struct{}),
		currentPosition: -1,
	}
}

// addIDPosition will add the id and the position to the index
// if it hasn't been added yet and advance the current position
func (i *indexData) addIDPosition(id uint64, pos uint32) {
	if _, ok := i.idsInIndex[id]; !ok {
		i.ids = append(i.ids, id)
		i.idsInIndex[id] = struct{}{}
		i.positions = append(i.positions, pos)
		i.minTimes = append(i.minTimes, int64(math.MaxInt64))
		i.maxTimes = append(i.maxTimes, int64(math.MinInt64))
		i.currentPosition++
	}
}

// setMinTime will set the current position's min time if the passed time is less
func (i *indexData) setMinTime(t int64) {
	if t < i.minTimes[i.currentPosition] {
		i.minTimes[i.currentPosition] = t

		if t < i.fileMinTime {
			i.fileMinTime = t
		}
	}
}

// setMaxTime will set the current position's max time if the passed time is greater
func (i *indexData) setMaxTime(t int64) {
	if t > i.maxTimes[i.currentPosition] {
		i.maxTimes[i.currentPosition] = t

		if t > i.fileMaxTime {
			i.fileMaxTime = t
		}
	}
}

type rewriteOperation struct {
	// df is the data file we're rewriting with new values
	df    *dataFile
	dfEOF uint32

	// tracking information for the data coming from the file
	currentKey string
	currentID  uint64
	nextPos    uint32

	// f is the new file we're writing
	f    *os.File
	fPos uint32

	// index is for the new file
	index *indexData

	// newFiles are the 1 or more files that the df and new values are written to
	newFiles []*dataFile

	engine *Engine

	// buf for compressing blocks into the file
	buf []byte

	// valsBuf is a buffer for decoding blocks into values
	valsBuf []Value

	// valsWriteBuf is a buffer for writes
	valsWriteBuf []Value
}

func newRewriteOperation(engine *Engine, df *dataFile) *rewriteOperation {
	r := &rewriteOperation{
		df:      df,
		dfEOF:   df.indexPosition(),
		nextPos: fileHeaderSize,

		f:    openFileAndCheckpoint(engine.nextFileName()),
		fPos: fileHeaderSize,

		index:  newIndexData(),
		engine: engine,
		buf:    make([]byte, blockBufferSize),
	}

	r.currentKey, _, _ = df.block(fileHeaderSize)
	r.currentID = engine.HashSeriesField(r.currentKey)

	return r
}

// sourceFileEOF returns true if we've reached the end of the file getting rewritten
func (r *rewriteOperation) sourceFileEOF() bool {
	return r.nextPos >= r.dfEOF
}

// copyBlocksForCurrentID will copy the file blocks from the souce to the destination, mark the index,
// advance the pointers and rotate the new file if over the max size
func (r *rewriteOperation) copyBlocksForCurrentID() {
	currentID := r.currentID
	for {
		r.writeToNextKey()

		// if the ID is different, we're done with writing data from the file for this ID
		if currentID != r.currentID {
			return
		}
	}
}

// rotateIfOverMaxSize will rottate to a new file if the current new file we're writing into
// is over the maximum allowable size and return true if rotated
func (r *rewriteOperation) rotateIfOverMaxSize() bool {
	if r.fPos+footerSize(len(r.index.idsInIndex)) > r.engine.MaxFileSize {
		r.finishNewFile()
		r.f = openFileAndCheckpoint(r.engine.nextFileName())
		r.fPos = fileHeaderSize

		return true
	}

	return false
}

// finishNewFile will write the indx and add this file to the new files for this operation
func (r *rewriteOperation) finishNewFile() {
	writeIndex(r.f, r.index)
	r.newFiles = append(r.newFiles, NewDataFile(r.f))
	r.index = newIndexData()
}

// copyRemainingBlocksFromFile will iterate through the source file and copy all blocks to the new file
// while rotating if the file goes over the max size and keeping track of the new index information
func (r *rewriteOperation) copyRemaingBlocksFromFile() {
	for !r.sourceFileEOF() {
		r.copyBlocksForCurrentID()
	}
}

// writeNewValues will write the new values to the new file while advancing pointers and keeping the
// index information and ensuring the file gets rotated if over the max size
func (r *rewriteOperation) writeNewValues(id uint64, vals []*valuesWithKey) {
	for _, v := range vals {
		r.writeValues(true, id, v)
	}
}

// writeValues writes the passed in values to the new file as compressed blocks with the block header
// data, ensuring that the new file will be switched out if it goes over the max size
func (r *rewriteOperation) writeValues(firstInsert bool, id uint64, vals *valuesWithKey) {
	for len(vals.values) > 0 {
		// rotate if over the max file limit
		rotated := r.rotateIfOverMaxSize()
		if rotated {
			firstInsert = true
		}

		// store the starting position if this is the first block for this ID
		r.index.addIDPosition(id, r.fPos)

		// set the times in the index
		r.index.setMinTime(vals.values[0].UnixNano())
		r.index.setMaxTime(vals.values[len(vals.values)-1].UnixNano())

		// ensure we write blocks with the maximum number of values
		valuesToWrite := vals.values
		if len(vals.values) > r.engine.MaxPointsPerBlock {
			valuesToWrite = vals.values[:r.engine.MaxPointsPerBlock]
			vals.values = vals.values[r.engine.MaxPointsPerBlock:]
		} else {
			vals.values = vals.values[:0]
		}
		bytesWritten := writeValues(r.f, id, vals.key, valuesToWrite, firstInsert, r.buf)
		r.fPos += bytesWritten

		// set firstinsert to true if we start a new file
		firstInsert = rotated
	}
}

// merge will combine blocks matching the current key with the passed in values into the new file
func (r *rewriteOperation) merge(vals *valuesWithKey) {
	id := r.currentID
	firstWriteToFile := true
	r.valsWriteBuf = r.valsWriteBuf[:0]
	for {
		if r.sourceFileEOF() {
			r.currentID = math.MaxUint64
			r.currentKey = ""

			// add in anything that was sitting around from previous iteration
			r.mergeValuesBuffer(vals)
			r.writeValues(firstWriteToFile, id, vals)
			return
		}

		// rotate to a new file before writing new data if we're over the limit
		rotated := r.rotateIfOverMaxSize()
		if rotated {
			firstWriteToFile = true
		}

		// decompress the block from the source file
		key, block, nextPos := r.df.block(r.nextPos)
		if key != "" {
			r.currentKey = key
			r.currentID = r.engine.HashSeriesField(key)
		}

		// if we've already written all the blocks from the file for this id,
		// write out the remaining values and return
		if r.currentID != id {
			// add in anything that was sitting around from previous iteration
			r.mergeValuesBuffer(vals)
			r.writeValues(firstWriteToFile, id, vals)
			return
		}

		r.nextPos = nextPos
		r.valsBuf = r.valsBuf[:0]
		if err := DecodeBlock(block, &r.valsBuf); err != nil {
			panic(fmt.Sprintf("error decoding block on rewrite: %s", err.Error()))
		}

		// combine all new values less than the max time of the block and leave the remainder in vals.values
		maxTime := r.valsBuf[len(r.valsBuf)-1].UnixNano()
		i := sort.Search(len(vals.values), func(i int) bool { return vals.values[i].UnixNano() > maxTime })

		if i > 0 {
			r.valsBuf = append(r.valsBuf, vals.values[:i]...)
			vals.values = vals.values[i:]
			r.valsBuf = Values(r.valsBuf).Deduplicate()
		}

		r.valsWriteBuf = append(r.valsWriteBuf, r.valsBuf...)

		// write the values out if we've hit the limit
		if len(r.valsWriteBuf) > r.engine.MaxPointsPerBlock {
			r.writeValues(firstWriteToFile, id, &valuesWithKey{key: vals.key, values: r.valsWriteBuf})
			r.valsWriteBuf = r.valsWriteBuf[:0]
			firstWriteToFile = false
		}
	}
}

func (r *rewriteOperation) mergeValuesBuffer(vals *valuesWithKey) {
	if len(r.valsWriteBuf) == 0 {
		return
	}

	if len(vals.values) == 0 {
		vals.values = r.valsWriteBuf
		return
	}

	needSort := r.valsWriteBuf[0].UnixNano() > vals.values[0].UnixNano()
	vals.values = append(r.valsWriteBuf, vals.values...)
	if needSort {
		vals.values = Values(vals.values).Deduplicate()
	}
}

// mergeValues will take the values and read from the source file combining the two together into the
// new file while tracking index information and rotating to a new file if the current one gets over max size
func (r *rewriteOperation) mergeValues(vals []*valuesWithKey) {
	currentID := r.currentID
	for {
		// calls in the loop can change the currentID, make sure they're still the same
		if currentID != r.currentID {
			return
		}

		// if we've written all new values, copy anything left for this ID from the file
		if len(vals) == 0 {
			// write the rest out from the file
			r.copyBlocksForCurrentID()

			return
		}

		// just empty out the values if the source file is all written out
		if r.sourceFileEOF() {
			r.writeValues(true, r.currentID, vals[0])
			vals = vals[1:]
			continue
		}

		// we have data for this id in the source file and in the values, merge them
		cmp := strings.Compare(r.currentKey, vals[0].key)

		switch cmp {
		case 0: // if they're the same, merge them together
			r.merge(vals[0])
			vals = vals[1:]
		case 1: // if the vals are less, write them out
			r.writeValues(true, r.currentID, vals[0])
			vals = vals[1:]
		case -1: // if the key in the file is less, write out the blocks for this key
			r.writeToNextKey()
		}
	}
}

// writeToNextKey will write from the source file to the new file all data for the
// given ID and key until either the next ID or key is hit
func (r *rewriteOperation) writeToNextKey() {
	for {
		if r.sourceFileEOF() {
			r.currentID = math.MaxUint64
			r.currentKey = ""
			return
		}

		key, block, nextPos := r.df.block(r.nextPos)

		// if the key is blank, we have another block for the same ID and key
		if key != "" {
			// make sure we're on the right key
			if key != r.currentKey {
				r.currentKey = key
				r.currentID = r.engine.HashSeriesField(r.currentKey)
				return
			}
		}

		// rotate to a new file before writing new data if we're over the limit
		rotated := r.rotateIfOverMaxSize()

		bytesWritten := uint32(0)

		// if we've rotated to a new file, we need to write the block with the key
		if rotated {
			// write the key out
			bytesWritten += writeKey(r.f, r.currentKey)

			// now write the compressed block unmodified
			mustWrite(r.f, block)
			bytesWritten += uint32(len(block))
		} else {
			// write all the block header info and the block unmodified
			mustWrite(r.f, r.df.mmap[r.nextPos:nextPos])

			// mark how much we've written to the new file and the new position on the source file
			bytesWritten += (nextPos - r.nextPos)
		}

		// store the starting position if this is the first block for this ID
		r.index.addIDPosition(r.currentID, r.fPos)
		r.index.setMinTime(r.df.compressedBlockMinTime(block))

		// save the position in the new file and the source file position
		r.fPos += bytesWritten
		r.nextPos = nextPos

		// TODO: update this so we don't need to decode the entire block to get the max time
		r.valsBuf = r.valsBuf[:0]
		if err := DecodeBlock(block, &r.valsBuf); err != nil {
			panic(fmt.Sprintf("error decoding block on rewrite: %s", err.Error()))
		}
		r.index.setMaxTime(r.valsBuf[len(r.valsBuf)-1].UnixNano())
	}
}

// rewriteFile will write a new file with the data from the source file merged with the new data
// passed in
func (e *Engine) rewriteFile(df *dataFile, valuesByID map[uint64][]*valuesWithKey) []*dataFile {
	if len(valuesByID) == 0 {
		return []*dataFile{df}
	}

	// insert the data by ID ascending
	insertIDs := sortedIDs(valuesByID)
	insertIDPos := 0

	rewrite := newRewriteOperation(e, df)

	for {
		// if there are no more new values to write, we just need to carry over the rest from the file
		if insertIDPos >= len(insertIDs) {
			// check if we're done with everything
			if rewrite.sourceFileEOF() {
				break
			}

			rewrite.copyRemaingBlocksFromFile()
			break
		}

		nextInsertID := insertIDs[insertIDPos]

		// write just the source file data if its ID is not in the new data and is next
		if nextInsertID > rewrite.currentID {
			rewrite.copyBlocksForCurrentID()
			continue
		}

		// we're going to read this ID's data, so advance the cursor
		insertIDPos++

		// write just the new data if this ID is not in the source file and is next
		if nextInsertID < rewrite.currentID {
			rewrite.writeNewValues(nextInsertID, valuesByID[nextInsertID])

			continue
		}

		// combine the new data and the old file data
		rewrite.mergeValues(valuesByID[nextInsertID])
	}

	rewrite.finishNewFile()

	return rewrite.newFiles
}

func (e *Engine) writeNewFile(valuesByID map[uint64][]*valuesWithKey) *dataFile {
	index := newIndexData()

	// data should always be written in ascending id, then ascending time for each id
	index.ids = sortedIDs(valuesByID)
	index.positions = make([]uint32, len(index.ids))
	index.minTimes = make([]int64, len(index.ids))
	index.maxTimes = make([]int64, len(index.ids))

	f := openFileAndCheckpoint(e.nextFileName())

	buf := make([]byte, blockBufferSize)

	// now loop through the ids writing out the data
	position := uint32(fileHeaderSize)
	for i, id := range index.ids {
		index.positions[i] = position
		valuesWithKeys := valuesByID[id]
		minTime := int64(math.MaxInt64)
		maxTime := int64(math.MinInt64)

		// because of potential hash collisions, multiple keys could have the same ID
		for _, v := range valuesWithKeys {
			// track if it's the first write to this file for this key
			firstWrite := true
			for len(v.values) > 0 {
				valsToWrite := v.values
				if len(v.values) > e.MaxPointsPerBlock {
					valsToWrite = v.values[:e.MaxPointsPerBlock]
					v.values = v.values[e.MaxPointsPerBlock:]
				} else {
					v.values = v.values[:0]
				}

				// keep track of min and max times for this ID
				mint := valsToWrite[0].UnixNano()
				maxt := valsToWrite[len(valsToWrite)-1].UnixNano()

				if mint < minTime {
					minTime = mint
				}
				if maxt > maxTime {
					maxTime = maxt
				}

				bytesWritten := writeValues(f, id, v.key, valsToWrite, firstWrite, buf)
				position += bytesWritten
				firstWrite = false
			}
		}
		index.minTimes[i] = minTime
		index.maxTimes[i] = maxTime

		if minTime < index.fileMinTime {
			index.fileMinTime = minTime
		}
		if maxTime > index.fileMaxTime {
			index.fileMaxTime = maxTime
		}
	}

	// write the index and we're done
	writeIndex(f, index)

	return NewDataFile(f)
}

// flushDeletes will lock the entire shard and rewrite all index files so they no
// longer contain the flushed IDs
func (e *Engine) flushDeletes() error {
	e.writeLock.LockRange(math.MinInt64, math.MaxInt64)
	defer e.writeLock.UnlockRange(math.MinInt64, math.MaxInt64)
	e.metaLock.Lock()
	defer e.metaLock.Unlock()

	measurements := make(map[string]bool)
	deletes := make(map[string]bool)
	e.filesLock.RLock()
	for name := range e.deleteMeasurements {
		measurements[name] = true
	}
	for key, _ := range e.deletes {
		deletes[key] = true
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
	for key, _ := range deletes {
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
	for key := range deletes {
		delete(e.deletes, key)
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
	f := openFileAndCheckpoint(e.nextFileName())

	index := newIndexData()

	fileIndexTimes := oldDF.indexMinMaxTimes()

	indexPosition := oldDF.indexPosition()
	currentPosition := uint32(fileHeaderSize)
	deleteCurrentKey := false
	currentID := uint64(0)
	for currentPosition < indexPosition {
		key, _, end := oldDF.block(currentPosition)

		// if the key is empty, then it's the same as whatever the last one was
		if key == "" {
			if deleteCurrentKey {
				currentPosition = end
				continue
			}
		} else {
			// if it's in the delete map, don't copy it to the new file
			if _, ok := e.deletes[key]; ok {
				deleteCurrentKey = true
				currentPosition = end
				continue
			}

			// otherwise it's a new key that we want to keep, update the index and write
			deleteCurrentKey = false
			currentID = e.HashSeriesField(key)
			times := fileIndexTimes[currentID]
			index.addIDPosition(currentID, currentPosition)
			index.setMinTime(times.min)
			index.setMaxTime(times.max)
		}

		mustWrite(f, oldDF.mmap[currentPosition:end])
		currentPosition = end
	}

	writeIndex(f, index)

	if err := os.RemoveAll(checkpointFileName(f.Name())); err != nil {
		panic(fmt.Sprintf("failed to remove checkpoint file: %v: %v", f.Name(), err))
	}

	return NewDataFile(f)
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
		e.deletes[key] = true
	}

	return e.WAL.DeleteSeries(keyFields)
}

// DeleteMeasurement deletes a measurement and all related series.
func (e *Engine) DeleteMeasurement(name string, seriesKeys []string) error {
	e.metaLock.Lock()
	defer e.metaLock.Unlock()

	_, err := e.readFields()
	if err != nil {
		return err
	}

	// mark the measurement, series keys and the fields for deletion on the next flush
	// also serves as a tombstone for any queries that come in before the flush
	e.filesLock.Lock()
	defer e.filesLock.Unlock()

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
			f.Reference()
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
				f.Unreference()
			}
			continue
		}

		// we're good to go
		break
	}

	return &tx{files: files, engine: e}, nil
}

func (e *Engine) WriteTo(w io.Writer) (n int64, err error) { panic("not implemented") }

func (e *Engine) keyAndFieldToID(series, field string) uint64 {
	key := SeriesFieldKey(series, field)
	return e.HashSeriesField(key)
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
	engine   *Engine
	newFiles []*os.File

	// variables for tracking the files getting compacted
	dataFilesToCompact []*dataFile
	currentKeys        []string
	currentIDs         []uint64
	currentBlocks      [][]byte
	nextPositions      []uint32

	// variables for the new file we're compacting into
	currentFile     *os.File
	currentPosition uint32
	index           *indexData

	// buffer for encoding
	buf []byte

	// valsBuf is a buffer for decoding values
	valsBuf []Value

	// writeValsBuf is buffer for writing values
	writeValsBuf []Value
}

// dataFileOEF is a sentinel values marking that there is no more data to be read from the data file
const dataFileEOF = uint64(math.MaxUint64)

func newCompactionJob(engine *Engine, files dataFiles) *compactionJob {
	c := &compactionJob{
		engine:             engine,
		dataFilesToCompact: files,
		currentKeys:        make([]string, len(files)),
		currentIDs:         make([]uint64, len(files)),
		currentBlocks:      make([][]byte, len(files)),
		nextPositions:      make([]uint32, len(files)),

		buf: make([]byte, blockBufferSize),
	}

	// initialize the starting variables for the files getting compacted
	for i, df := range files {
		pos := uint32(fileHeaderSize)
		c.currentKeys[i], c.currentBlocks[i], c.nextPositions[i] = df.block(pos)
		c.currentIDs[i] = c.engine.HashSeriesField(c.currentKeys[i])
	}

	return c
}

func (c *compactionJob) compact() []*os.File {
	c.newCurrentFile()
	c.engine.logger.Printf("Starting compaction in %s of %d files to new file %s", c.engine.path, len(c.dataFilesToCompact), c.currentFile.Name())

	// loop writing data for each key in order until we've read through all the files
	for key, id := c.nextKey(); id != dataFileEOF; key, id = c.nextKey() {
		c.writeBlocksForKey(key, id)
	}

	if c.currentFile != nil {
		writeIndex(c.currentFile, c.index)
	}
	c.newFiles = append(c.newFiles, c.currentFile)
	c.currentFile = nil
	c.index = nil

	return c.newFiles
}

// newCurrentFile will create a new compaction file and reset the ids and positions
// in the file so we can write the index out later
func (c *compactionJob) newCurrentFile() {
	c.index = newIndexData()

	c.currentFile = openFileAndCheckpoint(c.engine.nextFileName())
	c.currentPosition = uint32(fileHeaderSize)
}

// writeBlocksForKey will read data for the given key from all the files getting compacted
// and write it into a new compacted file. Blocks from different files will be combined to
// create larger blocks in the compacted file. If the compacted file goes over the max
// file size limit, true will be returned indicating that its time to create a new compaction file
func (c *compactionJob) writeBlocksForKey(key string, id uint64) {
	// track if the current file being written into has had this key in it before. will be used in the loop below
	firstInsertToFile := true

	c.writeValsBuf = c.writeValsBuf[:0]

	// loop through the files in order emptying each one of its data for this key
	for i, df := range c.dataFilesToCompact {
		// if the id doesn't match, move to the next file
		fid := c.currentIDs[i]
		if fid != id {
			continue
		}

		// if the key doesn't match, move to the next one
		fkey := c.currentKeys[i]
		if fkey != key {
			continue
		}

		// this file has the key, empty it out

		// TODO: update this so that blocks with the max points don't get decoded
		// read the first block since that key will be set
		c.valsBuf = c.valsBuf[:0]
		if err := DecodeBlock(c.currentBlocks[i], &c.valsBuf); err != nil {
			panic(fmt.Sprintf("error decoding: %s", err.Error()))
		}
		c.writeValsBuf = append(c.writeValsBuf, c.valsBuf...)

		// read any future blocks for the same key (their key value will be the empty string)
		for {
			// write values if we're over the max size
			if len(c.writeValsBuf) > c.engine.MaxPointsPerBlock {
				if firstInsertToFile {
					// mark this ID as new and track its starting position
					c.index.addIDPosition(id, c.currentPosition)
				}

				valsToWrite := c.writeValsBuf[:c.engine.MaxPointsPerBlock]
				c.writeValsBuf = c.writeValsBuf[c.engine.MaxPointsPerBlock:]
				bytesWritten := writeValues(c.currentFile, id, key, valsToWrite, firstInsertToFile, c.buf)
				c.index.setMinTime(valsToWrite[0].UnixNano())
				c.index.setMaxTime(valsToWrite[len(valsToWrite)-1].UnixNano())
				c.currentPosition += bytesWritten

				// rotate the file if necessary
				firstInsertToFile = c.rotateIfOverMaxSize()
			}

			if c.nextPositions[i] >= df.indexPosition() {
				c.currentKeys[i] = ""
				c.currentIDs[i] = dataFileEOF
				break
			}

			key, block, next := df.block(c.nextPositions[i])
			c.nextPositions[i] = next

			// if it's a different key, save the values and break out
			if key != "" {
				c.currentKeys[i] = key
				c.currentIDs[i] = c.engine.HashSeriesField(key)
				c.currentBlocks[i] = block

				break
			}

			c.valsBuf = c.valsBuf[:0]
			if err := DecodeBlock(block, &c.valsBuf); err != nil {
				panic(fmt.Sprintf("error decoding: %s", err.Error()))
			}
			c.writeValsBuf = append(c.writeValsBuf, c.valsBuf...)
		}
	}

	// write out any remaining values
	for len(c.writeValsBuf) > 0 {
		valsToWrite := c.writeValsBuf
		if len(c.writeValsBuf) > c.engine.MaxPointsPerBlock {
			valsToWrite = c.writeValsBuf[:c.engine.MaxPointsPerBlock]
			c.writeValsBuf = c.writeValsBuf[c.engine.MaxPointsPerBlock:]
		} else {
			c.writeValsBuf = c.writeValsBuf[:0]
		}

		if firstInsertToFile {
			// mark this ID as new and track its starting position
			c.index.addIDPosition(id, c.currentPosition)
		}

		bytesWritten := writeValues(c.currentFile, id, key, valsToWrite, firstInsertToFile, c.buf)
		c.index.setMinTime(valsToWrite[0].UnixNano())
		c.index.setMaxTime(valsToWrite[len(valsToWrite)-1].UnixNano())
		c.currentPosition += bytesWritten

		// rotate the file if necessary
		firstInsertToFile = c.rotateIfOverMaxSize()
	}
}

// nextKey returns the key and ID with the lowest number ID and then lowest key if there are collisions
// in that ID. The key will be read from all of the data files getting compacted in order. It
// will return an empty string and dataFileEOF if all files have been read and compacted
func (c *compactionJob) nextKey() (string, uint64) {
	minID := dataFileEOF
	keys := make(map[string]bool)
	for i, id := range c.currentIDs {
		if id == dataFileEOF {
			continue
		}

		if minID > id {
			minID = id
			keys = map[string]bool{c.currentKeys[i]: true}
		} else if minID == id {
			keys[c.currentKeys[i]] = true
		}
	}

	// if the min is still EOF, we're done with all the data from all files to compact
	if minID == dataFileEOF || len(keys) == 0 {
		return "", dataFileEOF
	}

	sortedKeys := make([]string, 0, len(keys))
	for k, _ := range keys {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	return sortedKeys[0], minID
}

// rotateIfOverMaxSize will rottate to a new file if the current new file we're writing into
// is over the maximum allowable size and return true if rotated
func (c *compactionJob) rotateIfOverMaxSize() bool {
	if c.currentPosition+footerSize(len(c.index.idsInIndex)) > c.engine.MaxFileSize {
		writeIndex(c.currentFile, c.index)
		c.currentFile = openFileAndCheckpoint(c.engine.nextFileName())
		c.currentPosition = fileHeaderSize
		c.index = newIndexData()

		return true
	}

	return false
}

type dataFile struct {
	f       *os.File
	mu      sync.RWMutex
	size    uint32
	modTime time.Time
	mmap    []byte

	// refs tracks lives references to this dataFile
	refs sync.RWMutex
}

// byte size constants for the data file
const (
	fileHeaderSize  = 4
	seriesCountSize = 4
	timeSize        = 8
	seriesIDSize    = 8
	positionSize    = 4
	blockLengthSize = 4

	minTimeOffset = 20
	maxTimeOffset = 12

	// keyLengthSize is the number of bytes used to specify the length of a key in a block
	keyLengthSize = 2

	// indexEntry consists of an ID, starting position, and the min and max time
	indexEntrySize = seriesIDSize + positionSize + 2*timeSize

	// fileFooterSize is the size of the footer that's written after the index
	fileFooterSize = 2*timeSize + seriesCountSize
)

func NewDataFile(f *os.File) *dataFile {
	// seek back to the beginning to hand off to the mmap
	if _, err := f.Seek(0, 0); err != nil {
		panic(fmt.Sprintf("error seeking to beginning of file: %s", err.Error()))
	}

	fInfo, err := f.Stat()
	if err != nil {
		panic(fmt.Sprintf("error getting stats of file: %s", err.Error()))
	}
	mmap, err := syscall.Mmap(int(f.Fd()), 0, int(fInfo.Size()), syscall.PROT_READ, syscall.MAP_SHARED|MAP_POPULATE)
	if err != nil {
		panic(fmt.Sprintf("error opening mmap: %s", err.Error()))
	}

	return &dataFile{
		f:       f,
		mmap:    mmap,
		size:    uint32(fInfo.Size()),
		modTime: fInfo.ModTime(),
	}
}

// Reference records a live usage of this dataFile.  When there are
// live references, the dataFile will not be able to be closed or deleted
// until it is unreferences.
func (d *dataFile) Reference() {
	d.refs.RLock()
}

// Ureference removes a live usage of this dataFile
func (d *dataFile) Unreference() {
	d.refs.RUnlock()
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
	// Allow the current references to expire
	d.refs.Lock()
	defer d.refs.Unlock()

	d.mu.Lock()
	defer d.mu.Unlock()
	return d.close()
}

func (d *dataFile) Delete() error {
	// Allow the current references to expire
	d.refs.Lock()
	defer d.refs.Unlock()

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

func (d *dataFile) indexPosition() uint32 {
	return d.size - uint32(d.SeriesCount()*indexEntrySize+20)
}

// keyAndBlockStart will read the key at the beginning of a block or return an empty string
// if the key isn't present and give the position of the compressed block
func (d *dataFile) keyAndBlockStart(pos uint32) (string, uint32) {
	keyStart := pos + keyLengthSize
	keyLen := uint32(btou16(d.mmap[pos:keyStart]))
	if keyLen == 0 {
		return "", keyStart
	}
	key, err := snappy.Decode(nil, d.mmap[keyStart:keyStart+keyLen])
	if err != nil {
		panic(fmt.Sprintf("error decoding key: %s", err.Error()))
	}
	return string(key), keyStart + keyLen
}

func (d *dataFile) blockLengthAndEnd(blockStart uint32) (uint32, uint32) {
	length := btou32(d.mmap[blockStart : blockStart+blockLengthSize])
	return length, blockStart + length + blockLengthSize
}

// compressedBlockMinTime will return the starting time for a compressed block given
// its position
func (d *dataFile) compressedBlockMinTime(block []byte) int64 {
	return int64(btou64(block[blockLengthSize : blockLengthSize+timeSize]))
}

// StartingPositionForID returns the position in the file of the
// first block for the given ID. If zero is returned the ID doesn't
// have any data in this file.
func (d *dataFile) StartingPositionForID(id uint64) uint32 {
	pos, _, _ := d.positionMinMaxTime(id)
	return pos
}

// positionMinMaxTime will return the position of the first block for the given ID
// and the min and max time of the values for the given ID in the data file
func (d *dataFile) positionMinMaxTime(id uint64) (uint32, int64, int64) {
	seriesCount := d.SeriesCount()
	indexStart := d.indexPosition()

	min := uint32(0)
	max := uint32(seriesCount)

	for min < max {
		mid := (max-min)/2 + min

		offset := mid*indexEntrySize + indexStart
		checkID := btou64(d.mmap[offset : offset+seriesIDSize])

		if checkID == id {
			positionStart := offset + seriesIDSize
			positionEnd := positionStart + positionSize
			minTimeEnd := positionEnd + timeSize

			return btou32(d.mmap[positionStart:positionEnd]), int64(btou64(d.mmap[positionEnd:minTimeEnd])), int64(btou64(d.mmap[minTimeEnd : minTimeEnd+timeSize]))
		} else if checkID < id {
			min = mid + 1
		} else {
			max = mid
		}
	}

	return uint32(0), int64(0), int64(0)
}

// block will return the key, the encoded byte slice and the position of next block
func (d *dataFile) block(pos uint32) (key string, encodedBlock []byte, nextBlockStart uint32) {
	key, start := d.keyAndBlockStart(pos)
	_, end := d.blockLengthAndEnd(start)
	return key, d.mmap[start+blockLengthSize : end], end
}

// blockLength will return the length of the compressed block that starts at the given position
func (d *dataFile) blockLength(pos uint32) uint32 {
	return btou32(d.mmap[pos : pos+positionSize])
}

// indexMinMaxTimes will return a map of the IDs in the file along with their min and max times from the index
func (d *dataFile) indexMinMaxTimes() map[uint64]times {
	pos := d.indexPosition()
	stop := d.size - fileFooterSize

	m := make(map[uint64]times)
	for pos < stop {
		idEndPos := pos + seriesIDSize
		minEndPos := idEndPos + timeSize
		end := minEndPos + timeSize
		id := btou64(d.mmap[pos:idEndPos])
		min := int64(btou64(d.mmap[idEndPos:minEndPos]))
		max := int64(btou64(d.mmap[minEndPos:end]))
		pos = end

		m[id] = times{min: min, max: max}
	}

	return m
}

type times struct {
	min int64
	max int64
}

// func (d *dataFile) block(pos uint32) (id uint64, t int64, block []byte) {
// 	defer func() {
// 		if r := recover(); r != nil {
// 			panic(fmt.Sprintf("panic decoding file: %s at position %d for id %d at time %d", d.f.Name(), pos, id, t))
// 		}
// 	}()
// 	if pos < d.indexPosition() {
// 		id = d.idForPosition(pos)
// 		length := btou32(d.mmap[pos+8 : pos+12])
// 		block = d.mmap[pos+blockHeaderSize : pos+blockHeaderSize+length]
// 		t = int64(btou64(d.mmap[pos+blockHeaderSize : pos+blockHeaderSize+8]))
// 	}
// 	return
// }

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

// completionCheckpoint holds the new files and old files from a compaction or rewrite
type completionCheckpoint struct {
	OldFiles []string
	NewFiles []string
}

// cleanup will remove the checkpoints for new files, remove the old files. meant to be called only on startup
func (c *completionCheckpoint) cleanup() {
	// first clear out the checkpoints
	for _, f := range c.NewFiles {
		if err := os.RemoveAll(checkpointFileName(f)); err != nil && !os.IsNotExist(err) {
			panic(fmt.Sprintf("error removing checkpoint file: %s", err.Error()))
		}
	}

	// now delete the old data files
	for _, f := range c.OldFiles {
		if err := os.RemoveAll(f); err != nil && !os.IsNotExist(err) {
			panic(fmt.Sprintf("error removing old file: %s", err.Error()))
		}
	}
}

// footerSize will return what the size of the index and footer of a data file
// will be given the passed in series count
func footerSize(seriesCount int) uint32 {
	return uint32(seriesCount*indexEntrySize + fileFooterSize)
}

// writeValues will encode the values and write them as a compressed block to the file.
func writeValues(f *os.File, id uint64, key string, values Values, firstWriteForKey bool, buf []byte) uint32 {
	bytesWritten := uint32(keyLengthSize)

	// write the key length and, optionally, the key
	if firstWriteForKey {
		bytesWritten += writeKey(f, key)
	} else {
		mustWrite(f, zeroLenKey)
	}

	// encode the block and write it
	b, err := values.Encode(buf)
	if err != nil {
		panic(fmt.Sprintf("failure encoding block: %s", err.Error()))
	}

	mustWrite(f, u32tob(uint32(len(b))))
	mustWrite(f, b)

	return bytesWritten + uint32(blockLengthSize+len(b))
}

// writeKey will snappy encode the key, write the length and the compressed key and return the bytes written
func writeKey(f *os.File, key string) uint32 {
	b := snappy.Encode(nil, []byte(key))
	mustWrite(f, u16tob(uint16(len(b))))
	mustWrite(f, b)
	return uint32(len(b))
}

// writeIndex will write out the index block and the footer of the file. After this call it should
// be a read only file that can be mmap'd as a dataFile
func writeIndex(f *os.File, index *indexData) {
	// write the file index, starting with the series ids, their starting positions, and their min and max times
	for i, id := range index.ids {
		mustWrite(f, u64tob(id))
		mustWrite(f, u32tob(index.positions[i]))
		mustWrite(f, i64tob(index.minTimes[i]))
		mustWrite(f, i64tob(index.maxTimes[i]))
	}

	// min and max time for the file
	mustWrite(f, i64tob(index.fileMinTime))
	mustWrite(f, i64tob(index.fileMaxTime+1))

	// finally the series count
	mustWrite(f, u32tob(uint32(len(index.ids))))

	// sync it
	if err := f.Sync(); err != nil {
		panic(fmt.Sprintf("error syncing file after index write: %s", err.Error()))
	}
}

// mustWrite will write the bytes to the file or panic if an error is returned
func mustWrite(f *os.File, b []byte) {
	if _, err := f.Write(b); err != nil {
		panic(fmt.Sprintf("error writing: %s", err.Error()))
	}
}

// openFileAndCehckpoint will create a checkpoint file, open a new file for
// writing a data index, write the header and return the file
func openFileAndCheckpoint(fileName string) *os.File {
	checkpointFile := checkpointFileName(fileName)
	cf, err := os.OpenFile(checkpointFile, os.O_CREATE, 0666)
	if err != nil {
		panic(fmt.Sprintf("error opening checkpoint file: %s", err.Error()))
	}
	if err := cf.Close(); err != nil {
		panic(fmt.Sprintf("error closing checkpoint file: %s", err.Error()))
	}

	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		panic(fmt.Sprintf("error opening new data file: %s", err.Error()))
	}

	// write the header, which is just the magic number
	if _, err := f.Write(u32tob(magicNumber)); err != nil {
		f.Close()
		panic(fmt.Sprintf("error writing header to data file: %s", err.Error()))
	}

	return f
}

// checkpointFileName will return the checkpoint name for the data files
func checkpointFileName(fileName string) string {
	return fmt.Sprintf("%s.%s", fileName, CheckpointExtension)
}

// removeCheckpoint removes the checkpoint for a new data file that was getting written
func removeCheckpoint(fileName string) {
	checkpointFile := fmt.Sprintf("%s.%s", fileName, CheckpointExtension)
	err := os.RemoveAll(checkpointFile)
	if err != nil && !os.IsNotExist(err) {
		panic(fmt.Sprintf("error removing checkpoint: %s", err.Error()))
	}
}

// u64tob converts a uint64 into an 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func i64tob(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
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

func u16tob(v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return b
}

func btou16(b []byte) uint16 {
	return uint16(binary.BigEndian.Uint16(b))
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

// sortedIDs returns a sorted slice of the ids in the passed in map
func sortedIDs(valuesByID map[uint64][]*valuesWithKey) []uint64 {
	ids := make([]uint64, 0, len(valuesByID))
	for id, _ := range valuesByID {
		ids = append(ids, id)
	}
	sort.Sort(uint64slice(ids))
	return ids
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

type valuesWithKey struct {
	key    string
	values []Value
}

type valuesWithKeySlice []*valuesWithKey

func (a valuesWithKeySlice) Len() int           { return len(a) }
func (a valuesWithKeySlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a valuesWithKeySlice) Less(i, j int) bool { return strings.Compare(a[i].key, a[j].key) == -1 }
