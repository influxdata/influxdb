package diagnostic

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	tsdb "github.com/influxdata/influxdb/tsdb/diagnostic"
	tsm1 "github.com/influxdata/influxdb/tsdb/engine/tsm1/diagnostic"
	"go.uber.org/zap"
)

type StoreHandler struct {
	base *zap.Logger
	l    *zap.Logger
}

func (s *Service) StoreContext() tsdb.Context {
	if s == nil {
		return nil
	}
	return &StoreHandler{
		base: s.l,
		l:    s.l.With(zap.String("service", "store")),
	}
}

func (h *StoreHandler) AttachEngine(name string, engine interface{}) {
	switch engine := engine.(type) {
	case interface {
		WithDiagnosticContext(tsm1.Context)
	}:
		engine.WithDiagnosticContext(h.TSM1Context())
	default:
		h.l.Warn("no diagnostic handler found for engine", zap.String("engine", name))
	}
}

func (h *StoreHandler) AttachIndex(name string, index interface{}) {
	switch index := index.(type) {
	case interface {
		WithDiagnosticContext(TSI1Context)
	}:
		index.WithDiagnosticContext(h.TSI1Context())
	default:
		if name == "inmem" {
			// This index does not have a diagnostic context.
			return
		}
		h.l.Warn("no diagnostic handler found for index", zap.String("index", name))
	}
}

func (h *StoreHandler) SeriesCardinalityError(err error) {
	h.l.Error("cannot retrieve series cardinality", zap.Error(err))
}

func (h *StoreHandler) MeasurementsCardinalityError(err error) {
	h.l.Error("cannot retrieve measurement cardinality", zap.Error(err))
}

func (h *StoreHandler) UsingDataDir(path string) {
	h.l.Info(fmt.Sprintf("Using data dir: %v", path))
}

func (h *StoreHandler) NotADatabaseDir(name string) {
	h.l.Info("Not loading. Not a database directory.", zap.String("name", name))
}

func (h *StoreHandler) SkippingRetentionPolicyDir(name string) {
	h.l.Info(fmt.Sprintf("Skipping retention policy dir: %s. Not a directory", name))
}

func (h *StoreHandler) ShardOpened(path string, dur time.Duration) {
	h.l.Info(fmt.Sprintf("%s opened in %s", path, dur))
}

func (h *StoreHandler) ShardOpenError(err error) {
	h.l.Info(err.Error())
}

func (h *StoreHandler) FreeColdShardError(err error) {
	h.l.Warn("error free cold shard resources:", zap.Error(err))
}

func (h *StoreHandler) MeasurementNamesByExprError(err error) {
	h.l.Warn("cannot retrieve measurement names", zap.Error(err))
}

func (h *StoreHandler) WarnMaxValuesPerTagLimitExceeded(perc, n, max int, db string, name, tag []byte) {
	h.l.Info(fmt.Sprintf("WARN: %d%% of max-values-per-tag limit exceeded: (%d/%d), db=%s measurement=%s tag=%s",
		perc, n, max, db, name, tag))
}

type TSM1Handler struct {
	l           *zap.Logger
	cacheLoader *zap.Logger
	wal         *zap.Logger
	fs          *zap.Logger
}

func (h *StoreHandler) TSM1Context() tsm1.Context {
	logger := h.base.With(zap.String("engine", "tsm1"))
	return &TSM1Handler{
		l:           logger,
		cacheLoader: logger.With(zap.String("service", "cacheloader")),
		wal:         logger.With(zap.String("service", "wal")),
		fs:          logger.With(zap.String("service", "filestore")),
	}
}

func (h *TSM1Handler) SnapshotWritten(path string, dur time.Duration) {
	h.l.Info(fmt.Sprintf("Snapshot for path %s written in %v", path, dur))
}

func (h *TSM1Handler) SnapshotDeduplicated(path string, dur time.Duration) {
	h.l.Info(fmt.Sprintf("Snapshot for path %s deduplicated in %v", path, dur))
}

func (h *TSM1Handler) SnapshotWriteError(err error) {
	h.l.Info(fmt.Sprintf("error writing snapshot: %v", err))
}

func (h *TSM1Handler) DataTypeRetrievalError(key []byte, err error) {
	h.l.Info(fmt.Sprintf("error getting the data type of values for key %s: %s", key, err.Error()))
}

func (h *TSM1Handler) MetaDataIndexLoaded(shardID uint64, dur time.Duration) {
	h.l.Info(fmt.Sprintf("Meta data index for shard %d loaded in %v", shardID, dur))
}

func (h *TSM1Handler) ReloadedWALCache(path string, dur time.Duration) {
	h.l.Info(fmt.Sprintf("Reloaded WAL cache %s in %v", path, dur))
}

func (h *TSM1Handler) WriteCompactorSnapshotError(err error) {
	h.l.Info(fmt.Sprintf("error writing snapshot from compactor: %v", err))
}

func (h *TSM1Handler) AddNewTSMFilesFromSnapshotError(err error) {
	h.l.Info(fmt.Sprintf("error adding new TSM files from snapshot: %v", err))
}

func (h *TSM1Handler) RemovingClosedWALSegmentsError(err error) {
	h.l.Info(fmt.Sprintf("error removing closed wal segments: %v", err))
}

func (h *TSM1Handler) CompactingCache(path string) {
	h.l.Info(fmt.Sprintf("Compacting cache for %s", path))
}

func (h *TSM1Handler) RefreshIndexError(level int, err error) {
	h.l.Error(fmt.Sprintf("refresh index (%d): %v", level, err))
}

func (h *TSM1Handler) StartCompactionLoop(id uint64, levelGroups [][]int) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "compact id=%d", id)
	for _, group := range levelGroups {
		buf.WriteString(" (")
		for i, v := range group {
			if i != 0 {
				buf.WriteString("/")
			}
			buf.WriteString(strconv.Itoa(v))
		}
		buf.WriteString(")")
	}
	h.l.Info(buf.String())
}

func (h *TSM1Handler) RunCompaction(level int, id uint64, levelGroups [][]int) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "compact run=%d id=%d", level, id)
	for _, group := range levelGroups {
		buf.WriteString(" (")
		for i, v := range group {
			if i != 0 {
				buf.WriteString("/")
			}
			buf.WriteString(strconv.Itoa(v))
		}
		buf.WriteString(")")
	}
	h.l.Info(buf.String())
}

func (h *TSM1Handler) BeginningCompaction(description string, group []string) {
	h.l.Info(fmt.Sprintf("beginning %s compaction, %d TSM files", description, len(group)))
	for i, f := range group {
		h.l.Info(fmt.Sprintf("compacting %s %s (#%d)", description, f, i))
	}
}

func (h *TSM1Handler) AbortedCompaction(description string, err error) {
	h.l.Info(fmt.Sprintf("aborted %s compaction. %v", description, err))
}

func (h *TSM1Handler) CompactionError(err error) {
	h.l.Info(fmt.Sprintf("error compacting TSM files: %v", err))
}

func (h *TSM1Handler) FinishedCompaction(description string, group []string, files []string, dur time.Duration) {
	for i, f := range files {
		h.l.Info(fmt.Sprintf("compacted %s into %s (#%d)", description, f, i))
	}
	h.l.Info(fmt.Sprintf("compacted %s %d files into %d files in %s", description, len(group), len(files), dur))
}

func (h *TSM1Handler) ReplaceNewTSMFilesError(err error) {
	h.l.Info(fmt.Sprintf("error replacing new TSM files: %v", err))
}

func (h *TSM1Handler) IteratorFinalized(itrType string) {
	h.l.Error(fmt.Sprintf("%s finalized by GC", itrType))
}

func (h *TSM1Handler) CacheReadingFile(path string, size int64) {
	h.cacheLoader.Info(fmt.Sprintf("reading file %s, size %d", path, size))
}

func (h *TSM1Handler) CacheFileCorrupt(path string, pos int64) {
	h.cacheLoader.Info(fmt.Sprintf("file %s corrupt at position %d, truncating", path, pos))
}

func (h *TSM1Handler) StartingWAL(path string, segmentSize int) {
	h.wal.Info(fmt.Sprintf("tsm1 WAL starting with %d segment size", segmentSize))
	h.wal.Info(fmt.Sprintf("tsm1 WAL writing to %s", path))
}

func (h *TSM1Handler) RemovingWALFile(path string) {
	h.wal.Info(fmt.Sprintf("Removing %s", path))
}

func (h *TSM1Handler) ClosingWALFile(path string) {
	h.wal.Info(fmt.Sprintf("Closing %s", path))
}

func (h *TSM1Handler) OpenedFile(path string, idx int, dur time.Duration) {
	h.fs.Info(fmt.Sprintf("%s (#%d) opened in %v", path, idx, dur))
}

func (h *TSM1Handler) CreatingSnapshot(dir string) {
	h.fs.Info(fmt.Sprintf("Creating snapshot in %s", dir))
}

func (h *TSM1Handler) PurgeFileCloseError(err error) {
	h.fs.Info(fmt.Sprintf("purge: close file: %v", err))
}

func (h *TSM1Handler) PurgeFileRemoveError(err error) {
	h.fs.Info(fmt.Sprintf("purge: remove file: %v", err))
}

type TSI1Context interface {
	WithCompactionToken(token string) TSI1CompactionContext
	WithLogFileCompactionToken(token string, id int) TSI1LogFileCompactionContext
}

type TSI1CompactionContext interface {
	PerformingFullCompaction(src []int, dst string)
	CompletedFullCompaction(path string, elapsed time.Duration, bytes int64)
	CreateCompactionFilesError(err error)
	ManifestWriteError(err error)
	CannotCompactIndexFiles(err error)
	ErrorClosingIndexFile(err error)
	CannotOpenNewIndexFile(err error)
	RemovingIndexFile(path string)
	CannotCloseIndexFile(err error)
	CannotRemoveIndexFile(err error)
}

type TSI1LogFileCompactionContext interface {
	CannotCreateIndexFile(err error)
	CannotCompactLogFile(path string, err error)
	CannotOpenCompactedIndexFile(path string, err error)
	UpdateManifestError(err error)
	LogFileCompacted(elapsed time.Duration, bytes int64)
	CannotCloseLogFile(err error)
	CannotRemoveLogFile(err error)
}

type TSI1Handler struct {
	l *zap.Logger
}

func (h *StoreHandler) TSI1Context() TSI1Context {
	logger := h.l.With(zap.String("index", "tsi"))
	return &TSI1Handler{l: logger}
}

type TSI1CompactionHandler struct {
	l *zap.Logger
}

func (h *TSI1Handler) WithCompactionToken(token string) TSI1CompactionContext {
	logger := h.l.With(zap.String("token", token))
	return &TSI1CompactionHandler{l: logger}
}

func (h *TSI1CompactionHandler) PerformingFullCompaction(src []int, dst string) {
	h.l.Info("performing full compaction",
		zap.String("src", joinIntSlice(src, ",")),
		zap.String("dst", dst),
	)
}

func (h *TSI1CompactionHandler) CompletedFullCompaction(path string, elapsed time.Duration, bytes int64) {
	h.l.Info("full compaction complete",
		zap.String("path", path),
		zap.String("elapsed", elapsed.String()),
		zap.Int64("bytes", bytes),
		zap.Int("kb_per_sec", int(float64(bytes)/elapsed.Seconds())/1024),
	)
}

func (h *TSI1CompactionHandler) CreateCompactionFilesError(err error) {
	h.l.Error("cannot create compaction files", zap.Error(err))
}

func (h *TSI1CompactionHandler) ManifestWriteError(err error) {
	h.l.Error("cannot write manifest", zap.Error(err))
}

func (h *TSI1CompactionHandler) CannotCompactIndexFiles(err error) {
	h.l.Error("cannot compact index files", zap.Error(err))
}

func (h *TSI1CompactionHandler) ErrorClosingIndexFile(err error) {
	h.l.Error("error closing index file", zap.Error(err))
}

func (h *TSI1CompactionHandler) CannotOpenNewIndexFile(err error) {
	h.l.Error("cannot open new index file", zap.Error(err))
}

func (h *TSI1CompactionHandler) RemovingIndexFile(path string) {
	h.l.Info("removing index file", zap.String("path", path))
}

func (h *TSI1CompactionHandler) CannotCloseIndexFile(err error) {
	h.l.Error("cannot close index file", zap.Error(err))
}

func (h *TSI1CompactionHandler) CannotRemoveIndexFile(err error) {
	h.l.Error("cannot remove index file", zap.Error(err))
}

type TSI1LogFileCompactionHandler struct {
	l *zap.Logger
}

func (h *TSI1Handler) WithLogFileCompactionToken(token string, id int) TSI1LogFileCompactionContext {
	logger := h.l.With(
		zap.String("token", token),
		zap.Int("id", id),
	)
	return &TSI1LogFileCompactionHandler{l: logger}
}

func (h *TSI1LogFileCompactionHandler) CannotCreateIndexFile(err error) {
	h.l.Error("cannot create index file", zap.Error(err))
}

func (h *TSI1LogFileCompactionHandler) CannotCompactLogFile(path string, err error) {
	h.l.Error("cannot compact log file", zap.Error(err), zap.String("path", path))
}

func (h *TSI1LogFileCompactionHandler) CannotOpenCompactedIndexFile(path string, err error) {
	h.l.Error("cannot open compacted index file", zap.Error(err), zap.String("path", path))
}

func (h *TSI1LogFileCompactionHandler) UpdateManifestError(err error) {
	h.l.Error("cannot update manifest", zap.Error(err))
}

func (h *TSI1LogFileCompactionHandler) LogFileCompacted(elapsed time.Duration, bytes int64) {
	h.l.Error("log file compacted",
		zap.String("elapsed", elapsed.String()),
		zap.Int64("bytes", bytes),
		zap.Int("kb_per_sec", int(float64(bytes)/elapsed.Seconds())/1024),
	)
}

func (h *TSI1LogFileCompactionHandler) CannotCloseLogFile(err error) {
	h.l.Error("cannot close log file", zap.Error(err))
}

func (h *TSI1LogFileCompactionHandler) CannotRemoveLogFile(err error) {
	h.l.Error("cannot remove log file", zap.Error(err))
}

func joinIntSlice(a []int, sep string) string {
	other := make([]string, len(a))
	for i := range a {
		other[i] = strconv.Itoa(a[i])
	}
	return strings.Join(other, sep)
}
