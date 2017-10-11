package diagnostic

import "time"

type Context interface {
	SnapshotWritten(path string, dur time.Duration)
	SnapshotDeduplicated(path string, dur time.Duration)
	SnapshotWriteError(err error)

	DataTypeRetrievalError(key []byte, err error)
	MetaDataIndexLoaded(shardID uint64, dur time.Duration)
	ReloadedWALCache(path string, dur time.Duration)

	WriteCompactorSnapshotError(err error)
	AddNewTSMFilesFromSnapshotError(err error)
	RemovingClosedWALSegmentsError(err error)

	CompactingCache(path string)
	RefreshIndexError(level int, err error)
	StartCompactionLoop(id uint64, levelGroups [][]int)
	RunCompaction(level int, id uint64, levelGroups [][]int)

	BeginningCompaction(description string, group []string)
	AbortedCompaction(description string, err error)
	CompactionError(err error)
	FinishedCompaction(description string, group []string, files []string, dur time.Duration)
	ReplaceNewTSMFilesError(err error)

	IteratorFinalized(itrType string)

	CacheLoaderContext
	WALContext
	FileStoreContext
}

type CacheLoaderContext interface {
	CacheReadingFile(path string, size int64)
	CacheFileCorrupt(path string, pos int64)
}

type WALContext interface {
	StartingWAL(path string, segmentSize int)
	RemovingWALFile(path string)
	ClosingWALFile(path string)
}

type FileStoreContext interface {
	OpenedFile(path string, idx int, dur time.Duration)
	CreatingSnapshot(dir string)
	PurgeFileCloseError(err error)
	PurgeFileRemoveError(err error)
}
