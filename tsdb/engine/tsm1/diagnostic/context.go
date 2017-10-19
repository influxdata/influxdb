package diagnostic

import "time"

type Context struct {
	Handler Handler
}

type Handler interface {
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

	CacheLoaderHandler
	WALHandler
	FileStoreHandler
}

func (c *Context) SnapshotWritten(path string, dur time.Duration) {
	if c.Handler != nil {
		c.Handler.SnapshotWritten(path, dur)
	}
}

func (c *Context) SnapshotDeduplicated(path string, dur time.Duration) {
	if c.Handler != nil {
		c.Handler.SnapshotDeduplicated(path, dur)
	}
}

func (c *Context) SnapshotWriteError(err error) {
	if c.Handler != nil {
		c.Handler.SnapshotWriteError(err)
	}
}

func (c *Context) DataTypeRetrievalError(key []byte, err error) {
	if c.Handler != nil {
		c.Handler.DataTypeRetrievalError(key, err)
	}
}

func (c *Context) MetaDataIndexLoaded(shardID uint64, dur time.Duration) {
	if c.Handler != nil {
		c.Handler.MetaDataIndexLoaded(shardID, dur)
	}
}

func (c *Context) ReloadedWALCache(path string, dur time.Duration) {
	if c.Handler != nil {
		c.Handler.ReloadedWALCache(path, dur)
	}
}

func (c *Context) WriteCompactorSnapshotError(err error) {
	if c.Handler != nil {
		c.Handler.WriteCompactorSnapshotError(err)
	}
}

func (c *Context) AddNewTSMFilesFromSnapshotError(err error) {
	if c.Handler != nil {
		c.Handler.AddNewTSMFilesFromSnapshotError(err)
	}
}

func (c *Context) RemovingClosedWALSegmentsError(err error) {
	if c.Handler != nil {
		c.Handler.RemovingClosedWALSegmentsError(err)
	}
}

func (c *Context) CompactingCache(path string) {
	if c.Handler != nil {
		c.Handler.CompactingCache(path)
	}
}

func (c *Context) RefreshIndexError(level int, err error) {
	if c.Handler != nil {
		c.Handler.RefreshIndexError(level, err)
	}
}

func (c *Context) StartCompactionLoop(id uint64, levelGroups [][]int) {
	if c.Handler != nil {
		c.Handler.StartCompactionLoop(id, levelGroups)
	}
}

func (c *Context) RunCompaction(level int, id uint64, levelGroups [][]int) {
	if c.Handler != nil {
		c.Handler.RunCompaction(level, id, levelGroups)
	}
}

func (c *Context) BeginningCompaction(description string, group []string) {
	if c.Handler != nil {
		c.Handler.BeginningCompaction(description, group)
	}
}

func (c *Context) AbortedCompaction(description string, err error) {
	if c.Handler != nil {
		c.Handler.AbortedCompaction(description, err)
	}
}

func (c *Context) CompactionError(err error) {
	if c.Handler != nil {
		c.Handler.CompactionError(err)
	}
}

func (c *Context) FinishedCompaction(description string, group []string, files []string, dur time.Duration) {
	if c.Handler != nil {
		c.Handler.FinishedCompaction(description, group, files, dur)
	}
}

func (c *Context) ReplaceNewTSMFilesError(err error) {
	if c.Handler != nil {
		c.Handler.ReplaceNewTSMFilesError(err)
	}
}

func (c *Context) IteratorFinalized(itrType string) {
	if c.Handler != nil {
		c.Handler.IteratorFinalized(itrType)
	}
}

type CacheLoaderContext struct {
	Handler CacheLoaderHandler
}

type CacheLoaderHandler interface {
	CacheReadingFile(path string, size int64)
	CacheFileCorrupt(path string, pos int64)
}

func (c *CacheLoaderContext) CacheReadingFile(path string, size int64) {
	if c.Handler != nil {
		c.Handler.CacheReadingFile(path, size)
	}
}

func (c *CacheLoaderContext) CacheFileCorrupt(path string, pos int64) {
	if c.Handler != nil {
		c.Handler.CacheFileCorrupt(path, pos)
	}
}

type WALContext struct {
	Handler WALHandler
}

type WALHandler interface {
	StartingWAL(path string, segmentSize int)
	RemovingWALFile(path string)
	ClosingWALFile(path string)
}

func (c *WALContext) StartingWAL(path string, segmentSize int) {
	if c.Handler != nil {
		c.Handler.StartingWAL(path, segmentSize)
	}
}

func (c *WALContext) RemovingWALFile(path string) {
	if c.Handler != nil {
		c.Handler.RemovingWALFile(path)
	}
}

func (c *WALContext) ClosingWALFile(path string) {
	if c.Handler != nil {
		c.Handler.ClosingWALFile(path)
	}
}

type FileStoreContext struct {
	Handler FileStoreHandler
}

type FileStoreHandler interface {
	OpenedFile(path string, idx int, dur time.Duration)
	CreatingSnapshot(dir string)
	PurgeFileCloseError(err error)
	PurgeFileRemoveError(err error)
}

func (c *FileStoreContext) OpenedFile(path string, idx int, dur time.Duration) {
	if c.Handler != nil {
		c.Handler.OpenedFile(path, idx, dur)
	}
}

func (c *FileStoreContext) CreatingSnapshot(dir string) {
	if c.Handler != nil {
		c.Handler.CreatingSnapshot(dir)
	}
}

func (c *FileStoreContext) PurgeFileCloseError(err error) {
	if c.Handler != nil {
		c.Handler.PurgeFileCloseError(err)
	}
}

func (c *FileStoreContext) PurgeFileRemoveError(err error) {
	if c.Handler != nil {
		c.Handler.PurgeFileRemoveError(err)
	}
}
