package diagnostic

import "time"

type Context interface {
	WithCompactionToken(token string) CompactionContext
	WithLogFileCompactionToken(token string, id int) LogFileCompactionContext
}

type CompactionContext interface {
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

type LogFileCompactionContext interface {
	CannotCreateIndexFile(err error)
	CannotCompactLogFile(path string, err error)
	CannotOpenCompactedIndexFile(path string, err error)
	UpdateManifestError(err error)
	LogFileCompacted(elapsed time.Duration, bytes int64)
	CannotCloseLogFile(err error)
	CannotRemoveLogFile(err error)
}
