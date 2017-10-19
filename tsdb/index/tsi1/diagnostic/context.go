package diagnostic

import "time"

type Context struct {
	Handler Handler
}

type Handler interface {
	WithCompactionToken(token string) CompactionHandler
	WithLogFileCompactionToken(token string, id int) LogFileCompactionHandler
}

func (c *Context) WithCompactionToken(token string) CompactionContext {
	var handler CompactionHandler
	if c.Handler != nil {
		handler = c.Handler.WithCompactionToken(token)
	}
	return CompactionContext{Handler: handler}
}

func (c *Context) WithLogFileCompactionToken(token string, id int) LogFileCompactionContext {
	var handler LogFileCompactionHandler
	if c.Handler != nil {
		handler = c.Handler.WithLogFileCompactionToken(token, id)
	}
	return LogFileCompactionContext{Handler: handler}
}

type CompactionContext struct {
	Handler CompactionHandler
}

type CompactionHandler interface {
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

func (c *CompactionContext) PerformingFullCompaction(src []int, dst string) {
	if c.Handler != nil {
		c.Handler.PerformingFullCompaction(src, dst)
	}
}

func (c *CompactionContext) CompletedFullCompaction(path string, elapsed time.Duration, bytes int64) {
	if c.Handler != nil {
		c.Handler.CompletedFullCompaction(path, elapsed, bytes)
	}
}

func (c *CompactionContext) CreateCompactionFilesError(err error) {
	if c.Handler != nil {
		c.Handler.CreateCompactionFilesError(err)
	}
}

func (c *CompactionContext) ManifestWriteError(err error) {
	if c.Handler != nil {
		c.Handler.ManifestWriteError(err)
	}
}

func (c *CompactionContext) CannotCompactIndexFiles(err error) {
	if c.Handler != nil {
		c.Handler.CannotCompactIndexFiles(err)
	}
}

func (c *CompactionContext) ErrorClosingIndexFile(err error) {
	if c.Handler != nil {
		c.Handler.ErrorClosingIndexFile(err)
	}
}

func (c *CompactionContext) CannotOpenNewIndexFile(err error) {
	if c.Handler != nil {
		c.Handler.CannotOpenNewIndexFile(err)
	}
}

func (c *CompactionContext) RemovingIndexFile(path string) {
	if c.Handler != nil {
		c.Handler.RemovingIndexFile(path)
	}
}

func (c *CompactionContext) CannotCloseIndexFile(err error) {
	if c.Handler != nil {
		c.Handler.CannotCloseIndexFile(err)
	}
}

func (c *CompactionContext) CannotRemoveIndexFile(err error) {
	if c.Handler != nil {
		c.Handler.CannotRemoveIndexFile(err)
	}
}

type LogFileCompactionContext struct {
	Handler LogFileCompactionHandler
}

type LogFileCompactionHandler interface {
	CannotCreateIndexFile(err error)
	CannotCompactLogFile(path string, err error)
	CannotOpenCompactedIndexFile(path string, err error)
	UpdateManifestError(err error)
	LogFileCompacted(elapsed time.Duration, bytes int64)
	CannotCloseLogFile(err error)
	CannotRemoveLogFile(err error)
}

func (c *LogFileCompactionContext) CannotCreateIndexFile(err error) {
	if c.Handler != nil {
		c.Handler.CannotCreateIndexFile(err)
	}
}

func (c *LogFileCompactionContext) CannotCompactLogFile(path string, err error) {
	if c.Handler != nil {
		c.Handler.CannotCompactLogFile(path, err)
	}
}

func (c *LogFileCompactionContext) CannotOpenCompactedIndexFile(path string, err error) {
	if c.Handler != nil {
		c.Handler.CannotOpenCompactedIndexFile(path, err)
	}
}

func (c *LogFileCompactionContext) UpdateManifestError(err error) {
	if c.Handler != nil {
		c.Handler.UpdateManifestError(err)
	}
}

func (c *LogFileCompactionContext) LogFileCompacted(elapsed time.Duration, bytes int64) {
	if c.Handler != nil {
		c.Handler.LogFileCompacted(elapsed, bytes)
	}
}

func (c *LogFileCompactionContext) CannotCloseLogFile(err error) {
	if c.Handler != nil {
		c.Handler.CannotCloseLogFile(err)
	}
}

func (c *LogFileCompactionContext) CannotRemoveLogFile(err error) {
	if c.Handler != nil {
		c.Handler.CannotRemoveLogFile(err)
	}
}
