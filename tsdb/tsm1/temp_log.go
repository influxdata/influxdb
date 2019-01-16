package tsm1

import (
	"time"

	"go.uber.org/zap"
)

// TODO(jeff): this only exists temporarily while we move the WAL into storage

// Log describes an interface for a durable disk-based log.
type Log interface {
	WithLogger(*zap.Logger)

	Open() error
	Close() error
	Path() string

	LastWriteTime() time.Time
	DiskSizeBytes() int64

	WriteMulti(values map[string][]Value) (int, error)
	DeleteRange(keys [][]byte, min, max int64) (int, error)

	CloseSegment() error
	ClosedSegments() ([]string, error)
	Remove(files []string) error
}

// NopWAL implements the Log interface and provides a no-op WAL implementation.
type NopWAL struct{}

func (w NopWAL) WithLogger(*zap.Logger) {}

func (w NopWAL) Open() error  { return nil }
func (w NopWAL) Close() error { return nil }
func (w NopWAL) Path() string { return "" }

func (w NopWAL) LastWriteTime() time.Time { return time.Time{} }
func (w NopWAL) DiskSizeBytes() int64     { return 0 }

func (w NopWAL) WriteMulti(values map[string][]Value) (int, error)      { return 0, nil }
func (w NopWAL) DeleteRange(keys [][]byte, min, max int64) (int, error) { return 0, nil }

func (w NopWAL) CloseSegment() error               { return nil }
func (w NopWAL) ClosedSegments() ([]string, error) { return nil, nil }
func (w NopWAL) Remove(files []string) error       { return nil }
