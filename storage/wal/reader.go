package wal

import (
	"os"
	"sort"

	"go.uber.org/zap"
)

// WALReader helps one read out the WAL into entries.
type WALReader struct {
	files  []string
	logger *zap.Logger
	r      *WALSegmentReader
}

// NewWALReader constructs a WALReader over the given set of files.
func NewWALReader(files []string) *WALReader {
	sort.Strings(files)
	return &WALReader{
		files:  files,
		logger: zap.NewNop(),
		r:      nil,
	}
}

// WithLogger sets the logger for the WALReader.
func (r *WALReader) WithLogger(logger *zap.Logger) { r.logger = logger }

// Read calls the callback with every entry in the WAL files. If, during
// reading of a segment file, corruption is encountered, that segment file
// is truncated up to and including the last valid byte, and processing
// continues with the next segment file.
func (r *WALReader) Read(cb func(WALEntry) error) error {
	for _, file := range r.files {
		if err := r.readFile(file, cb); err != nil {
			return err
		}
	}
	return nil
}

// readFile reads the file and calls the callback with each WAL entry.
// It uses the provided logger for information about progress and corruptions.
func (r *WALReader) readFile(file string, cb func(WALEntry) error) error {
	f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return err
	}
	r.logger.Info("Reading file", zap.String("path", file), zap.Int64("size", stat.Size()))

	if stat.Size() == 0 {
		return nil
	}

	if r.r == nil {
		r.r = NewWALSegmentReader(f)
	} else {
		r.r.Reset(f)
	}
	defer r.r.Close()

	for r.r.Next() {
		entry, err := r.r.Read()
		if err != nil {
			n := r.r.Count()
			r.logger.Info("File corrupt", zap.Error(err), zap.String("path", file), zap.Int64("pos", n))
			if err := f.Truncate(n); err != nil {
				return err
			}
			break
		}

		if err := cb(entry); err != nil {
			return err
		}
	}

	return r.r.Close()
}
