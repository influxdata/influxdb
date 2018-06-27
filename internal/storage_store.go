package internal

import (
	"context"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

// TSDBStoreMock is a mockable implementation of storage.Store.
//
// It's currently a partial implementation as one of a store's exported methods
// returns an unexported type.
type StorageStoreMock struct {
	ReadFn       func(ctx context.Context, req *storage.ReadRequest) (storage.Results, error)
	WithLoggerFn func(log *zap.Logger)

	ResultSet *StorageResultsMock
	// TODO(edd): can't mock GroupRead as it returns an unexported type.
}

// NewStorageStoreMock initialises a StorageStoreMock with methods that return
// their zero values. It also initialises a StorageResultsMock, which can be
// configured via the ResultSet field.
func NewStorageStoreMock() *StorageStoreMock {
	store := &StorageStoreMock{
		WithLoggerFn: func(*zap.Logger) {},
		ResultSet:    NewStorageResultsMock(),
	}
	store.ReadFn = func(context.Context, *storage.ReadRequest) (storage.Results, error) {
		return store.ResultSet, nil
	}
	return store
}

// WithLogger sets the logger.
func (s *StorageStoreMock) WithLogger(log *zap.Logger) {
	s.WithLoggerFn(log)
}

// Read reads the storage request and returns a cursor to access results.
func (s *StorageStoreMock) Read(ctx context.Context, req *storage.ReadRequest) (storage.Results, error) {
	return s.ReadFn(ctx, req)
}

// StorageResultsMock implements the storage.Results interface providing the
// ability to emit mock results from calls to the StorageStoreMock.Read method.
type StorageResultsMock struct {
	CloseFn  func()
	NextFn   func() bool
	CursorFn func() tsdb.Cursor
	TagsFn   func() models.Tags
}

// NewStorageResultsMock initialises a StorageResultsMock whose methods all return
// their zero value.
func NewStorageResultsMock() *StorageResultsMock {
	return &StorageResultsMock{
		CloseFn:  func() {},
		NextFn:   func() bool { return false },
		CursorFn: func() tsdb.Cursor { return nil },
		TagsFn:   func() models.Tags { return nil },
	}
}

// Close closes the result set.
func (r *StorageResultsMock) Close() { r.CloseFn() }

// Next returns true if there are more results available.
func (r *StorageResultsMock) Next() bool { return r.NextFn() }

// Cursor returns the cursor for the result set.
func (r *StorageResultsMock) Cursor() tsdb.Cursor { return r.CursorFn() }

// Tags returns the series' tag set.
func (r *StorageResultsMock) Tags() models.Tags { return r.TagsFn() }
