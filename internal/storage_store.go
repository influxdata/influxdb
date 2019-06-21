package internal

import (
	"context"
	"errors"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"go.uber.org/zap"
)

// TSDBStoreMock is a mockable implementation of storage.Store.
//
// It's currently a partial implementation as one of a store's exported methods
// returns an unexported type.
type StorageStoreMock struct {
	ReadFilterFn func(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error)
	ReadGroupFn  func(ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error)

	TagKeysFn    func(ctx context.Context, req *datatypes.TagKeysRequest) (cursors.StringIterator, error)
	TagValuesFn  func(ctx context.Context, req *datatypes.TagValuesRequest) (cursors.StringIterator, error)
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
	store.ReadFilterFn = func(context.Context, *datatypes.ReadFilterRequest) (reads.ResultSet, error) {
		return store.ResultSet, nil
	}
	store.ReadGroupFn = func(context.Context, *datatypes.ReadGroupRequest) (reads.GroupResultSet, error) {
		return nil, errors.New("implement me")
	}
	store.TagKeysFn = func(context.Context, *datatypes.TagKeysRequest) (cursors.StringIterator, error) {
		return nil, errors.New("implement me")
	}
	store.TagValuesFn = func(context.Context, *datatypes.TagValuesRequest) (cursors.StringIterator, error) {
		return nil, errors.New("implement me")
	}
	return store
}

func (s *StorageStoreMock) ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error) {
	return s.ReadFilterFn(ctx, req)
}

func (s *StorageStoreMock) ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error) {
	return s.ReadGroupFn(ctx, req)
}

func (s *StorageStoreMock) TagKeys(ctx context.Context, req *datatypes.TagKeysRequest) (cursors.StringIterator, error) {
	return s.TagKeysFn(ctx, req)
}

func (s *StorageStoreMock) TagValues(ctx context.Context, req *datatypes.TagValuesRequest) (cursors.StringIterator, error) {
	return s.TagValuesFn(ctx, req)
}

func (s *StorageStoreMock) GetSource(db, rp string) proto.Message {
	return &storage.ReadSource{Database: db, RetentionPolicy: rp}
}

// WithLogger sets the logger.
func (s *StorageStoreMock) WithLogger(log *zap.Logger) {
	s.WithLoggerFn(log)
}

// StorageResultsMock implements the storage.Results interface providing the
// ability to emit mock results from calls to the StorageStoreMock.Read method.
type StorageResultsMock struct {
	CloseFn  func()
	NextFn   func() bool
	CursorFn func() tsdb.Cursor
	TagsFn   func() models.Tags
	ErrFn    func() error
	StatsFn  func() tsdb.CursorStats
}

// NewStorageResultsMock initialises a StorageResultsMock whose methods all return
// their zero value.
func NewStorageResultsMock() *StorageResultsMock {
	return &StorageResultsMock{
		CloseFn:  func() {},
		NextFn:   func() bool { return false },
		CursorFn: func() tsdb.Cursor { return nil },
		TagsFn:   func() models.Tags { return nil },
		ErrFn:    func() error { return nil },
		StatsFn:  func() tsdb.CursorStats { return tsdb.CursorStats{} },
	}
}

func (r *StorageResultsMock) Err() error { return r.ErrFn() }

func (r *StorageResultsMock) Stats() tsdb.CursorStats {
	return r.StatsFn()
}

// Close closes the result set.
func (r *StorageResultsMock) Close() { r.CloseFn() }

// Next returns true if there are more results available.
func (r *StorageResultsMock) Next() bool { return r.NextFn() }

// Cursor returns the cursor for the result set.
func (r *StorageResultsMock) Cursor() tsdb.Cursor { return r.CursorFn() }

// Tags returns the series' tag set.
func (r *StorageResultsMock) Tags() models.Tags { return r.TagsFn() }
