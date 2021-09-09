package mock

import (
	"context"

	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb/v2/query"
)

type StorageReader struct {
	ReadFilterFn                   func(ctx context.Context, spec query.ReadFilterSpec, alloc *memory.Allocator) (query.TableIterator, error)
	ReadGroupFn                    func(ctx context.Context, spec query.ReadGroupSpec, alloc *memory.Allocator) (query.TableIterator, error)
	ReadTagKeysFn                  func(ctx context.Context, spec query.ReadTagKeysSpec, alloc *memory.Allocator) (query.TableIterator, error)
	ReadTagValuesFn                func(ctx context.Context, spec query.ReadTagValuesSpec, alloc *memory.Allocator) (query.TableIterator, error)
	ReadWindowAggregateFn          func(ctx context.Context, spec query.ReadWindowAggregateSpec, alloc *memory.Allocator) (query.TableIterator, error)
	ReadSeriesCardinalityFn        func(ctx context.Context, spec query.ReadSeriesCardinalitySpec, alloc *memory.Allocator) (query.TableIterator, error)
	SupportReadSeriesCardinalityFn func(ctx context.Context) bool
	CloseFn                        func()
}

func (s *StorageReader) ReadFilter(ctx context.Context, spec query.ReadFilterSpec, alloc *memory.Allocator) (query.TableIterator, error) {
	return s.ReadFilterFn(ctx, spec, alloc)
}

func (s *StorageReader) ReadGroup(ctx context.Context, spec query.ReadGroupSpec, alloc *memory.Allocator) (query.TableIterator, error) {
	return s.ReadGroupFn(ctx, spec, alloc)
}

func (s *StorageReader) ReadTagKeys(ctx context.Context, spec query.ReadTagKeysSpec, alloc *memory.Allocator) (query.TableIterator, error) {
	return s.ReadTagKeysFn(ctx, spec, alloc)
}

func (s *StorageReader) ReadTagValues(ctx context.Context, spec query.ReadTagValuesSpec, alloc *memory.Allocator) (query.TableIterator, error) {
	return s.ReadTagValuesFn(ctx, spec, alloc)
}

func (s *StorageReader) ReadSeriesCardinality(ctx context.Context, spec query.ReadSeriesCardinalitySpec, alloc *memory.Allocator) (query.TableIterator, error) {
	return s.ReadSeriesCardinalityFn(ctx, spec, alloc)

}
func (s *StorageReader) SupportReadSeriesCardinality(ctx context.Context) bool {
	return s.SupportReadSeriesCardinalityFn(ctx)
}

func (s *StorageReader) Close() {
	// Only invoke the close function if it is set.
	// We want this to be a no-op and work without
	// explicitly setting up a close function.
	if s.CloseFn != nil {
		s.CloseFn()
	}
}

func (s *StorageReader) ReadWindowAggregate(ctx context.Context, spec query.ReadWindowAggregateSpec, alloc *memory.Allocator) (query.TableIterator, error) {
	return s.ReadWindowAggregateFn(ctx, spec, alloc)
}
