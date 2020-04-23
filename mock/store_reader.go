package mock

import (
	"context"

	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb/v2/query"
)

type StoreReader struct {
	ReadFilterFn    func(ctx context.Context, spec query.ReadFilterSpec, alloc *memory.Allocator) (query.TableIterator, error)
	ReadGroupFn     func(ctx context.Context, spec query.ReadGroupSpec, alloc *memory.Allocator) (query.TableIterator, error)
	ReadTagKeysFn   func(ctx context.Context, spec query.ReadTagKeysSpec, alloc *memory.Allocator) (query.TableIterator, error)
	ReadTagValuesFn func(ctx context.Context, spec query.ReadTagValuesSpec, alloc *memory.Allocator) (query.TableIterator, error)
	CloseFn         func()
}

func (s *StoreReader) ReadFilter(ctx context.Context, spec query.ReadFilterSpec, alloc *memory.Allocator) (query.TableIterator, error) {
	return s.ReadFilterFn(ctx, spec, alloc)
}

func (s *StoreReader) ReadGroup(ctx context.Context, spec query.ReadGroupSpec, alloc *memory.Allocator) (query.TableIterator, error) {
	return s.ReadGroupFn(ctx, spec, alloc)
}

func (s *StoreReader) ReadTagKeys(ctx context.Context, spec query.ReadTagKeysSpec, alloc *memory.Allocator) (query.TableIterator, error) {
	return s.ReadTagKeysFn(ctx, spec, alloc)
}

func (s *StoreReader) ReadTagValues(ctx context.Context, spec query.ReadTagValuesSpec, alloc *memory.Allocator) (query.TableIterator, error) {
	return s.ReadTagValuesFn(ctx, spec, alloc)
}

func (s *StoreReader) Close() {
	// Only invoke the close function if it is set.
	// We want this to be a no-op and work without
	// explicitly setting up a close function.
	if s.CloseFn != nil {
		s.CloseFn()
	}
}

type WindowAggregateStoreReader struct {
	*StoreReader
	HasWindowAggregateCapabilityFn func(ctx context.Context) bool
	ReadWindowAggregateFn          func(ctx context.Context, spec query.ReadWindowAggregateSpec, alloc *memory.Allocator) (query.TableIterator, error)
}

func (s *WindowAggregateStoreReader) HasWindowAggregateCapability(ctx context.Context) bool {
	// Use the function if it exists.
	if s.HasWindowAggregateCapabilityFn != nil {
		return s.HasWindowAggregateCapabilityFn(ctx)
	}

	// Provide a default implementation if one wasn't set.
	// This will return true if the other function was set.
	return s.ReadWindowAggregateFn != nil
}

func (s *WindowAggregateStoreReader) ReadWindowAggregate(ctx context.Context, spec query.ReadWindowAggregateSpec, alloc *memory.Allocator) (query.TableIterator, error) {
	return s.ReadWindowAggregateFn(ctx, spec, alloc)
}
