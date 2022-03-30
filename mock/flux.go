package mock

import (
	"context"

	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/flux/stdlib/influxdata/influxdb"
)

// Reader is a mock implementation of flux/stdlib/influxdata/influxdb.Reader
type Reader struct {
	ReadFilterFn          func(ctx context.Context, spec influxdb.ReadFilterSpec, alloc memory.Allocator) (influxdb.TableIterator, error)
	ReadGroupFn           func(ctx context.Context, spec influxdb.ReadGroupSpec, alloc memory.Allocator) (influxdb.TableIterator, error)
	ReadTagKeysFn         func(ctx context.Context, spec influxdb.ReadTagKeysSpec, alloc memory.Allocator) (influxdb.TableIterator, error)
	ReadTagValuesFn       func(ctx context.Context, spec influxdb.ReadTagValuesSpec, alloc memory.Allocator) (influxdb.TableIterator, error)
	ReadWindowAggregateFn func(ctx context.Context, spec influxdb.ReadWindowAggregateSpec, alloc memory.Allocator) (influxdb.TableIterator, error)
	CloseFn               func()
}

func (m Reader) ReadFilter(ctx context.Context, spec influxdb.ReadFilterSpec, alloc memory.Allocator) (influxdb.TableIterator, error) {
	return m.ReadFilterFn(ctx, spec, alloc)
}

func (m Reader) ReadGroup(ctx context.Context, spec influxdb.ReadGroupSpec, alloc memory.Allocator) (influxdb.TableIterator, error) {
	return m.ReadGroupFn(ctx, spec, alloc)
}

func (m Reader) ReadWindowAggregate(ctx context.Context, spec influxdb.ReadWindowAggregateSpec, alloc memory.Allocator) (influxdb.TableIterator, error) {
	return m.ReadWindowAggregateFn(ctx, spec, alloc)
}

func (m Reader) ReadTagKeys(ctx context.Context, spec influxdb.ReadTagKeysSpec, alloc memory.Allocator) (influxdb.TableIterator, error) {
	return m.ReadTagKeysFn(ctx, spec, alloc)
}

func (m Reader) ReadTagValues(ctx context.Context, spec influxdb.ReadTagValuesSpec, alloc memory.Allocator) (influxdb.TableIterator, error) {
	return m.ReadTagValuesFn(ctx, spec, alloc)
}

func (m Reader) Close() {
	m.CloseFn()
}

type Writer struct {
}

func (w *Writer) WritePointsInto(request *coordinator.IntoWriteRequest) error {
	panic("not implemented")
}
