package mock

import (
	"context"

	"github.com/influxdata/influxdb/v2/storage/reads"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
	"google.golang.org/protobuf/proto"
)

type ReadsStore struct {
	ReadFilterFn                   func(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error)
	ReadGroupFn                    func(ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error)
	WindowAggregateFn              func(ctx context.Context, req *datatypes.ReadWindowAggregateRequest) (reads.ResultSet, error)
	TagKeysFn                      func(ctx context.Context, req *datatypes.TagKeysRequest) (cursors.StringIterator, error)
	TagValuesFn                    func(ctx context.Context, req *datatypes.TagValuesRequest) (cursors.StringIterator, error)
	ReadSeriesCardinalityFn        func(ctx context.Context, req *datatypes.ReadSeriesCardinalityRequest) (cursors.Int64Iterator, error)
	SupportReadSeriesCardinalityFn func(ctx context.Context) bool
	GetSourceFn                    func(orgID, bucketID uint64) proto.Message
}

func (s *ReadsStore) ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error) {
	return s.ReadFilterFn(ctx, req)
}

func (s *ReadsStore) ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error) {
	return s.ReadGroupFn(ctx, req)
}

func (s *ReadsStore) WindowAggregate(ctx context.Context, req *datatypes.ReadWindowAggregateRequest) (reads.ResultSet, error) {
	return s.WindowAggregateFn(ctx, req)
}

func (s *ReadsStore) TagKeys(ctx context.Context, req *datatypes.TagKeysRequest) (cursors.StringIterator, error) {
	return s.TagKeysFn(ctx, req)
}

func (s *ReadsStore) TagValues(ctx context.Context, req *datatypes.TagValuesRequest) (cursors.StringIterator, error) {
	return s.TagValuesFn(ctx, req)
}

func (s *ReadsStore) ReadSeriesCardinality(ctx context.Context, req *datatypes.ReadSeriesCardinalityRequest) (cursors.Int64Iterator, error) {
	return s.ReadSeriesCardinalityFn(ctx, req)
}

func (s *ReadsStore) SupportReadSeriesCardinality(ctx context.Context) bool {
	return s.SupportReadSeriesCardinalityFn(ctx)
}

func (s *ReadsStore) GetSource(orgID, bucketID uint64) proto.Message {
	return s.GetSourceFn(orgID, bucketID)
}
