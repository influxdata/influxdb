package readservice

import (
	"context"
	"errors"
	"math"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/influxdata/influxql"
)

type store struct {
	engine *storage.Engine
}

func newStore(engine *storage.Engine) *store {
	return &store{engine: engine}
}

func (s *store) ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error) {
	if req.ReadSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := getReadSource(*req.ReadSource)
	if err != nil {
		return nil, err
	}

	var cur reads.SeriesCursor
	if ic, err := newIndexSeriesCursor(ctx, &source, req.Predicate, s.engine); err != nil {
		return nil, err
	} else if ic == nil {
		return nil, nil
	} else {
		cur = ic
	}

	return reads.NewResultSetFromFilter(ctx, req, cur), nil
}

func (s *store) Read(ctx context.Context, req *datatypes.ReadRequest) (reads.ResultSet, error) {
	if len(req.GroupKeys) > 0 {
		panic("Read: len(Grouping) > 0")
	}

	if req.Hints.NoPoints() {
		req.PointsLimit = -1
	}

	if req.PointsLimit == 0 {
		req.PointsLimit = math.MaxInt64
	}

	if req.ReadSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := getReadSource(*req.ReadSource)
	if err != nil {
		return nil, err
	}

	if req.TimestampRange.Start == 0 {
		req.TimestampRange.Start = math.MaxInt64
	}

	if req.TimestampRange.End == 0 {
		req.TimestampRange.End = math.MaxInt64
	}

	var cur reads.SeriesCursor
	if ic, err := newIndexSeriesCursor(ctx, &source, req.Predicate, s.engine); err != nil {
		return nil, err
	} else if ic == nil {
		return nil, nil
	} else {
		cur = ic
	}

	if req.SeriesLimit > 0 || req.SeriesOffset > 0 {
		cur = reads.NewLimitSeriesCursor(ctx, cur, req.SeriesLimit, req.SeriesOffset)
	}

	return reads.NewResultSet(ctx, req, cur), nil
}

func (s *store) GroupRead(ctx context.Context, req *datatypes.ReadRequest) (reads.GroupResultSet, error) {
	if req.SeriesLimit > 0 || req.SeriesOffset > 0 {
		return nil, errors.New("groupRead: SeriesLimit and SeriesOffset not supported when Grouping")
	}

	if req.Hints.NoPoints() {
		req.PointsLimit = -1
	}

	if req.PointsLimit == 0 {
		req.PointsLimit = math.MaxInt64
	}

	if req.ReadSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := getReadSource(*req.ReadSource)
	if err != nil {
		return nil, err
	}

	if req.TimestampRange.Start <= 0 {
		req.TimestampRange.Start = math.MinInt64
	}

	if req.TimestampRange.End <= 0 {
		req.TimestampRange.End = math.MaxInt64
	}

	newCursor := func() (reads.SeriesCursor, error) {
		cur, err := newIndexSeriesCursor(ctx, &source, req.Predicate, s.engine)
		if cur == nil || err != nil {
			return nil, err
		}
		return cur, nil
	}

	return reads.NewGroupResultSet(ctx, req, newCursor), nil
}

func (s *store) TagKeys(ctx context.Context, req *datatypes.TagKeysRequest) (cursors.StringIterator, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if req.TagsSource == nil {
		return nil, errors.New("missing tags source")
	}

	if req.Range.Start == 0 {
		req.Range.Start = models.MinNanoTime
	}
	if req.Range.End == 0 {
		req.Range.End = models.MaxNanoTime
	}

	var expr influxql.Expr
	var err error
	if root := req.Predicate.GetRoot(); root != nil {
		expr, err = reads.NodeToExpr(root, nil)
		if err != nil {
			return nil, err
		}

		if found := reads.HasFieldValueKey(expr); found {
			return nil, errors.New("field values unsupported")
		}
		expr = influxql.Reduce(influxql.CloneExpr(expr), nil)
		if reads.IsTrueBooleanLiteral(expr) {
			expr = nil
		}
	}

	readSource, err := getReadSource(*req.TagsSource)
	if err != nil {
		return nil, err
	}
	return s.engine.TagKeys(ctx, influxdb.ID(readSource.OrganizationID), influxdb.ID(readSource.BucketID), req.Range.Start, req.Range.End, expr)
}

func (s *store) TagValues(ctx context.Context, req *datatypes.TagValuesRequest) (cursors.StringIterator, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if req.TagsSource == nil {
		return nil, errors.New("missing tags source")
	}

	if req.Range.Start == 0 {
		req.Range.Start = models.MinNanoTime
	}
	if req.Range.End == 0 {
		req.Range.End = models.MaxNanoTime
	}

	if req.TagKey == "" {
		return nil, errors.New("missing tag key")
	}

	var expr influxql.Expr
	var err error
	if root := req.Predicate.GetRoot(); root != nil {
		expr, err = reads.NodeToExpr(root, nil)
		if err != nil {
			return nil, err
		}

		if found := reads.HasFieldValueKey(expr); found {
			return nil, errors.New("field values unsupported")
		}
		expr = influxql.Reduce(influxql.CloneExpr(expr), nil)
		if reads.IsTrueBooleanLiteral(expr) {
			expr = nil
		}
	}

	readSource, err := getReadSource(*req.TagsSource)
	if err != nil {
		return nil, err
	}
	return s.engine.TagValues(ctx, influxdb.ID(readSource.OrganizationID), influxdb.ID(readSource.BucketID), req.TagKey, req.Range.Start, req.Range.End, expr)
}

// this is easier than fooling around with .proto files.

type readSource struct {
	BucketID       uint64 `protobuf:"varint,1,opt,name=bucket_id,proto3"`
	OrganizationID uint64 `protobuf:"varint,2,opt,name=organization_id,proto3"`
}

func (r *readSource) XXX_MessageName() string { return "readSource" }
func (r *readSource) Reset()                  { *r = readSource{} }
func (r *readSource) String() string          { return "readSource{}" }
func (r *readSource) ProtoMessage()           {}

func (s *store) GetSource(orgID, bucketID uint64) proto.Message {
	return &readSource{
		BucketID:       bucketID,
		OrganizationID: orgID,
	}
}

func getReadSource(any types.Any) (readSource, error) {
	var source readSource
	if err := types.UnmarshalAny(&any, &source); err != nil {
		return source, err
	}
	return source, nil
}
