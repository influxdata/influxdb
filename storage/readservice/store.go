package readservice

import (
	"context"
	"errors"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/storage/reads"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
	"github.com/influxdata/influxql"
)

type store struct {
	viewer reads.Viewer
	cap    WindowAggregateCapability
}

// NewStore creates a store used to query time-series data.
func NewStore(viewer reads.Viewer) reads.Store {
	return &store{
		viewer: viewer,
		cap: WindowAggregateCapability{
			Count: true,
		},
	}
}

func (s *store) ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if req.ReadSource == nil {
		return nil, tracing.LogError(span, errors.New("missing read source"))
	}

	source, err := getReadSource(*req.ReadSource)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}

	var cur reads.SeriesCursor
	if cur, err = reads.NewIndexSeriesCursor(ctx, source.GetOrgID(), source.GetBucketID(), req.Predicate, s.viewer); err != nil {
		return nil, tracing.LogError(span, err)
	} else if cur == nil {
		return nil, nil
	}

	return reads.NewFilteredResultSet(ctx, req, cur), nil
}

func (s *store) ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if req.ReadSource == nil {
		return nil, tracing.LogError(span, errors.New("missing read source"))
	}

	source, err := getReadSource(*req.ReadSource)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}

	newCursor := func() (reads.SeriesCursor, error) {
		return reads.NewIndexSeriesCursor(ctx, source.GetOrgID(), source.GetBucketID(), req.Predicate, s.viewer)
	}

	return reads.NewGroupResultSet(ctx, req, newCursor), nil
}

func (s *store) TagKeys(ctx context.Context, req *datatypes.TagKeysRequest) (cursors.StringIterator, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if req.TagsSource == nil {
		return nil, tracing.LogError(span, errors.New("missing tags source"))
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
			return nil, tracing.LogError(span, err)
		}

		if found := reads.HasFieldValueKey(expr); found {
			return nil, tracing.LogError(span, errors.New("field values unsupported"))
		}
		expr = influxql.Reduce(influxql.CloneExpr(expr), nil)
		if reads.IsTrueBooleanLiteral(expr) {
			expr = nil
		}
	}

	readSource, err := getReadSource(*req.TagsSource)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}
	return s.viewer.TagKeys(ctx, readSource.GetOrgID(), readSource.GetBucketID(), req.Range.Start, req.Range.End, expr)
}

func (s *store) TagValues(ctx context.Context, req *datatypes.TagValuesRequest) (cursors.StringIterator, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if req.TagsSource == nil {
		return nil, tracing.LogError(span, errors.New("missing tags source"))
	}

	if req.Range.Start == 0 {
		req.Range.Start = models.MinNanoTime
	}
	if req.Range.End == 0 {
		req.Range.End = models.MaxNanoTime
	}

	if req.TagKey == "" {
		return nil, tracing.LogError(span, errors.New("missing tag key"))
	}

	var expr influxql.Expr
	var err error
	if root := req.Predicate.GetRoot(); root != nil {
		expr, err = reads.NodeToExpr(root, nil)
		if err != nil {
			return nil, tracing.LogError(span, err)
		}

		if found := reads.HasFieldValueKey(expr); found {
			return nil, tracing.LogError(span, errors.New("field values unsupported"))
		}
		expr = influxql.Reduce(influxql.CloneExpr(expr), nil)
		if reads.IsTrueBooleanLiteral(expr) {
			expr = nil
		}
	}

	readSource, err := getReadSource(*req.TagsSource)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}
	return s.viewer.TagValues(ctx, readSource.GetOrgID(), readSource.GetBucketID(), req.TagKey, req.Range.Start, req.Range.End, expr)
}

func (s *store) GetSource(orgID, bucketID uint64) proto.Message {
	return &readSource{
		BucketID:       bucketID,
		OrganizationID: orgID,
	}
}

func (s *store) GetWindowAggregateCapability(ctx context.Context) reads.WindowAggregateCapability {
	return s.cap
}

// WindowAggregate will invoke a ReadWindowAggregateRequest against the Store.
func (s *store) WindowAggregate(ctx context.Context, req *datatypes.ReadWindowAggregateRequest) (reads.ResultSet, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if req.ReadSource == nil {
		return nil, tracing.LogError(span, errors.New("missing read source"))
	}

	source, err := getReadSource(*req.ReadSource)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}

	var cur reads.SeriesCursor
	if cur, err = reads.NewIndexSeriesCursor(ctx, source.GetOrgID(), source.GetBucketID(), req.Predicate, s.viewer); err != nil {
		return nil, tracing.LogError(span, err)
	} else if cur == nil {
		return nil, nil
	}

	return reads.NewWindowAggregateResultSet(ctx, req, cur)
}

type WindowAggregateCapability struct {
	Min   bool
	Max   bool
	Mean  bool
	Count bool
	Sum   bool
}

func (w WindowAggregateCapability) HaveMin() bool   { return w.Min }
func (w WindowAggregateCapability) HaveMax() bool   { return w.Max }
func (w WindowAggregateCapability) HaveMean() bool  { return w.Mean }
func (w WindowAggregateCapability) HaveCount() bool { return w.Count }
func (w WindowAggregateCapability) HaveSum() bool   { return w.Sum }
