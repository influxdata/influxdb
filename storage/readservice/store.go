package readservice

import (
	"context"
	"errors"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/influxdata/influxql"
)

// View is used by the store to query data from time-series files.
type View interface {
	CreateCursorIterator(ctx context.Context) (tsdb.CursorIterator, error)
	CreateSeriesCursor(ctx context.Context, req storage.SeriesCursorRequest, cond influxql.Expr) (storage.SeriesCursor, error)
	TagKeys(ctx context.Context, orgID, bucketID influxdb.ID, start, end int64, predicate influxql.Expr) (cursors.StringIterator, error)
	TagValues(ctx context.Context, orgID, bucketID influxdb.ID, tagKey string, start, end int64, predicate influxql.Expr) (cursors.StringIterator, error)
}

type store struct {
	view View
}

// NewStore creates a store used to query time-series data.
func NewStore(view View) reads.Store {
	return &store{view: view}
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
	if ic, err := newIndexSeriesCursor(ctx, &source, req.Predicate, s.view); err != nil {
		return nil, err
	} else if ic == nil {
		return nil, nil
	} else {
		cur = ic
	}

	return reads.NewFilteredResultSet(ctx, req, cur), nil
}

func (s *store) ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error) {
	if req.ReadSource == nil {
		return nil, errors.New("missing read source")
	}

	source, err := getReadSource(*req.ReadSource)
	if err != nil {
		return nil, err
	}

	newCursor := func() (reads.SeriesCursor, error) {
		cur, err := newIndexSeriesCursor(ctx, &source, req.Predicate, s.view)
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
	return s.view.TagKeys(ctx, influxdb.ID(readSource.OrganizationID), influxdb.ID(readSource.BucketID), req.Range.Start, req.Range.End, expr)
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
	return s.view.TagValues(ctx, influxdb.ID(readSource.OrganizationID), influxdb.ID(readSource.BucketID), req.TagKey, req.Range.Start, req.Range.End, expr)
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
