package readservice

import (
	"context"
	"errors"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/influxdata/influxql"
)

type store struct {
	viewer reads.Viewer
}

// NewStore creates a store used to query time-series data.
func NewStore(viewer reads.Viewer) reads.Store {
	return &store{viewer: viewer}
}

func (s *store) ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if req.ReadSource == nil {
		return nil, tracing.LogError(span, errors.New("missing read source"))
	}

	orgID, bucketID, err := getOrgIDAndBucketID(*req.ReadSource)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}

	var seriesCursor reads.SeriesCursor
	if seriesCursor, err = reads.NewIndexSeriesCursor(ctx, orgID, bucketID, req.Predicate, s.viewer); err != nil {
		return nil, tracing.LogError(span, err)
	} else if seriesCursor == nil {
		return nil, nil
	}

	return reads.NewFilteredResultSet(ctx, req, seriesCursor), nil
}

func (s *store) ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if req.ReadSource == nil {
		return nil, tracing.LogError(span, errors.New("missing read source"))
	}

	orgID, bucketID, err := getOrgIDAndBucketID(*req.ReadSource)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}

	newCursor := func() (reads.SeriesCursor, error) {
		return reads.NewIndexSeriesCursor(ctx, orgID, bucketID, req.Predicate, s.viewer)
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

	orgID, bucketID, err := getOrgIDAndBucketID(*req.TagsSource)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}
	return s.viewer.TagKeys(ctx, orgID, bucketID, req.Range.Start, req.Range.End, expr)
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

	orgID, bucketID, err := getOrgIDAndBucketID(*req.TagsSource)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}
	return s.viewer.TagValues(ctx, orgID, bucketID, req.TagKey, req.Range.Start, req.Range.End, expr)
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

func getOrgIDAndBucketID(any types.Any) (orgID, bucketID influxdb.ID, err error) {
	var source readSource
	if err = types.UnmarshalAny(&any, &source); err != nil {
		return 0, 0, err
	}
	orgID, bucketID = influxdb.ID(source.OrganizationID), influxdb.ID(source.BucketID)
	return orgID, bucketID, nil
}
