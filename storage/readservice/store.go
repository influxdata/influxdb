package readservice

import (
	"context"
	"errors"
	"math"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/influxdata/platform/models"
	fstorage "github.com/influxdata/platform/query/functions/inputs/storage"
	"github.com/influxdata/platform/storage"
	"github.com/influxdata/platform/storage/reads"
	"github.com/influxdata/platform/storage/reads/datatypes"
)

type store struct {
	engine *storage.Engine
}

func newStore(engine *storage.Engine) *store {
	return &store{engine: engine}
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

	source, err := getReadSource(req)
	if err != nil {
		return nil, err
	}

	if req.TimestampRange.Start == 0 {
		req.TimestampRange.Start = models.MinNanoTime
	}

	if req.TimestampRange.End == 0 {
		req.TimestampRange.End = models.MaxNanoTime
	}

	var cur reads.SeriesCursor
	if ic, err := newIndexSeriesCursor(ctx, source, req, s.engine); err != nil {
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
		return nil, errors.New("GroupRead: SeriesLimit and SeriesOffset not supported when Grouping")
	}

	if req.Hints.NoPoints() {
		req.PointsLimit = -1
	}

	if req.PointsLimit == 0 {
		req.PointsLimit = math.MaxInt64
	}

	source, err := getReadSource(req)
	if err != nil {
		return nil, err
	}

	if req.TimestampRange.Start <= 0 {
		req.TimestampRange.Start = models.MinNanoTime
	}

	if req.TimestampRange.End <= 0 {
		req.TimestampRange.End = models.MaxNanoTime
	}

	newCursor := func() (reads.SeriesCursor, error) {
		cur, err := newIndexSeriesCursor(ctx, source, req, s.engine)
		if cur == nil || err != nil {
			return nil, err
		}
		return cur, nil
	}

	return reads.NewGroupResultSet(ctx, req, newCursor), nil
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

func (s *store) GetSource(rs fstorage.ReadSpec) (proto.Message, error) {
	return &readSource{
		BucketID:       uint64(rs.BucketID),
		OrganizationID: uint64(rs.OrganizationID),
	}, nil
}

func getReadSource(req *datatypes.ReadRequest) (*readSource, error) {
	if req.ReadSource == nil {
		return nil, errors.New("missing read source")
	}

	var source readSource
	if err := types.UnmarshalAny(req.ReadSource, &source); err != nil {
		return nil, err
	}
	return &source, nil
}
