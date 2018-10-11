package storage

import (
	"context"
	"errors"
	"math"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/platform/query/functions/inputs/storage"
	"github.com/influxdata/platform/storage/reads"
	"github.com/influxdata/platform/storage/reads/datatypes"
	"go.uber.org/zap"
)

func getReadSource(req *datatypes.ReadRequest) (*ReadSource, error) {
	if req.ReadSource == nil {
		return nil, ErrMissingReadSource
	}

	var source ReadSource
	if err := types.UnmarshalAny(req.ReadSource, &source); err != nil {
		return nil, err
	}
	return &source, nil
}

type Store struct {
	TSDBStore  *tsdb.Store
	MetaClient MetaClient
	Logger     *zap.Logger
}

func NewStore(store *tsdb.Store, metaClient MetaClient) *Store {
	return &Store{
		TSDBStore:  store,
		MetaClient: metaClient,
		Logger:     zap.NewNop(),
	}
}

// WithLogger sets the logger for the service.
func (s *Store) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "store"))
}

func (s *Store) findShardIDs(database, rp string, desc bool, start, end int64) ([]uint64, error) {
	groups, err := s.MetaClient.ShardGroupsByTimeRange(database, rp, time.Unix(0, start), time.Unix(0, end))
	if err != nil {
		return nil, err
	}

	if len(groups) == 0 {
		return nil, nil
	}

	if desc {
		sort.Sort(sort.Reverse(meta.ShardGroupInfos(groups)))
	} else {
		sort.Sort(meta.ShardGroupInfos(groups))
	}

	shardIDs := make([]uint64, 0, len(groups[0].Shards)*len(groups))
	for _, g := range groups {
		for _, si := range g.Shards {
			shardIDs = append(shardIDs, si.ID)
		}
	}
	return shardIDs, nil
}

func (s *Store) validateArgs(database, rp string, start, end int64) (string, string, int64, int64, error) {
	di := s.MetaClient.Database(database)
	if di == nil {
		return "", "", 0, 0, errors.New("no database")
	}

	if rp == "" {
		rp = di.DefaultRetentionPolicy
	}

	rpi := di.RetentionPolicy(rp)
	if rpi == nil {
		return "", "", 0, 0, errors.New("invalid retention policy")
	}

	if start <= 0 {
		start = models.MinNanoTime
	}
	if end <= 0 {
		end = models.MaxNanoTime
	}
	return database, rp, start, end, nil
}

func (s *Store) Read(ctx context.Context, req *datatypes.ReadRequest) (reads.ResultSet, error) {
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

	database, rp, start, end, err := s.validateArgs(source.Database, source.RetentionPolicy, req.TimestampRange.Start, req.TimestampRange.End)
	if err != nil {
		return nil, err
	}

	shardIDs, err := s.findShardIDs(database, rp, req.Descending, start, end)
	if err != nil {
		return nil, err
	}
	if len(shardIDs) == 0 { // TODO(jeff): this was a typed nil
		return nil, nil
	}

	var cur reads.SeriesCursor
	if ic, err := newIndexSeriesCursor(ctx, req.Predicate, s.TSDBStore.Shards(shardIDs)); err != nil {
		return nil, err
	} else if ic == nil { // TODO(jeff): this was a typed nil
		return nil, nil
	} else {
		cur = ic
	}

	if req.SeriesLimit > 0 || req.SeriesOffset > 0 {
		cur = reads.NewLimitSeriesCursor(ctx, cur, req.SeriesLimit, req.SeriesOffset)
	}

	req.TimestampRange.Start = start
	req.TimestampRange.End = end

	return reads.NewResultSet(ctx, req, cur), nil
}

func (s *Store) GroupRead(ctx context.Context, req *datatypes.ReadRequest) (reads.GroupResultSet, error) {
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

	database, rp, start, end, err := s.validateArgs(source.Database, source.RetentionPolicy, req.TimestampRange.Start, req.TimestampRange.End)
	if err != nil {
		return nil, err
	}

	shardIDs, err := s.findShardIDs(database, rp, req.Descending, start, end)
	if err != nil {
		return nil, err
	}
	if len(shardIDs) == 0 {
		return nil, nil
	}

	shards := s.TSDBStore.Shards(shardIDs)

	req.TimestampRange.Start = start
	req.TimestampRange.End = end

	newCursor := func() (reads.SeriesCursor, error) {
		cur, err := newIndexSeriesCursor(ctx, req.Predicate, shards)
		if cur == nil || err != nil {
			return nil, err
		}
		return cur, nil
	}

	rs := reads.NewGroupResultSet(ctx, req, newCursor)
	if rs == nil {
		return nil, nil
	}

	return rs, nil
}

func (s *Store) GetSource(rs storage.ReadSpec) (proto.Message, error) {
	return &ReadSource{Database: rs.Database, RetentionPolicy: rs.RetentionPolicy}, nil
}
