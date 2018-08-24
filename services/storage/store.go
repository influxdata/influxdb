package storage

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

type ResultSet interface {
	Close()
	Next() bool
	Cursor() tsdb.Cursor
	Tags() models.Tags
}

type GroupResultSet interface {
	Next() GroupCursor
	Close()
}

type GroupCursor interface {
	Tags() models.Tags
	Keys() [][]byte
	PartitionKeyVals() [][]byte
	Next() bool
	Cursor() tsdb.Cursor
	Close()
}

type Store interface {
	Read(ctx context.Context, req *ReadRequest) (ResultSet, error)
	GroupRead(ctx context.Context, req *ReadRequest) (GroupResultSet, error)
	WithLogger(log *zap.Logger)
}

func getReadSource(req *ReadRequest) (*ReadSource, error) {
	if req.ReadSource == nil {
		return nil, ErrMissingReadSource
	}

	var source ReadSource
	if err := types.UnmarshalAny(req.ReadSource, &source); err != nil {
		return nil, err
	}
	return &source, nil
}

type localStore struct {
	TSDBStore  *tsdb.Store
	MetaClient MetaClient
	Logger     *zap.Logger
}

func NewStore(store *tsdb.Store, metaClient MetaClient) Store {
	return &localStore{
		TSDBStore:  store,
		MetaClient: metaClient,
		Logger:     zap.NewNop(),
	}
}

// WithLogger sets the logger for the service.
func (s *localStore) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "store"))
}

func (s *localStore) findShardIDs(database, rp string, desc bool, start, end int64) ([]uint64, error) {
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

func (s *localStore) validateArgs(database, rp string, start, end int64) (string, string, int64, int64, error) {
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

func (s *localStore) Read(ctx context.Context, req *ReadRequest) (ResultSet, error) {
	if len(req.GroupKeys) > 0 {
		panic("Read: len(Grouping) > 0")
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
		return (*resultSet)(nil), nil
	}

	var cur SeriesCursor
	if ic, err := newIndexSeriesCursor(ctx, req.Predicate, s.TSDBStore.Shards(shardIDs)); err != nil {
		return nil, err
	} else if ic == nil {
		return (*resultSet)(nil), nil
	} else {
		cur = ic
	}

	if req.SeriesLimit > 0 || req.SeriesOffset > 0 {
		cur = NewLimitSeriesCursor(ctx, cur, req.SeriesLimit, req.SeriesOffset)
	}

	req.TimestampRange.Start = start
	req.TimestampRange.End = end

	return NewResultSet(ctx, req, cur), nil
}

func (s *localStore) GroupRead(ctx context.Context, req *ReadRequest) (GroupResultSet, error) {
	if req.SeriesLimit > 0 || req.SeriesOffset > 0 {
		return nil, errors.New("GroupRead: SeriesLimit and SeriesOffset not supported when Grouping")
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

	newCursor := func() (SeriesCursor, error) {
		cur, err := newIndexSeriesCursor(ctx, req.Predicate, shards)
		if cur == nil || err != nil {
			return nil, err
		}
		return cur, nil
	}

	rs := NewGroupResultSet(ctx, req, newCursor)
	if rs == nil {
		return nil, nil
	}

	return rs, nil
}
