package storage

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

type Store struct {
	TSDBStore  *tsdb.Store
	MetaClient StorageMetaClient
	Logger     *zap.Logger
}

func NewStore() *Store {
	return &Store{Logger: zap.NewNop()}
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

func (s *Store) validateArgs(database string, start, end int64) (string, string, int64, int64, error) {
	rp := ""
	if p := strings.IndexByte(database, '/'); p > -1 {
		database, rp = database[:p], database[p+1:]
	}

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

type Results interface {
	Close()
	Next() bool
	Cursor() tsdb.Cursor
	Tags() models.Tags
}

func (s *Store) Read(ctx context.Context, req *ReadRequest) (Results, error) {
	if len(req.GroupKeys) > 0 {
		panic("Read: len(Grouping) > 0")
	}

	database, rp, start, end, err := s.validateArgs(req.Database, req.TimestampRange.Start, req.TimestampRange.End)
	if err != nil {
		return nil, err
	}

	shardIDs, err := s.findShardIDs(database, rp, req.Descending, start, end)
	if err != nil {
		return nil, err
	}
	if len(shardIDs) == 0 {
		return (*ResultSet)(nil), nil
	}

	var cur seriesCursor
	if ic, err := newIndexSeriesCursor(ctx, req.Predicate, s.TSDBStore.Shards(shardIDs)); err != nil {
		return nil, err
	} else if ic == nil {
		return (*ResultSet)(nil), nil
	} else {
		cur = ic
	}

	if req.SeriesLimit > 0 || req.SeriesOffset > 0 {
		cur = newLimitSeriesCursor(ctx, cur, req.SeriesLimit, req.SeriesOffset)
	}

	rr := readRequest{
		ctx:       ctx,
		start:     start,
		end:       end,
		asc:       !req.Descending,
		limit:     req.PointsLimit,
		aggregate: req.Aggregate,
	}

	return &ResultSet{
		req: rr,
		cur: cur,
		mb:  newMultiShardBatchCursors(ctx, &rr),
	}, nil
}

func (s *Store) GroupRead(ctx context.Context, req *ReadRequest) (*groupResultSet, error) {
	if req.SeriesLimit > 0 || req.SeriesOffset > 0 {
		return nil, errors.New("GroupRead: SeriesLimit and SeriesOffset not supported when Grouping")
	}

	database, rp, start, end, err := s.validateArgs(req.Database, req.TimestampRange.Start, req.TimestampRange.End)
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

	newCursor := func() (seriesCursor, error) {
		return newIndexSeriesCursor(ctx, req.Predicate, shards)
	}

	return newGroupResultSet(ctx, req, newCursor), nil
}
