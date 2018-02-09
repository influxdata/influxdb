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

func (s *Store) Read(ctx context.Context, req *ReadRequest) (*ResultSet, error) {
	database, rp := req.Database, ""

	if p := strings.IndexByte(database, '/'); p > -1 {
		database, rp = database[:p], database[p+1:]
	}

	di := s.MetaClient.Database(database)
	if di == nil {
		return nil, errors.New("no database")
	}

	if rp == "" {
		rp = di.DefaultRetentionPolicy
	}

	rpi := di.RetentionPolicy(rp)
	if rpi == nil {
		return nil, errors.New("invalid retention policy")
	}

	var start, end = models.MinNanoTime, models.MaxNanoTime
	if req.TimestampRange.Start > 0 {
		start = req.TimestampRange.Start
	}

	if req.TimestampRange.End > 0 {
		end = req.TimestampRange.End
	}

	groups, err := s.MetaClient.ShardGroupsByTimeRange(database, rp, time.Unix(0, start), time.Unix(0, end))
	if err != nil {
		return nil, err
	}

	if len(groups) == 0 {
		return nil, nil
	}

	if req.Descending {
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

	var cur seriesCursor
	if ic, err := newIndexSeriesCursor(ctx, req, s.TSDBStore.Shards(shardIDs)); err != nil {
		return nil, err
	} else if ic == nil {
		return nil, nil
	} else {
		cur = ic
	}

	if len(req.Grouping) > 0 {
		cur = newGroupSeriesCursor(ctx, cur, req.Grouping)
	}

	if req.SeriesLimit > 0 || req.SeriesOffset > 0 {
		cur = newLimitSeriesCursor(ctx, cur, req.SeriesLimit, req.SeriesOffset)
	}

	return &ResultSet{
		req: readRequest{
			ctx:       ctx,
			start:     start,
			end:       end,
			asc:       !req.Descending,
			limit:     req.PointsLimit,
			aggregate: req.Aggregate,
		},
		cur: cur,
	}, nil
}
