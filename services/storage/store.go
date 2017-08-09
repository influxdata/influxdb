package storage

import (
	"time"

	"errors"
	"strings"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/uber-go/zap"
)

type Store struct {
	TSDBStore *tsdb.Store

	MetaClient interface {
		Database(name string) *meta.DatabaseInfo
		ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	}

	Logger zap.Logger
}

func NewStore() *Store {
	return &Store{Logger: zap.New(zap.NullEncoder())}
}

// WithLogger sets the logger for the service.
func (s *Store) WithLogger(log zap.Logger) {
	s.Logger = log.With(zap.String("service", "store"))
}

func (s *Store) Read(req ReadRequest) (*ResultSet, error) {
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

	var start, end int64 = models.MinNanoTime, models.MaxNanoTime
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

	shardIDs := make([]uint64, 0, len(groups[0].Shards)*len(groups))
	for _, g := range groups {
		for _, si := range g.Shards {
			shardIDs = append(shardIDs, si.ID)
		}
	}

	qp, err := newAllMeasurementsPlanner(req, s.TSDBStore.Shards(shardIDs), s.Logger)
	if err != nil {
		return nil, err
	}

	return &ResultSet{p: qp, start: start, end: end, asc: !req.Descending}, nil
}

type ResultSet struct {
	p          planner
	start, end int64
	asc        bool

	shards    []*tsdb.Shard
	m, key, f string
	shard     *tsdb.Shard
}

func (r *ResultSet) Next() bool {
	if len(r.shards) == 0 {
		if ok := r.p.Next(); !ok {
			return false
		}

		r.m, r.key, r.f, r.shards = r.p.Read()
	}

	r.shard, r.shards = r.shards[0], r.shards[1:]
	return true
}

func (r *ResultSet) Cursor() tsdb.Cursor {
	req := tsdb.CursorRequest{Measurement: r.m, Series: r.key, Field: r.f, Ascending: r.asc, StartTime: r.start, EndTime: r.end}
	c, _ := r.shard.CreateCursor(req)
	return c
}
