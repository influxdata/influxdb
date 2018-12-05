package export

import (
	"context"
	"fmt"
	"io"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb/cmd/influx_tools/internal/format"
	"github.com/influxdata/influxdb/cmd/influx_tools/internal/storage"
	"github.com/influxdata/influxdb/cmd/influx_tools/server"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
)

type exporterConfig struct {
	Database      string
	RP            string
	ShardDuration time.Duration
	Min, Max      uint64
}

type exporter struct {
	metaClient server.MetaClient
	tsdbStore  *tsdb.Store
	store      *storage.Store

	min, max     uint64
	db, rp       string
	d            time.Duration
	sourceGroups []meta.ShardGroupInfo
	targetGroups []meta.ShardGroupInfo

	// source data time range
	startDate time.Time
	endDate   time.Time
}

func newExporter(server server.Interface, cfg *exporterConfig) (*exporter, error) {
	client := server.MetaClient()

	dbi := client.Database(cfg.Database)
	if dbi == nil {
		return nil, fmt.Errorf("database '%s' does not exist", cfg.Database)
	}

	if cfg.RP == "" {
		// select default RP
		cfg.RP = dbi.DefaultRetentionPolicy
	}

	rpi, err := client.RetentionPolicy(cfg.Database, cfg.RP)
	if rpi == nil || err != nil {
		return nil, fmt.Errorf("retention policy '%s' does not exist", cfg.RP)
	}

	store := tsdb.NewStore(server.TSDBConfig().Dir)
	if server.Logger() != nil {
		store.WithLogger(server.Logger())
	}
	store.EngineOptions.MonitorDisabled = true
	store.EngineOptions.CompactionDisabled = true
	store.EngineOptions.Config = server.TSDBConfig()
	store.EngineOptions.EngineVersion = server.TSDBConfig().Engine
	store.EngineOptions.IndexVersion = server.TSDBConfig().Index
	store.EngineOptions.DatabaseFilter = func(database string) bool {
		return database == cfg.Database
	}
	store.EngineOptions.RetentionPolicyFilter = func(_, rp string) bool {
		return rp == cfg.RP
	}
	store.EngineOptions.ShardFilter = func(_, _ string, _ uint64) bool {
		return false
	}

	return &exporter{
		metaClient: client,
		tsdbStore:  store,
		store:      &storage.Store{TSDBStore: store},
		min:        cfg.Min,
		max:        cfg.Max,
		db:         cfg.Database,
		rp:         cfg.RP,
		d:          cfg.ShardDuration,
	}, nil
}

func (e *exporter) Open() (err error) {
	err = e.tsdbStore.Open()
	if err != nil {
		return err
	}

	err = e.loadShardGroups()
	if err != nil {
		return err
	}

	e.targetGroups = planShardGroups(e.sourceGroups, e.startDate, e.endDate, e.d)
	if e.max >= uint64(len(e.targetGroups)) {
		e.max = uint64(len(e.targetGroups) - 1)
	}
	if e.min > e.max {
		return fmt.Errorf("invalid shard group range %d to %d", e.min, e.max)
	}

	e.targetGroups = e.targetGroups[e.min : e.max+1]

	return nil
}

func (e *exporter) PrintPlan(w io.Writer) {
	fmt.Fprintf(w, "Source data from: %s -> %s\n\n", e.startDate, e.endDate)
	fmt.Fprintf(w, "Converting source from %d shard group(s) to %d shard groups:\n\n", len(e.sourceGroups), len(e.targetGroups))
	e.printShardGroups(w, 0, e.sourceGroups)
	fmt.Fprintln(w)
	e.printShardGroups(w, int(e.min), e.targetGroups)
}

func (e *exporter) printShardGroups(w io.Writer, base int, target []meta.ShardGroupInfo) {
	tw := tabwriter.NewWriter(w, 10, 8, 1, '\t', 0)
	fmt.Fprintln(tw, "Seq #\tID\tStart\tEnd")
	for i := 0; i < len(target); i++ {
		g := target[i]
		fmt.Fprintf(tw, "%d\t%d\t%s\t%s\n", i+base, g.ID, g.StartTime, g.EndTime)
	}
	tw.Flush()
}

func (e *exporter) SourceTimeRange() (time.Time, time.Time)  { return e.startDate, e.endDate }
func (e *exporter) SourceShardGroups() []meta.ShardGroupInfo { return e.sourceGroups }
func (e *exporter) TargetShardGroups() []meta.ShardGroupInfo { return e.targetGroups }

func (e *exporter) loadShardGroups() error {
	min := time.Unix(0, models.MinNanoTime)
	max := time.Unix(0, models.MaxNanoTime)

	groups, err := e.metaClient.NodeShardGroupsByTimeRange(e.db, e.rp, min, max)
	if err != nil {
		return err
	}

	if len(groups) == 0 {
		return nil
	}

	sort.Sort(meta.ShardGroupInfos(groups))
	e.sourceGroups = groups
	e.startDate = groups[0].StartTime
	e.endDate = groups[len(groups)-1].EndTime

	return nil
}

func (e *exporter) shardsGroupsByTimeRange(min, max time.Time) []meta.ShardGroupInfo {
	groups := make([]meta.ShardGroupInfo, 0, len(e.sourceGroups))
	for _, g := range e.sourceGroups {
		if !g.Overlaps(min, max) {
			continue
		}
		groups = append(groups, g)
	}
	return groups
}

func (e *exporter) WriteTo(w format.Writer) error {
	for _, g := range e.targetGroups {
		min, max := g.StartTime, g.EndTime
		rs, err := e.read(min, max.Add(-1))
		if err != nil || rs == nil {
			return err
		}

		format.WriteBucket(w, min.UnixNano(), max.UnixNano(), rs)
		rs.Close()
	}
	return nil
}

// Read creates a ResultSet that reads all points with a timestamp ts, such that start ≤ ts < end.
func (e *exporter) read(min, max time.Time) (*storage.ResultSet, error) {
	shards, err := e.getShards(min, max)
	if err != nil {
		return nil, err
	}

	req := storage.ReadRequest{
		Database: e.db,
		RP:       e.rp,
		Shards:   shards,
		Start:    min.UnixNano(),
		End:      max.UnixNano(),
	}

	return e.store.Read(context.Background(), &req)
}

func (e *exporter) Close() error {
	return e.tsdbStore.Close()
}

func (e *exporter) getShards(min, max time.Time) ([]*tsdb.Shard, error) {
	groups := e.shardsGroupsByTimeRange(min, max)
	var ids []uint64
	for _, g := range groups {
		for _, s := range g.Shards {
			ids = append(ids, s.ID)
		}
	}

	shards := e.tsdbStore.Shards(ids)
	if len(shards) == len(ids) {
		return shards, nil
	}

	return e.openStoreWithShardsIDs(ids)
}

func (e *exporter) openStoreWithShardsIDs(ids []uint64) ([]*tsdb.Shard, error) {
	e.tsdbStore.Close()
	e.tsdbStore.EngineOptions.ShardFilter = func(_, _ string, id uint64) bool {
		for i := range ids {
			if id == ids[i] {
				return true
			}
		}
		return false
	}
	if err := e.tsdbStore.Open(); err != nil {
		return nil, err
	}
	return e.tsdbStore.Shards(ids), nil
}
