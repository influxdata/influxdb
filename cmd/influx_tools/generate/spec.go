package generate

import (
	"flag"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/cmd/influx_tools/server"
)

type TagCardinalities []int

func (t TagCardinalities) String() string {
	s := make([]string, 0, len(t))
	for i := 0; i < len(t); i++ {
		s = append(s, strconv.Itoa(t[i]))
	}
	return fmt.Sprintf("[%s]", strings.Join(s, ","))
}

func (t TagCardinalities) Cardinality() int {
	n := 1
	for i := range t {
		n *= t[i]
	}
	return n
}

func (t *TagCardinalities) Set(tags string) error {
	*t = (*t)[:0]
	for _, s := range strings.Split(tags, ",") {
		v, err := strconv.Atoi(s)
		if err != nil {
			return fmt.Errorf("cannot parse tag cardinality: %s", s)
		}
		*t = append(*t, v)
	}
	return nil
}

type StorageSpec struct {
	StartTime     string
	Database      string
	Retention     string
	ReplicaN      int
	ShardCount    int
	ShardDuration time.Duration
}

func (a *StorageSpec) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&a.StartTime, "start-time", "", "Start time")
	fs.StringVar(&a.Database, "db", "db", "Name of database to create")
	fs.StringVar(&a.Retention, "rp", "rp", "Name of retention policy")
	fs.IntVar(&a.ReplicaN, "rf", 1, "Replication factor")
	fs.IntVar(&a.ShardCount, "shards", 1, "Number of shards to create")
	fs.DurationVar(&a.ShardDuration, "shard-duration", 24*time.Hour, "Shard duration (default 24h)")
}

func (a *StorageSpec) Plan(server server.Interface) (*StoragePlan, error) {
	plan := &StoragePlan{
		Database:      a.Database,
		Retention:     a.Retention,
		ReplicaN:      a.ReplicaN,
		ShardCount:    a.ShardCount,
		ShardDuration: a.ShardDuration,
		DatabasePath:  filepath.Join(server.TSDBConfig().Dir, a.Database),
	}

	if a.StartTime != "" {
		if t, err := time.Parse(time.RFC3339, a.StartTime); err != nil {
			return nil, err
		} else {
			plan.StartTime = t.UTC()
		}
	}

	if err := plan.Validate(); err != nil {
		return nil, err
	}

	return plan, nil
}

type SchemaSpec struct {
	Tags                    TagCardinalities
	PointsPerSeriesPerShard int
}

func (s *SchemaSpec) AddFlags(fs *flag.FlagSet) {
	s.Tags = []int{10, 10, 10}
	fs.Var(&s.Tags, "t", "Tag cardinality")
	fs.IntVar(&s.PointsPerSeriesPerShard, "p", 100, "Points per series per shard")
}

func (s *SchemaSpec) Plan(sp *StoragePlan) (*SchemaPlan, error) {
	return &SchemaPlan{
		StoragePlan:             sp,
		Tags:                    s.Tags,
		PointsPerSeriesPerShard: s.PointsPerSeriesPerShard,
	}, nil
}
