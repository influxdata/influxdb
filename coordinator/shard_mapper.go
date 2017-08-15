package coordinator

import (
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
)

// ShardMapper retrieves and maps shards into an IteratorCreator that can later be
// used for executing queries.
type ShardMapper interface {
	MapShards(m *influxql.Measurement, opt *influxql.SelectOptions) (query.Database, error)
}

// LocalShardMapper implements a ShardMapper for local shards.
type LocalShardMapper struct {
	MetaClient interface {
		ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	}

	TSDBStore interface {
		ShardGroup(ids []uint64) query.ShardGroup
	}
}

// MapShards maps the sources to the appropriate shards into an IteratorCreator.
func (e *LocalShardMapper) MapShards(m *influxql.Measurement, opt *influxql.SelectOptions) (query.Database, error) {
	groups, err := e.MetaClient.ShardGroupsByTimeRange(m.Database, m.RetentionPolicy, opt.MinTime, opt.MaxTime)
	if err != nil {
		return nil, err
	}

	if len(groups) == 0 {
		return (*LocalShardMapping)(nil), nil
	}

	shardIDs := make([]uint64, 0, len(groups[0].Shards)*len(groups))
	for _, g := range groups {
		for _, si := range g.Shards {
			shardIDs = append(shardIDs, si.ID)
		}
	}
	shard := e.TSDBStore.ShardGroup(shardIDs)

	// Determine the measurements associated with this measurement in the shard.
	var measurements []string
	if m.Regex != nil {
		measurements = shard.MeasurementsByRegex(m.Regex.Val)
	} else {
		measurements = []string{m.Name}
	}
	return &LocalShardMapping{
		Shard:        shard,
		Measurements: measurements,
	}, nil
}

// ShardMapper maps data sources to a list of shard information.
type LocalShardMapping struct {
	Shard        query.ShardGroup
	Measurements []string
}

func (a *LocalShardMapping) FieldDimensions() (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	if a == nil {
		return nil, nil, nil
	}
	return a.Shard.FieldDimensions(a.Measurements)
}

func (a *LocalShardMapping) MapType(field string) influxql.DataType {
	if a == nil {
		return influxql.Unknown
	}

	var typ influxql.DataType
	for _, name := range a.Measurements {
		t := a.Shard.MapType(name, field)
		if typ.LessThan(t) {
			typ = t
		}
	}
	return typ
}

func (a *LocalShardMapping) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	if a == nil || len(a.Measurements) == 0 {
		return nil, nil
	}

	if len(a.Measurements) == 1 {
		return a.Shard.CreateIterator(a.Measurements[0], opt)
	}

	inputs := make([]influxql.Iterator, 0, len(a.Measurements))
	if err := func() error {
		for _, measurement := range a.Measurements {
			input, err := a.Shard.CreateIterator(measurement, opt)
			if err != nil {
				return err
			}
			inputs = append(inputs, input)
		}
		return nil
	}(); err != nil {
		influxql.Iterators(inputs).Close()
		return nil, err
	}
	return influxql.Iterators(inputs).Merge(opt)
}

// Close does nothing for a LocalShardMapping.
func (a *LocalShardMapping) Close() error {
	return nil
}

// Source contains the database and retention policy source for data.
type Source struct {
	Database        string
	RetentionPolicy string
}
