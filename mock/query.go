package mock

import (
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
)

type Database struct {
	Measurements []string
	Shard        query.ShardGroup
}

func (d *Database) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	itrs := make([]influxql.Iterator, 0, len(d.Measurements))
	for _, name := range d.Measurements {
		itr, err := d.Shard.CreateIterator(name, opt)
		if err != nil {
			influxql.Iterators(itrs).Close()
			return nil, err
		}
		itrs = append(itrs, itr)
	}
	return influxql.NewMergeIterator(itrs, opt), nil
}

func (d *Database) FieldDimensions() (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	return d.Shard.FieldDimensions(d.Measurements)
}

func (d *Database) MapType(field string) influxql.DataType {
	var typ influxql.DataType
	for _, name := range d.Measurements {
		if t := d.Shard.MapType(name, field); typ.LessThan(t) {
			typ = t
		}
	}
	return typ
}

func (d *Database) Close() error {
	return nil
}

type ShardMapper struct {
	ShardsByTimeRangeFn func(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error)
	ShardGroupFn        func(ids []uint64) query.ShardGroup
	MapShardsFn         func(m *influxql.Measurement, opt *influxql.SelectOptions) (query.Database, error)
}

func (m *ShardMapper) ShardsByTimeRange(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error) {
	if m.ShardsByTimeRangeFn != nil {
		return m.ShardsByTimeRangeFn(sources, tmin, tmax)
	}
	return nil, nil
}

func (m *ShardMapper) ShardGroup(ids []uint64) query.ShardGroup {
	if m.ShardGroupFn != nil {
		return m.ShardGroupFn(ids)
	}
	return &ShardGroup{}
}

func (m *ShardMapper) MapShards(measurement *influxql.Measurement, opt *influxql.SelectOptions) (query.Database, error) {
	if m.MapShardsFn != nil {
		return m.MapShardsFn(measurement, opt)
	}

	shards, err := m.ShardsByTimeRange(influxql.Sources{measurement}, opt.MinTime, opt.MaxTime)
	if err != nil {
		return nil, err
	}

	ids := make([]uint64, len(shards))
	for i, sh := range shards {
		ids[i] = sh.ID
	}

	var measurements []string
	shard := m.ShardGroup(ids)
	if measurement.Regex != nil {
		measurements = shard.MeasurementsByRegex(measurement.Regex.Val)
	} else {
		measurements = []string{measurement.Name}
	}
	return &Database{
		Measurements: measurements,
		Shard:        shard,
	}, nil
}

type ShardMapperFn func(stub *ShardMapper)

func NewShardMapper(fn ShardMapperFn) query.ShardMapper {
	stub := ShardMapper{}
	if fn != nil {
		fn(&stub)
	}
	return &stub
}
