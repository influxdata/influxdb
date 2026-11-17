package coordinator

import (
	"context"
	"io"
	"time"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

// IteratorCreator is an interface that combines mapping fields and creating iterators.
type IteratorCreator interface {
	query.IteratorCreator
	influxql.FieldMapper
	io.Closer
}

// LocalShardMapper implements a ShardMapper for local shards.
type LocalShardMapper struct {
	MetaClient interface {
		ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	}

	TSDBStore interface {
		ShardGroup(ids []uint64) tsdb.ShardGroup
	}
}

// MapShards maps the sources to the appropriate shards into an IteratorCreator.
func (e *LocalShardMapper) MapShards(sources influxql.Sources, t influxql.TimeRange, opt query.SelectOptions) (query.ShardGroup, error) {
	a := &LocalShardMapping{
		ShardMap: make(map[Source]tsdb.ShardGroup),
		Groups:   make(map[Source][]groupedShards),
	}

	tmin := time.Unix(0, t.MinTimeNano())
	tmax := time.Unix(0, t.MaxTimeNano())
	if err := e.mapShards(a, sources, tmin, tmax); err != nil {
		return nil, err
	}
	a.MinTime, a.MaxTime = tmin, tmax
	return a, nil
}

func (e *LocalShardMapper) mapShards(a *LocalShardMapping, sources influxql.Sources, tmin, tmax time.Time) error {
	for _, s := range sources {
		switch s := s.(type) {
		case *influxql.Measurement:
			source := Source{
				Database:        s.Database,
				RetentionPolicy: s.RetentionPolicy,
			}
			// Retrieve the list of shards for this database. This list of
			// shards is always the same regardless of which measurement we are
			// using.
			if _, ok := a.ShardMap[source]; !ok {
				groups, err := e.MetaClient.ShardGroupsByTimeRange(s.Database, s.RetentionPolicy, tmin, tmax)
				if err != nil {
					return err
				}

				if len(groups) == 0 {
					a.ShardMap[source] = nil
					continue
				}

				shardIDs := make([]uint64, 0, len(groups[0].Shards)*len(groups))
				groupedIDs := make([]groupedShards, 0, len(groups))
				for _, g := range groups {
					ids := make([]uint64, 0, len(g.Shards))
					for _, si := range g.Shards {
						shardIDs = append(shardIDs, si.ID)
						ids = append(ids, si.ID)
					}
					// groups is ordered ascending by time (see
					// meta.ShardGroupInfos.Less), so groupedIDs is too.
					groupedIDs = append(groupedIDs, groupedShards{
						StartTime: g.StartTime,
						EndTime:   g.EndTime,
						Shards:    e.TSDBStore.ShardGroup(ids),
					})
				}
				a.ShardMap[source] = e.TSDBStore.ShardGroup(shardIDs)
				a.Groups[source] = groupedIDs
			}
		case *influxql.SubQuery:
			if err := e.mapShards(a, s.Statement.Sources, tmin, tmax); err != nil {
				return err
			}
		}
	}
	return nil
}

// ShardMapper maps data sources to a list of shard information.
type LocalShardMapping struct {
	ShardMap map[Source]tsdb.ShardGroup

	// Groups holds the same shards as ShardMap, but partitioned by their
	// originating meta shard group and ordered ascending by time. It is
	// used to let CreateIterator stop opening chronologically later (or,
	// for descending queries, earlier) shard groups once a global LIMIT is
	// already satisfied by groups already opened.
	Groups map[Source][]groupedShards

	// MinTime is the minimum time that this shard mapper will allow.
	// Any attempt to use a time before this one will automatically result in using
	// this time instead.
	MinTime time.Time

	// MaxTime is the maximum time that this shard mapper will allow.
	// Any attempt to use a time after this one will automatically result in using
	// this time instead.
	MaxTime time.Time
}

// groupedShards pairs one meta shard-group's time boundary with a
// tsdb.ShardGroup handle covering only that group's shard IDs.
type groupedShards struct {
	StartTime time.Time
	EndTime   time.Time
	Shards    tsdb.ShardGroup
}

func (a *LocalShardMapping) FieldDimensions(m *influxql.Measurement) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	sg := a.ShardMap[source]
	if sg == nil {
		return
	}

	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

	var measurements []string
	if m.Regex != nil {
		measurements = sg.MeasurementsByRegex(m.Regex.Val)
	} else {
		measurements = []string{m.Name}
	}

	f, d, err := sg.FieldDimensions(measurements)
	if err != nil {
		return nil, nil, err
	}
	for k, typ := range f {
		fields[k] = typ
	}
	for k := range d {
		dimensions[k] = struct{}{}
	}
	return
}

func (a *LocalShardMapping) MapType(m *influxql.Measurement, field string) influxql.DataType {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	sg := a.ShardMap[source]
	if sg == nil {
		return influxql.Unknown
	}

	var names []string
	if m.Regex != nil {
		names = sg.MeasurementsByRegex(m.Regex.Val)
	} else {
		names = []string{m.Name}
	}

	var typ influxql.DataType
	for _, name := range names {
		if m.SystemIterator != "" {
			name = m.SystemIterator
		}
		t := sg.MapType(name, field)
		if typ.LessThan(t) {
			typ = t
		}
	}
	return typ
}

func (a *LocalShardMapping) CreateIterator(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	sg := a.ShardMap[source]
	if sg == nil {
		return nil, nil
	}

	// Override the time constraints if they don't match each other.
	if !a.MinTime.IsZero() && opt.StartTime < a.MinTime.UnixNano() {
		opt.StartTime = a.MinTime.UnixNano()
	}
	if !a.MaxTime.IsZero() && opt.EndTime > a.MaxTime.UnixNano() {
		opt.EndTime = a.MaxTime.UnixNano()
	}

	if m.Regex != nil {
		measurements := sg.MeasurementsByRegex(m.Regex.Val)
		inputs := make([]query.Iterator, 0, len(measurements))
		if err := func() error {
			// Create a Measurement for each returned matching measurement value
			// from the regex.
			for _, measurement := range measurements {
				mm := m.Clone()
				mm.Name = measurement // Set the name to this matching regex value.
				input, err := sg.CreateIterator(ctx, mm, opt)
				if err != nil {
					return err
				}
				inputs = append(inputs, input)
			}
			return nil
		}(); err != nil {
			query.Iterators(inputs).Close()
			return nil, err
		}

		return query.Iterators(inputs).Merge(opt)
	}

	if groups := a.Groups[source]; len(groups) > 1 && isLimitPushdownEligible(m, opt) {
		return newShardGroupLimitIterator(ctx, m, opt, groups)
	}
	return sg.CreateIterator(ctx, m, opt)
}

func (a *LocalShardMapping) IteratorCost(m *influxql.Measurement, opt query.IteratorOptions) (query.IteratorCost, error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	sg := a.ShardMap[source]
	if sg == nil {
		return query.IteratorCost{}, nil
	}

	// Override the time constraints if they don't match each other.
	if !a.MinTime.IsZero() && opt.StartTime < a.MinTime.UnixNano() {
		opt.StartTime = a.MinTime.UnixNano()
	}
	if !a.MaxTime.IsZero() && opt.EndTime > a.MaxTime.UnixNano() {
		opt.EndTime = a.MaxTime.UnixNano()
	}

	if m.Regex != nil {
		var costs query.IteratorCost
		measurements := sg.MeasurementsByRegex(m.Regex.Val)
		for _, measurement := range measurements {
			cost, err := sg.IteratorCost(measurement, opt)
			if err != nil {
				return query.IteratorCost{}, err
			}
			costs = costs.Combine(cost)
		}
		return costs, nil
	}
	return sg.IteratorCost(m.Name, opt)
}

// Close clears out the list of mapped shards.
func (a *LocalShardMapping) Close() error {
	a.ShardMap = nil
	a.Groups = nil
	return nil
}

// Source contains the database and retention policy source for data.
type Source struct {
	Database        string
	RetentionPolicy string
}
