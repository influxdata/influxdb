package coordinator

import (
	"io"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
)

// IteratorCreator is an interface that combines mapping fields and creating iterators.
type IteratorCreator interface {
	influxql.IteratorCreator
	influxql.FieldMapper
	io.Closer
}

// ShardMapper retrieves and maps shards into an IteratorCreator that can later be
// used for executing queries.
type ShardMapper interface {
	MapShards(sources influxql.Sources, opt *influxql.SelectOptions) (IteratorCreator, error)
}

// LocalShardMapper implements a ShardMapper for local shards.
type LocalShardMapper struct {
	MetaClient interface {
		ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	}

	TSDBStore interface {
		Shard(id uint64) Shard
	}
}

// MapShards maps the sources to the appropriate shards into an IteratorCreator.
func (e *LocalShardMapper) MapShards(sources influxql.Sources, opt *influxql.SelectOptions) (IteratorCreator, error) {
	a := &LocalShardMapping{
		ShardMap: make(map[Source][]*ShardInfo),
	}

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
			shards, ok := a.ShardMap[source]
			if !ok {
				groups, err := e.MetaClient.ShardGroupsByTimeRange(s.Database, s.RetentionPolicy, opt.MinTime, opt.MaxTime)
				if err != nil {
					return nil, err
				}

				if len(groups) == 0 {
					a.ShardMap[source] = nil
					continue
				}

				shards = make([]*ShardInfo, 0, len(groups[0].Shards)*len(groups))
				for _, g := range groups {
					for _, si := range g.Shards {
						shard := e.TSDBStore.Shard(si.ID)
						if shard == nil {
							continue
						}

						shards = append(shards, &ShardInfo{
							Shard:     shard,
							StartTime: g.StartTime,
						})
					}
				}
				a.ShardMap[source] = shards
				a.Size += len(shards)
			}

			// Iterate through the shards within this database/retention policy
			// and calculate the measurements that we would retrieve data. Add
			// these measurements to the list of measurements in the shard
			// info.
			for _, sh := range shards {
				var measurements []string
				if s.Regex != nil {
					measurements = sh.Shard.MeasurementsByRegex(s.Regex.Val)
				} else {
					measurements = []string{s.Name}
				}

				sh.Measurements = append(sh.Measurements, measurements...)
			}
		}
	}
	return a, nil
}

// ShardInfo wraps a shard and keeps information about which measurements will
// be queried from the shard and what the start time of the shard group that
// this shard belongs to is.
type ShardInfo struct {
	// Shard is a wrapper around the shard.
	Shard Shard

	// Measurements is a list of measurements within this shard.
	Measurements []string

	// StartTime is the start time of the shard group. Multiple shards can have
	// the same shard time on different machines when clustering.
	StartTime time.Time
}

// ShardMapper maps data sources to a list of shard information.
type LocalShardMapping struct {
	ShardMap map[Source][]*ShardInfo
	Size     int
}

func (a *LocalShardMapping) WalkShards(fn func(s Source, sh *ShardInfo, sz int) error) error {
	limiter := make(chan struct{}, runtime.GOMAXPROCS(0))
	errch := make(chan error, 1)

	// Wait for all goroutines to finish before exiting.
	var wg sync.WaitGroup
	defer wg.Wait()

	for s, shards := range a.ShardMap {
		for _, sh := range shards {
			wg.Add(1)
			select {
			case limiter <- struct{}{}:
			case err := <-errch:
				return err
			}

			go func(s Source, sh *ShardInfo, sz int) {
				defer wg.Done()
				defer func() { <-limiter }()

				if err := fn(s, sh, sz); err != nil {
					// Only the first error is important and will be returned.
					select {
					case errch <- err:
					default:
					}
				}
			}(s, sh, len(shards))
		}
	}
	return nil
}

func (a *LocalShardMapping) FieldDimensions() (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

	var mu sync.Mutex
	err = a.WalkShards(func(s Source, sh *ShardInfo, sz int) error {
		f, d, err := sh.Shard.FieldDimensions(sh.Measurements)
		if err != nil {
			return err
		}

		mu.Lock()
		defer mu.Unlock()

		for k, typ := range f {
			if _, ok := fields[k]; typ != influxql.Unknown && (!ok || typ < fields[k]) {
				fields[k] = typ
			}
		}
		for k := range d {
			dimensions[k] = struct{}{}
		}
		return nil
	})
	return
}

// SeriesMap maps a series key to a list of the series used to construct the full series.
type SeriesMap map[string][]*SeriesInfo

func (a *LocalShardMapping) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	if influxql.Sources(opt.Sources).HasSystemSource() {
		shards := make([]influxql.IteratorCreator, 0, a.Size)
		for _, group := range a.ShardMap {
			for _, sh := range group {
				shards = append(shards, sh.Shard)
			}
		}
		return influxql.NewLazyIterator(shards, opt)
	}

	var mu sync.Mutex
	seriesMap := make(map[string]map[Source]map[string][]*SeriesInfo)
	if err := a.WalkShards(func(source Source, sh *ShardInfo, sz int) error {
		// Limit the size of the preallocated shards to 64 so if the database
		// contains a very large number of shards, we don't allocate too much
		// memory for the cardinality.
		if sz > 64 {
			sz = 64
		}

		for _, name := range sh.Measurements {
			series, err := createTagSets(sh, name, &opt)
			if series == nil || err != nil {
				return err
			}

			mu.Lock()
			// Retrieve the series mapping for the measurement.
			m, ok := seriesMap[name]
			if !ok {
				m = make(map[Source]map[string][]*SeriesInfo)
				seriesMap[name] = m
			}

			// Retrieve the series mapping for the current source.
			ms, ok := m[source]
			if !ok {
				ms = make(map[string][]*SeriesInfo)
				m[source] = ms
			}

			// Insert each series into map in the appropriate location.
			for _, s := range series {
				keys, ok := ms[string(s.SeriesKey)]
				if !ok {
					// Allocate a slice that is the size of the number of
					// shards with a limit of 64. This assumes that the series
					// exists in every shard, but places a limit on that
					// assumption in case there are greater than 64 shards.
					keys = make([]*SeriesInfo, 0, sz)
				}
				ms[string(s.SeriesKey)] = append(keys, s)
			}
			mu.Unlock()
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Retrieve the measurements and order them.
	names := make([]string, 0, len(seriesMap))
	for name := range seriesMap {
		names = append(names, name)
	}
	sort.Strings(names)

	// Create lazy iterator for each measurement in order.
	ics := make([]influxql.IteratorCreator, len(names))
	for i, name := range names {
		sources := seriesMap[name] // map[Source]map[string][]interface{}

		// Merge the iterators from each database source.
		outer := make([]influxql.IteratorCreator, 0, len(sources))
		for _, s := range sources {
			keys := make([]string, 0, len(s))
			for k := range s {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			// Iterate through each series key.
			inner := make([]influxql.IteratorCreator, len(keys))
			for i, key := range keys {
				// For each key, sort the series info by time and create a lazy iterator.
				shards := s[key]
				sort.Sort(seriesInfoByTime(shards))

				shardics := make([]influxql.IteratorCreator, len(shards))
				for i, shard := range shards {
					shardics[i] = shard
				}
				inner[i] = influxql.LazyIteratorCreator(shardics)
			}
			outer = append(outer, influxql.LazyIteratorCreator(inner))
		}
		ics[i] = influxql.IteratorCreators(outer)
	}
	return influxql.NewLazyIterator(ics, opt)
}

// Close does nothing for a LocalShardMapping.
func (a *LocalShardMapping) Close() error {
	return nil
}

type SeriesInfo struct {
	Shard       Shard
	Measurement string
	SeriesKey   []byte
	TagSet      *influxql.TagSet
	StartTime   time.Time
}

func (si *SeriesInfo) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	return si.Shard.CreateSeriesIterator(si.Measurement, si.TagSet, opt)
}

type seriesInfoByTime []*SeriesInfo

func (a seriesInfoByTime) Len() int { return len(a) }
func (a seriesInfoByTime) Less(i, j int) bool {
	return a[i].StartTime.Before(a[j].StartTime)
}
func (a seriesInfoByTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Source contains the database and retention policy source for data.
type Source struct {
	Database        string
	RetentionPolicy string
}

// marshalTags converts a tag mapping to a byte string. This is different from the normal
// series key because it contains empty tags too.
func marshalTags(tags map[string]string) []byte {
	// Empty maps marshal to empty bytes.
	if len(tags) == 0 {
		return nil
	}

	// Extract keys and determine final size.
	sz := (len(tags) * 2) - 1 // separators
	keys := make([]string, 0, len(tags))
	for k, v := range tags {
		keys = append(keys, k)
		sz += len(k) + len(v)
	}
	sort.Strings(keys)

	// Generate marshaled bytes.
	b := make([]byte, sz)
	buf := b
	for i, k := range keys {
		copy(buf, k)
		buf[len(k)] = '='
		buf = buf[len(k)+1:]

		v := tags[k]
		copy(buf, v)
		if i < len(keys)-1 {
			buf[len(v)] = ','
			buf = buf[len(v)+1:]
		}
	}
	return b
}

// createTagSets creates the series information for the shard from the tag sets.
func createTagSets(si *ShardInfo, name string, opt *influxql.IteratorOptions) ([]*SeriesInfo, error) {
	// Determine tagsets for this measurement based on dimensions and filters.
	tagSets, err := si.Shard.TagSets(name, opt.Dimensions, opt.Condition)
	if tagSets == nil || err != nil {
		return nil, err
	}

	// Calculate tag sets and apply SLIMIT/SOFFSET.
	tagSets = influxql.LimitTagSets(tagSets, opt.SLimit, opt.SOffset)

	if len(tagSets) == 0 {
		return nil, nil
	}

	series := make([]*SeriesInfo, 0, len(tagSets))
	for _, t := range tagSets {
		series = append(series, &SeriesInfo{
			Shard:       si.Shard,
			Measurement: name,
			SeriesKey:   marshalTags(t.Tags),
			TagSet:      t,
			StartTime:   si.StartTime,
		})
	}
	return series, nil
}
