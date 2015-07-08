package tsdb

import (
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
)

type Mapper interface {
	// Open will open the necessary resources to being the map job. Could be connections to remote servers or
	// hitting the local store
	Open() error

	// Close will close the mapper
	Close()

	// TagSets returns the tagsets for which the Mapper has data.
	TagSets() []string

	// NextChunk returns the next chunk of points for the given tagset. The chunk will be a maximum size
	// of chunkSize, but it may be less. A chunkSize of 0 means do no chunking.
	NextChunk(tagset string, chunkSize int) (*rawMapperOutput, error)
}

type Executor interface {
	Execute(chunkSize int) <-chan *influxql.Row
}

type Planner struct {
	MetaStore interface {
		ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
		NodeID() uint64
	}

	Cluster interface {
		NewRawMapper(shardID uint64, stmt string) (Mapper, error)
	}

	store *Store

	Logger *log.Logger
}

func NewPlanner(store *Store) *Planner {
	return &Planner{
		store:  store,
		Logger: log.New(os.Stderr, "[planner] ", log.LstdFlags),
	}
}

// Plan creates an execution plan for the given SelectStatement and returns an Executor.
func (p *Planner) Plan(stmt *influxql.SelectStatement) (Executor, error) {
	shards := map[uint64]meta.ShardInfo{} // Shards requiring mappers.

	for _, src := range stmt.Sources {
		mm, ok := src.(*influxql.Measurement)
		if !ok {
			return nil, fmt.Errorf("invalid source type: %#v", src)
		}

		// Replace instances of "now()" with the current time, and check the resultant times.
		stmt.Condition = influxql.Reduce(stmt.Condition, &influxql.NowValuer{Now: time.Now().UTC()})
		tmin, tmax := influxql.TimeRange(stmt.Condition)
		if tmax.IsZero() {
			tmax = time.Now()
		}
		if tmin.IsZero() {
			tmin = time.Unix(0, 0)
		}

		// Build the set of target shards. Using shard IDs as keys ensures each shard ID
		// occurs only once.
		shardGroups, err := p.MetaStore.ShardGroupsByTimeRange(mm.Database, mm.RetentionPolicy, tmin, tmax)
		if err != nil {
			return nil, err
		}
		for _, g := range shardGroups {
			for _, sh := range g.Shards {
				shards[sh.ID] = sh
			}
		}
	}

	if stmt.IsRawQuery && !stmt.HasDistinct() {
		return p.planRawQuery(stmt, shards)
	}
	return p.planAggregateQuery(stmt, shards)
}

func (p *Planner) planRawQuery(stmt *influxql.SelectStatement, shards map[uint64]meta.ShardInfo) (Executor, error) {
	// Build the Mappers, one per shard. If the shard is local to this node, always use
	// that one, versus asking the cluster.
	mappers := []Mapper{}
	for _, sh := range shards {
		if sh.OwnedBy(p.MetaStore.NodeID()) {
			shard := p.store.Shard(sh.ID)
			if shard == nil {
				// If the store returns nil, no data has actually been written to the shard.
				// In this case, since there is no data, don't make a mapper.
				continue
			}
			mappers = append(mappers, NewRawMapper(shard, stmt))
		} else {
			mapper, err := p.Cluster.NewRawMapper(sh.ID, stmt.String())
			if err != nil {
				return nil, err
			}
			mappers = append(mappers, mapper)
		}
	}

	return NewRawExecutor(stmt, mappers), nil
}

func (p *Planner) planAggregateQuery(stmt *influxql.SelectStatement, shards map[uint64]meta.ShardInfo) (Executor, error) {
	return nil, nil
}

// RawExecutor is an executor for RawMappers.
type RawExecutor struct {
	stmt    *influxql.SelectStatement
	mappers []Mapper
}

// NewRawExecutor returns a new RawExecutor.
func NewRawExecutor(stmt *influxql.SelectStatement, mappers []Mapper) *RawExecutor {
	return &RawExecutor{
		stmt:    stmt,
		mappers: mappers,
	}
}

// Execute begins execution of the query and returns a channel to receive rows.
func (re *RawExecutor) Execute(chunkSize int) <-chan *influxql.Row {
	// Create output channel and stream data in a separate goroutine.
	out := make(chan *influxql.Row, 0)
	go re.execute(out, chunkSize)
	return out
}

func (re *RawExecutor) execute(out chan *influxql.Row, chunkSize int) {
	// It's important that all resources are released when execution completes.
	defer re.close()

	// Open the mappers.
	for _, m := range re.mappers {
		if err := m.Open(); err != nil {
			out <- &influxql.Row{Err: err}
			return
		}
	}

	// Build the set of available tagsets across all mappers.
	availTagSets := newStringSet()
	for _, m := range re.mappers {
		for _, t := range m.TagSets() {
			availTagSets.add(t)
		}
	}

tagsetLoop:
	// For each mapper, drain it by tagset.
	for _, t := range availTagSets.list() {
		// Used to read ahead chunks.
		mapperOutputs := make([]*rawMapperOutput, len(re.mappers))
		var rowWriter *limitedRowWriter

		for {
			// Get the next chunk from each mapper.
			for j, m := range re.mappers {
				if mapperOutputs[j] != nil {
					// There is data for this mapper, for this tagset, from a previous loop.
					continue
				}

				chunk, err := m.NextChunk(t, chunkSize)
				if err != nil {
					out <- &influxql.Row{Err: err}
					return
				}
				if len(chunk.Values) == 0 {
					// This mapper is empty for this tagset.
					continue
				}
				mapperOutputs[j] = chunk
			}

			// Process the mapper outputs. We can send out everything up to the min of the last time
			// of the chunks.
			minTime := int64(math.MaxInt64)
			for _, o := range mapperOutputs {
				// Some of the mappers could empty out before others so ignore them because they'll be nil
				if o == nil {
					continue
				}

				// Find the min of the last point across all the chunks.
				t := o.Values[len(o.Values)-1].Time
				if t < minTime {
					minTime = t
				}
			}

			// Now empty out all the chunks up to the min time. Create new output struct for this data.
			var chunkedOutput *rawMapperOutput
			for j, o := range mapperOutputs {
				if o == nil {
					continue
				}

				// Very first value in this mapper is at a higher timestamp. Skip it.
				if o.Values[0].Time > minTime {
					continue
				}

				// Find the index of the point up to the min.
				ind := len(o.Values)
				for i, mo := range o.Values {
					if mo.Time > minTime {
						ind = i
						break
					}
				}

				// Add up to the index to the values
				if chunkedOutput == nil {
					chunkedOutput = &rawMapperOutput{
						Name: o.Name,
						Tags: o.Tags,
					}
					chunkedOutput.Values = o.Values[:ind]
				} else {
					chunkedOutput.Values = append(chunkedOutput.Values, o.Values[:ind]...)
				}

				// Clear out the values being sent out, keep the remainder.
				mapperOutputs[j].Values = mapperOutputs[j].Values[ind:]

				// If we emptied out all the values, set this output to nil so that the mapper will get run again on the next loop
				if len(mapperOutputs[j].Values) == 0 {
					mapperOutputs[j] = nil
				}
			}

			// If we didn't pull out any values, this tagset is done.
			if chunkedOutput == nil {
				break
			}

			// Sort the values by time first so we can then handle offset and limit
			sort.Sort(rawMapperValues(chunkedOutput.Values))

			if rowWriter == nil {
				rowWriter = &limitedRowWriter{
					limit:       re.stmt.Limit,
					chunkSize:   chunkSize,
					name:        chunkedOutput.Name,
					tags:        chunkedOutput.Tags,
					selectNames: re.stmt.NamesInSelect(),
					c:           out,
				}
			}
			if limited := rowWriter.Add(chunkedOutput.Values); limited {
				// Limit for this tagset was reached, go to next one.
				continue tagsetLoop
			}
		}

		rowWriter.Flush()
	}

	// XXX Limit and chunk checking.
	close(out)
}

// Close closes the executor such that all resources are released. Once closed,
// an executor may not be re-used.
func (re *RawExecutor) close() {
	for _, m := range re.mappers {
		m.Close()
	}
}

// limitedRowWriter accepts raw maper values, and will emit those values as rows in chunks
// of the given size. If the chunk size is 0, no chunking will be performed. In addiiton if
// limit is reached, outstanding values will be emitted. If limit is zero, no limit is enforced.
type limitedRowWriter struct {
	chunkSize   int
	limit       int
	name        string
	tags        map[string]string
	selectNames []string
	c           chan *influxql.Row

	currValues []*rawMapperValue
	totalSent  int
}

// Add accepts a slice of values, and will emit those values as per chunking requirements.
// If limited is returned as true, the limit was also reached and no more values should be
// add. In that case only up the limit of values are emitted.
func (r *limitedRowWriter) Add(values []*rawMapperValue) (limited bool) {
	if r.currValues == nil {
		r.currValues = make([]*rawMapperValue, 0, r.chunkSize)
	}
	r.currValues = append(r.currValues, values...)

	// Check limit.
	limitReached := r.limit > 0 && r.totalSent+len(r.currValues) >= r.limit
	if limitReached {
		// Limit will be satified with current values. Truncate 'em.
		r.currValues = r.currValues[:r.limit-r.totalSent]
	}

	// Is chunking in effect?
	if r.chunkSize > 0 {
		// Chunking level reached?
		for len(r.currValues) >= r.chunkSize {
			index := len(r.currValues) - (len(r.currValues) - r.chunkSize)
			r.c <- r.processValues(r.currValues[:index])
			r.totalSent += index
			r.currValues = r.currValues[index:]
		}

		// After values have been sent out by chunking, there may still be some
		// values left, if the remainder is less than the chunk size. But if the
		// limit has been reached, kick them out.
		if len(r.currValues) > 0 && limitReached {
			r.c <- r.processValues(r.currValues)
			r.currValues = nil
		}
	} else if limitReached {
		// No chunking in effect, but the limit has been reached.
		r.c <- r.processValues(r.currValues)
		r.currValues = nil
	}

	return limitReached
}

// Flush instructs the limitedRowWriter to emit any pending values as a single row,
// adhering to any limits. Chunking is not enforced.
func (r *limitedRowWriter) Flush() {
	if len(r.currValues) == 0 {
		return
	}

	if r.limit > 0 && len(r.currValues) > r.limit {
		r.currValues = r.currValues[:r.limit]
	}
	r.c <- r.processValues(r.currValues)
	r.totalSent = len(r.currValues)
	r.currValues = nil
}

// processValues emits the given values in a single row.
func (r *limitedRowWriter) processValues(values []*rawMapperValue) *influxql.Row {
	selectNames := r.selectNames

	// ensure that time is in the select names and in the first position
	hasTime := false
	for i, n := range selectNames {
		if n == "time" {
			// Swap time to the first argument for names
			if i != 0 {
				selectNames[0], selectNames[i] = selectNames[i], selectNames[0]
			}
			hasTime = true
			break
		}
	}

	// time should always be in the list of names they get back
	if !hasTime {
		selectNames = append([]string{"time"}, selectNames...)
	}

	// since selectNames can contain tags, we need to strip them out
	selectFields := make([]string, 0, len(selectNames))

	for _, n := range selectNames {
		if _, found := r.tags[n]; !found {
			selectFields = append(selectFields, n)
		}
	}

	row := &influxql.Row{
		Name:    r.name,
		Tags:    r.tags,
		Columns: selectFields,
	}

	// if they've selected only a single value we have to handle things a little differently
	singleValue := len(selectFields) == influxql.SelectColumnCountWithOneValue

	// the results will have all of the raw mapper results, convert into the row
	for _, v := range values {
		vals := make([]interface{}, len(selectFields))

		if singleValue {
			vals[0] = time.Unix(0, v.Time).UTC()
			vals[1] = v.Value.(interface{})
		} else {
			fields := v.Value.(map[string]interface{})

			// time is always the first value
			vals[0] = time.Unix(0, v.Time).UTC()

			// populate the other values
			for i := 1; i < len(selectFields); i++ {
				vals[i] = fields[selectFields[i]]
			}
		}

		row.Values = append(row.Values, vals)
	}

	return row
}
