package tsdb

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/models"
)

// ShowTagValuesExecutor implements the Executor interface for a SHOW MEASUREMENTS statement.
type ShowTagValuesExecutor struct {
	stmt      *influxql.ShowTagValuesStatement
	mappers   []Mapper
	chunkSize int
}

// NewShowTagValuesExecutor returns a new ShowTagValuesExecutor.
func NewShowTagValuesExecutor(stmt *influxql.ShowTagValuesStatement, mappers []Mapper, chunkSize int) *ShowTagValuesExecutor {
	return &ShowTagValuesExecutor{
		stmt:      stmt,
		mappers:   mappers,
		chunkSize: chunkSize,
	}
}

// Execute begins execution of the query and returns a channel to receive rows.
func (e *ShowTagValuesExecutor) Execute() <-chan *models.Row {
	// Create output channel and stream data in a separate goroutine.
	out := make(chan *models.Row, 0)

	// It's important that all resources are released when execution completes.
	defer e.close()

	go func() {
		defer close(out)
		// Open the mappers.
		for _, m := range e.mappers {
			if err := m.Open(); err != nil {
				out <- &models.Row{Err: err}
				return
			}
		}

		// Create a map of tag keys to tag values.
		set := map[string]map[string]struct{}{}
		// Iterate through mappers collecting tag values.
		for _, m := range e.mappers {
			// Read all data from the mapper.
			for {
				c, err := m.NextChunk()
				if err != nil {
					out <- &models.Row{Err: err}
					return
				} else if c == nil {
					// Mapper has been drained.
					break
				}

				// Convert the mapper chunk to measurements with tag key and values.
				tsvs, ok := c.(TagsValues)
				if !ok {
					out <- &models.Row{Err: fmt.Errorf("show tag values mapper returned invalid type: %T", c)}
					return
				}

				// Merge mapper chunk with previous mapper outputs.
				for _, tvs := range tsvs {
					// Add the tag key to the set if not already there.
					if set[tvs.Key] == nil {
						set[tvs.Key] = map[string]struct{}{}
					}
					// Add the tag values to the tag key's set.
					for _, value := range tvs.Values {
						set[tvs.Key][value] = struct{}{}
					}
				}
			}
		}

		// All mappers are drained.

		// Convert the set into an array of tag keys and values.
		tsvs := make(TagsValues, 0)
		for key, values := range set {
			tvs := &TagValues{
				Key:    key,
				Values: make([]string, 0, len(values)),
			}
			for value := range values {
				tvs.Values = append(tvs.Values, value)
			}
			sort.Strings(tvs.Values)
			tsvs = append(tsvs, tvs)
		}
		// Sort by tag key.
		sort.Sort(tsvs)

		// Calculate series offset and limit.
		slim, soff := limitAndOffset(e.stmt.SLimit, e.stmt.SOffset, len(tsvs))

		// Send results.
		for _, tvs := range tsvs[soff:slim] {
			lim, off := limitAndOffset(e.stmt.Limit, e.stmt.Offset, len(tvs.Values))

			row := &models.Row{
				Name:    tvs.Key,
				Columns: []string{"tagKey"},
				Values:  make([][]interface{}, 0, lim-off),
			}

			for _, tv := range tvs.Values[off:lim] {
				v := []interface{}{tv}
				row.Values = append(row.Values, v)
			}

			out <- row
		}
	}()
	return out
}

// Close closes the executor such that all resources are released. Once closed,
// an executor may not be re-used.
func (e *ShowTagValuesExecutor) close() {
	if e != nil {
		for _, m := range e.mappers {
			m.Close()
		}
	}
}

// ShowTagValuesMapper is a mapper for collecting measurement names from a shard.
type ShowTagValuesMapper struct {
	remote    Mapper
	shard     *Shard
	stmt      *influxql.ShowTagValuesStatement
	chunkSize int
	state     interface{}
}

// NewShowTagValuesMapper returns a mapper for the given shard, which will return data for the meta statement.
func NewShowTagValuesMapper(shard *Shard, stmt *influxql.ShowTagValuesStatement, chunkSize int) *ShowTagValuesMapper {
	return &ShowTagValuesMapper{
		shard:     shard,
		stmt:      stmt,
		chunkSize: chunkSize,
	}
}

// TagValues represents measurement tag values.
type TagValues struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

// TagsValues represents values for multiple tag keys.
type TagsValues []*TagValues

func (a TagsValues) Len() int           { return len(a) }
func (a TagsValues) Less(i, j int) bool { return a[i].Key < a[j].Key }
func (a TagsValues) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// Size returns the total string length of tag key and values.
func (a TagsValues) Size() int {
	n := 0
	for _, tv := range a {
		n += len(tv.Key)
		for _, v := range tv.Values {
			n += len(v)
		}
	}
	return n
}

// Open opens the mapper for use.
func (m *ShowTagValuesMapper) Open() error {
	if m.remote != nil {
		return m.remote.Open()
	}

	// This can happen when a shard has been assigned to this node but we have not
	// written to it so it may not exist yet.
	if m.shard == nil {
		return nil
	}

	sources := influxql.Sources{}

	// Expand regex expressions in the FROM clause.
	if m.stmt.Sources != nil {
		var err error
		sources, err = m.shard.index.ExpandSources(m.stmt.Sources)
		if err != nil {
			return err
		}
	}

	// Get measurements from sources in the statement if provided or database if not.
	measurements, err := measurementsFromSourcesOrDB(m.shard.index, sources...)
	if err != nil {
		return err
	}

	// If a WHERE clause was specified, filter the measurements.
	if m.stmt.Condition != nil {
		var err error
		whereMs, err := m.shard.index.measurementsByExpr(m.stmt.Condition)
		if err != nil {
			return err
		}

		sort.Sort(whereMs)

		measurements = measurements.intersect(whereMs)
	}

	// Create a channel to send tag values on.
	ch := make(chan TagsValues)
	// Start a goroutine to send the names over the channel as needed.
	go func() {
		for _, mm := range measurements {
			tagKeys := mm.TagKeys()
			tsvs := make(TagsValues, 0, len(tagKeys))
			for _, tagKey := range tagKeys {
				tvs := &TagValues{
					Key:    tagKey,
					Values: mm.TagValues(tagKey),
				}
				tsvs = append(tsvs, tvs)
			}
			ch <- tsvs
		}
		close(ch)
	}()

	// Store the channel as the state of the mapper.
	m.state = ch

	return nil
}

// SetRemote sets the remote mapper to use.
func (m *ShowTagValuesMapper) SetRemote(remote Mapper) error {
	m.remote = remote
	return nil
}

// TagSets is only implemented on this mapper to satisfy the Mapper interface.
func (m *ShowTagValuesMapper) TagSets() []string { return nil }

// Fields returns a list of field names for this mapper.
func (m *ShowTagValuesMapper) Fields() []string { return []string{"tagKey"} }

// NextChunk returns the next chunk of measurements and tag keys.
func (m *ShowTagValuesMapper) NextChunk() (interface{}, error) {
	if m.remote != nil {
		b, err := m.remote.NextChunk()
		if err != nil {
			return nil, err
		} else if b == nil {
			return nil, nil
		}

		tvs := []*TagValues{}
		if err := json.Unmarshal(b.([]byte), &tvs); err != nil {
			return nil, err
		} else if len(tvs) == 0 {
			// Mapper on other node sent 0 values so it's done.
			return nil, nil
		}
		return tvs, nil
	}
	return m.nextChunk()
}

// nextChunk implements next chunk logic for a local shard.
func (m *ShowTagValuesMapper) nextChunk() (interface{}, error) {
	// Get the channel of measurement tag values from the state.
	ch, ok := m.state.(chan *TagValues)
	if !ok {
		return nil, nil
	}
	// Allocate array to hold tag values.
	tsvs := make(TagsValues, 0)
	// Get the next chunk of tag keys.
	for n := range ch {
		tsvs = append(tsvs, n)
		if tsvs.Size() >= m.chunkSize {
			break
		}
	}
	// See if we've read all the values.
	if len(tsvs) == 0 {
		return nil, nil
	}

	return tsvs, nil
}

// Close closes the mapper.
func (m *ShowTagValuesMapper) Close() {
	if m.remote != nil {
		m.remote.Close()
	}
}
