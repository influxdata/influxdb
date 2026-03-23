package query

import (
	"sort"

	"github.com/influxdata/influxdb/models"
)

// Emitter reads from a cursor into rows.
type Emitter struct {
	cur       Cursor
	chunkSize int

	series       Series
	groupingKeys map[string]struct{}
	row          *models.Row
	columns      []string
}

// NewEmitter returns a new instance of Emitter that pulls from itrs.
func NewEmitter(cur Cursor, chunkSize int) *Emitter {
	columns := make([]string, len(cur.Columns()))
	for i, col := range cur.Columns() {
		columns[i] = col.Val
	}
	return &Emitter{
		cur:       cur,
		chunkSize: chunkSize,
		columns:   columns,
	}
}

// Close closes the underlying iterators.
func (e *Emitter) Close() error {
	return e.cur.Close()
}

// Emit returns the next row from the iterators.
func (e *Emitter) Emit() (*models.Row, bool, error) {
	// Continually read from the cursor until it is exhausted.
	for {
		// Scan the next row. If there are no rows left, return the current row.
		var row Row
		if !e.cur.Scan(&row) {
			if err := e.cur.Err(); err != nil {
				return nil, false, err
			}
			r := e.row
			e.row = nil
			return r, false, nil
		}

		// If there's no row yet then create one.
		// If the name and tags match the existing row, append to that row if
		// the number of values doesn't exceed the chunk size.
		// Otherwise return existing row and add values to next emitted row.
		if e.row == nil {
			e.createRow(row.Series, row.GroupingKeys, row.Values)
		} else if e.series.SameSeries(row.Series) && sameGroupingKeys(e.groupingKeys, row.GroupingKeys) {
			if e.chunkSize > 0 && len(e.row.Values) >= e.chunkSize {
				r := e.row
				r.Partial = true
				e.createRow(row.Series, row.GroupingKeys, row.Values)
				return r, true, nil
			}
			e.row.Values = append(e.row.Values, row.Values)
		} else {
			r := e.row
			e.createRow(row.Series, row.GroupingKeys, row.Values)
			return r, true, nil
		}
	}
}

// createRow creates a new row attached to the emitter.
func (e *Emitter) createRow(series Series, groupingKeys map[string]struct{}, values []interface{}) {
	e.series = series
	e.groupingKeys = groupingKeys
	e.row = &models.Row{
		Name:         series.Name,
		Tags:         series.Tags.KeyValues(),
		GroupingKeys: sortedKeys(groupingKeys),
		Columns:      e.columns,
		Values:       [][]interface{}{values},
	}
}

// sameGroupingKeys returns true if two grouping key sets have the same dimension names.
func sameGroupingKeys(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

// sortedKeys returns a sorted slice of keys from a set.
func sortedKeys(m map[string]struct{}) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
