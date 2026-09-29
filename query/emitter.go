package query

import (
	"github.com/influxdata/influxdb/models"
)

// Emitter reads from a cursor into rows.
type Emitter struct {
	cur       Cursor
	chunkSize int

	series  Series
	row     *models.Row
	columns []string

	// grouping is the cursor's groupingKeyer when the query groups by
	// date_part, and nil otherwise; groupingKey is the active GROUP BY
	// date_part dimension of the current row.
	grouping    groupingKeyer
	groupingKey models.GroupingKey
}

// NewEmitter returns a new instance of Emitter that pulls from itrs.
func NewEmitter(cur Cursor, chunkSize int) *Emitter {
	columns := make([]string, len(cur.Columns()))
	for i, col := range cur.Columns() {
		columns[i] = col.Val
	}
	e := &Emitter{
		cur:       cur,
		chunkSize: chunkSize,
		columns:   columns,
	}
	if g, ok := cur.(groupingKeyer); ok && g.HasGroupingKeys() {
		e.grouping = g
	}
	return e
}

// Close closes the underlying iterators.
func (e *Emitter) Close() error {
	return e.cur.Close()
}

// Emit returns the next row from the iterators.
func (e *Emitter) Emit() (*models.Row, bool, error) {
	// A query grouped by date_part takes a separate path so this one stays as
	// it was for queries without date_part.
	if e.grouping != nil {
		return e.emitGrouped()
	}

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
			e.createRow(row.Series, row.Values)
		} else if e.series.SameSeries(row.Series) {
			if e.chunkSize > 0 && len(e.row.Values) >= e.chunkSize {
				r := e.row
				r.Partial = true
				e.createRow(row.Series, row.Values)
				return r, true, nil
			}
			e.row.Values = append(e.row.Values, row.Values)
		} else {
			r := e.row
			e.createRow(row.Series, row.Values)
			return r, true, nil
		}
	}
}

// emitGrouped is Emit for a query grouped by date_part, where the active GROUP
// BY date_part dimension identifies the series along with the name and tags.
func (e *Emitter) emitGrouped() (*models.Row, bool, error) {
	for {
		var row Row
		if !e.cur.Scan(&row) {
			if err := e.cur.Err(); err != nil {
				return nil, false, err
			}
			r := e.row
			e.row = nil
			return r, false, nil
		}

		groupingKey := e.grouping.GroupingKey()
		if e.row == nil {
			e.createGroupedRow(row.Series, groupingKey, row.Values)
		} else if e.series.SameSeries(row.Series) && e.groupingKey == groupingKey {
			if e.chunkSize > 0 && len(e.row.Values) >= e.chunkSize {
				r := e.row
				r.Partial = true
				e.createGroupedRow(row.Series, groupingKey, row.Values)
				return r, true, nil
			}
			e.row.Values = append(e.row.Values, row.Values)
		} else {
			r := e.row
			e.createGroupedRow(row.Series, groupingKey, row.Values)
			return r, true, nil
		}
	}
}

// createRow creates a new row attached to the emitter.
func (e *Emitter) createRow(series Series, values []interface{}) {
	e.series = series
	e.row = &models.Row{
		Name:    series.Name,
		Tags:    series.Tags.KeyValues(),
		Columns: e.columns,
		Values:  [][]interface{}{values},
	}
}

// createGroupedRow creates a new row for the GROUP BY date_part dimension
// groupingKey (zero when the row has none).
func (e *Emitter) createGroupedRow(series Series, groupingKey models.GroupingKey, values []interface{}) {
	e.createRow(series, values)
	e.groupingKey = groupingKey
	e.row.GroupingKey = groupingKey
}
