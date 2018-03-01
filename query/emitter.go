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
