package tsm1

import (
	"io"

	"github.com/influxdb/influxdb/tsdb"
)

type tx struct {
	files  dataFiles
	engine *Engine
}

// TODO: handle multiple fields and descending
func (t *tx) Cursor(series string, fields []string, dec *tsdb.FieldCodec, ascending bool) tsdb.Cursor {
	t.engine.filesLock.RLock()
	defer t.engine.filesLock.RUnlock()

	// don't add the overhead of the multifield cursor if we only have one field
	if len(fields) == 1 {
		id := t.engine.keyAndFieldToID(series, fields[0])
		_, isDeleted := t.engine.deletes[id]

		var indexCursor tsdb.Cursor
		if isDeleted {
			indexCursor = &emptyCursor{ascending: ascending}
		} else {
			indexCursor = newCursor(id, t.files, ascending)
		}
		wc := t.engine.WAL.Cursor(series, fields, dec, ascending)
		return NewCombinedEngineCursor(wc, indexCursor, ascending)
	}

	// multiple fields. use just the MultiFieldCursor, which also handles time collisions
	// so we don't need to use the combined cursor
	var cursors []tsdb.Cursor
	var cursorFields []string
	for _, field := range fields {
		id := t.engine.keyAndFieldToID(series, field)
		_, isDeleted := t.engine.deletes[id]

		var indexCursor tsdb.Cursor
		if isDeleted {
			indexCursor = &emptyCursor{ascending: ascending}
		} else {
			indexCursor = newCursor(id, t.files, ascending)
		}
		wc := t.engine.WAL.Cursor(series, []string{field}, dec, ascending)
		// double up the fields since there's one for the wal and one for the index
		cursorFields = append(cursorFields, field, field)
		cursors = append(cursors, indexCursor, wc)
	}

	return NewMultiFieldCursor(cursorFields, cursors, ascending)
}

func (t *tx) Rollback() error {
	t.engine.queryLock.RUnlock()
	for _, f := range t.files {
		f.mu.RUnlock()
	}

	return nil
}

// TODO: refactor the Tx interface to not have Size, Commit, or WriteTo since they're not used
func (t *tx) Size() int64                              { panic("not implemented") }
func (t *tx) Commit() error                            { panic("not implemented") }
func (t *tx) WriteTo(w io.Writer) (n int64, err error) { panic("not implemented") }
