package pd1

import (
	"math"

	"github.com/influxdb/influxdb/tsdb"
)

type combinedEngineCursor struct {
	walCursor      tsdb.Cursor
	engineCursor   tsdb.Cursor
	walKeyBuf      int64
	walValueBuf    interface{}
	engineKeyBuf   int64
	engineValueBuf interface{}
	ascending      bool
}

func NewCombinedEngineCursor(wc, ec tsdb.Cursor, ascending bool) tsdb.Cursor {
	return &combinedEngineCursor{
		walCursor:    wc,
		engineCursor: ec,
		ascending:    ascending,
	}
}

func (c *combinedEngineCursor) SeekTo(seek int64) (key int64, value interface{}) {
	c.walKeyBuf, c.walValueBuf = c.walCursor.SeekTo(seek)
	c.engineKeyBuf, c.engineValueBuf = c.engineCursor.SeekTo(seek)
	return c.read()
}

func (c *combinedEngineCursor) Next() (int64, interface{}) {
	return c.read()
}

func (c *combinedEngineCursor) Ascending() bool {
	return c.ascending
}

func (c *combinedEngineCursor) read() (key int64, value interface{}) {
	if c.walKeyBuf == tsdb.EOF && c.engineKeyBuf == tsdb.EOF {
		return tsdb.EOF, nil
	}

	// handle the case where they have the same point
	if c.walKeyBuf == c.engineKeyBuf {
		// keep the wal value since it will overwrite the engine value
		key = c.walKeyBuf
		value = c.walValueBuf
		c.walKeyBuf, c.walValueBuf = c.walCursor.Next()

		// overwrite the buffered engine values
		c.engineKeyBuf, c.engineValueBuf = c.engineCursor.Next()
		return
	}

	// ascending order
	if c.ascending {
		if c.walKeyBuf != tsdb.EOF && (c.walKeyBuf < c.engineKeyBuf || c.engineKeyBuf == tsdb.EOF) {
			key = c.walKeyBuf
			value = c.walValueBuf
			c.walKeyBuf, c.walValueBuf = c.walCursor.Next()
			return
		}

		key = c.engineKeyBuf
		value = c.engineValueBuf
		c.engineKeyBuf, c.engineValueBuf = c.engineCursor.Next()
		return
	}

	// descending order
	if c.walKeyBuf != tsdb.EOF && c.walKeyBuf > c.engineKeyBuf {
		key = c.walKeyBuf
		value = c.walValueBuf
		c.walKeyBuf, c.walValueBuf = c.walCursor.Next()
		return
	}

	key = c.engineKeyBuf
	value = c.engineValueBuf
	c.engineKeyBuf, c.engineValueBuf = c.engineCursor.Next()
	return
}

type multiFieldCursor struct {
	fields      []string
	cursors     []tsdb.Cursor
	ascending   bool
	keyBuffer   []int64
	valueBuffer []interface{}
}

func NewMultiFieldCursor(fields []string, cursors []tsdb.Cursor, ascending bool) tsdb.Cursor {
	return &multiFieldCursor{
		fields:      fields,
		cursors:     cursors,
		ascending:   ascending,
		keyBuffer:   make([]int64, len(cursors)),
		valueBuffer: make([]interface{}, len(cursors)),
	}
}

func (m *multiFieldCursor) SeekTo(seek int64) (key int64, value interface{}) {
	for i, c := range m.cursors {
		m.keyBuffer[i], m.valueBuffer[i] = c.SeekTo(seek)
	}
	return m.read()
}

func (m *multiFieldCursor) Next() (int64, interface{}) {
	return m.read()
}

func (m *multiFieldCursor) Ascending() bool {
	return m.ascending
}

func (m *multiFieldCursor) read() (int64, interface{}) {
	t := int64(math.MaxInt64)
	if !m.ascending {
		t = int64(math.MinInt64)
	}

	// find the time we need to combine all fields
	for _, k := range m.keyBuffer {
		if k == tsdb.EOF {
			continue
		}
		if m.ascending && t > k {
			t = k
		} else if !m.ascending && t < k {
			t = k
		}
	}

	// get the value and advance each of the cursors that have the matching time
	if t == math.MinInt64 || t == math.MaxInt64 {
		return tsdb.EOF, nil
	}

	mm := make(map[string]interface{})
	for i, k := range m.keyBuffer {
		if k == t {
			mm[m.fields[i]] = m.valueBuffer[i]
			m.keyBuffer[i], m.valueBuffer[i] = m.cursors[i].Next()
		}
	}
	return t, mm
}
