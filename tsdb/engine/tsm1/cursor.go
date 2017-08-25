package tsm1

import (
	"github.com/influxdata/influxdb/tsdb"
)

type rangeIntegerCursor struct {
	key string
	cur integerCursor
	t   int64
	asc bool
}

func newRangeIntegerCursor(key string, time int64, asc bool, cur integerCursor) *rangeIntegerCursor {
	return &rangeIntegerCursor{key: key, cur: cur, t: time, asc: asc}
}

func (l *rangeIntegerCursor) Close() {
	// cursors always return nil
	l.cur.close()
}

func (l *rangeIntegerCursor) SeriesKey() string { return l.key }

func (l *rangeIntegerCursor) Next() (int64, int64) {
	k, v := l.cur.nextInteger()
	if k == tsdb.EOF {
		return k, v
	}

	if l.asc {
		if k > l.t {
			l.cur.close()
			l.cur = integerNilCursorStatic
			k = tsdb.EOF
		}
	} else { // desc
		if k < l.t {
			l.cur.close()
			l.cur = integerNilCursorStatic
			k = tsdb.EOF
		}
	}

	return k, v
}

type rangeFloatCursor struct {
	key string
	cur floatCursor
	t   int64
	asc bool
}

func newRangeFloatCursor(key string, time int64, asc bool, cur floatCursor) *rangeFloatCursor {
	return &rangeFloatCursor{key: key, cur: cur, t: time, asc: asc}
}

func (l *rangeFloatCursor) Close() {
	// cursors always return nil
	l.cur.close()
}

func (l *rangeFloatCursor) SeriesKey() string { return l.key }

func (l *rangeFloatCursor) Next() (int64, float64) {
	k, v := l.cur.nextFloat()
	if k == tsdb.EOF {
		return k, v
	}

	if l.asc {
		if k > l.t {
			l.cur.close()
			l.cur = floatNilCursorStatic
			k = tsdb.EOF
		}
	} else { // desc
		if k < l.t {
			l.cur.close()
			l.cur = floatNilCursorStatic
			k = tsdb.EOF
		}
	}

	return k, v
}