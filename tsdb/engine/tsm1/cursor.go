package tsm1

import (
	"github.com/influxdata/influxdb/tsdb"
)

type integerRangeCursor struct {
	key string
	cur integerCursor
	t   int64
	asc bool
}

func newIntegerRangeCursor(key string, time int64, asc bool, cur integerCursor) *integerRangeCursor {
	return &integerRangeCursor{key: key, cur: cur, t: time, asc: asc}
}

func (l *integerRangeCursor) Close() {
	// cursors always return nil
	l.cur.close()
}

func (l *integerRangeCursor) SeriesKey() string { return l.key }

func (l *integerRangeCursor) Next() (int64, int64) {
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

type floatRangeCursor struct {
	key string
	cur floatCursor
	t   int64
	asc bool
}

func newFloatRangeCursor(key string, time int64, asc bool, cur floatCursor) *floatRangeCursor {
	return &floatRangeCursor{key: key, cur: cur, t: time, asc: asc}
}

func (l *floatRangeCursor) Close() {
	// cursors always return nil
	l.cur.close()
}

func (l *floatRangeCursor) SeriesKey() string { return l.key }

func (l *floatRangeCursor) Next() (int64, float64) {
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